import os
import sys
sys.path.append(os.path.dirname(__file__))
import boto3
from libraries import helper_utils as helper
import warnings
warnings.simplefilter('ignore')
import logging
import time
from threading import Thread

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("debug.log"),
        logging.StreamHandler()
    ]
)


def create_aws_client_connection(client_type,aws_connection):
    """
    Creates an AWS client connection of either s3, emr, ec2 to interact
    the specific client_type objects on a specified AWS profile

    """
    aws_client         = boto3.client(client_type, aws_access_key_id=aws_connection["aws_access_key_id"],
                     aws_secret_access_key=aws_connection["aws_secret_access_key"],
                     aws_session_token=aws_connection['aws_session_token'],
                     region_name='us-east-1')
    return aws_client


def create_aws_rds_instance(rds_params, aws_connection, wait_till_finish = False):
    """
    creates an aws rds instance

    """
    logging.info(f"\tCreating database with name {rds_params['DBInstanceIdentifier']}\n")
    db_identifier = rds_params["DBInstanceIdentifier"]
    aws_rds_client = create_aws_client_connection('rds', aws_connection)
    response = aws_rds_client.create_db_instance(**rds_params)
    try:
        if wait_till_finish: # Wait for the RDS instance to be available
            waiter = aws_rds_client.get_waiter('db_instance_available')
            waiter.wait(DBInstanceIdentifier=db_identifier)
            response = aws_rds_client.describe_db_instances(DBInstanceIdentifier=db_identifier)
            endpoint = response['DBInstances'][0]['Endpoint']['Address']
            logging.info(f"\tAWS RDS {rds_params['DBInstanceIdentifier']} created: {endpoint}\n")
            return endpoint
        else:
            logging.info(f"\tAWS RDS {rds_params['DBInstanceIdentifier']} will be created in the background.\n")
    except Exception as ex:
        logging.error(f"Error with creating rds {rds_params['DBInstanceIdentifier']}\n{ex}\n")
        sys.exit(1)



def create_s3_bucket(bucket_name, aws_connection):
    """
    Creates an S3 bucket

    """
    logging.info(f"Attempting to create AWS S3 Bucket: {bucket_name}")
    try:
        aws_s3_client = create_aws_client_connection('s3',aws_connection)
        aws_s3_client.create_bucket(Bucket=bucket_name)
        logging.info(f"AWS S3 Bucket created: {bucket_name}\n")
    except Exception as e:
        logging.error(f"Error creating S3 bucket: {e}\n")
        sys.exit()


def upload_file_to_s3_bucket(local_file_path, bucket_name, s3_file_location, aws_connection):
    """
    uploads a specified file to the specified bucket in the specified location

    """
    try:
        s3_client = create_aws_client_connection('s3',aws_connection)
        s3_client.upload_file(local_file_path, bucket_name, s3_file_location)
        logging.info(f"Upload for: {local_file_path} Completed Successfully.")
    except Exception as ex:
        logging.error(f"Error uploading file to S3: {ex}")

def upload_multiple_files_to_s3_bucket(local_folder_path, bucket_name, s3_folder_path, aws_connection, desired_file_type = 'csv'):
    """
    uploads multiple files  to the specified bucket
    """
    files = os.listdir(local_folder_path)
    for file_name in files:
        file_path = os.path.join(local_folder_path, file_name)
        file_type = helper.get_file_type(file_path)
        s3_file_location = f"{s3_folder_path}/{file_name}".replace("//", '/')
        if os.path.isfile(file_path) and (file_type==desired_file_type or file_name=='_SUCCESS') :
            upload_file_to_s3_bucket(file_path, bucket_name, s3_file_location,  aws_connection)


def upload_multiple_files_to_s3_bucket_as_parquet(files_folder, bucket_name, s3_bucket_path, aws_connection):
    """
    Uploads specified csv files to specified bucket as parquet
    """
    files_columns = helper.convert_multiple_files_to_parquet(files_folder)
    files_folder = "parquets/"
    subdirs = [x[0] for x in os.walk(files_folder) if x[0] != files_folder]
    subfolder_names = [i[i.find('/')+1:] for i in subdirs]
    for i in range(len(subdirs)):
        subdir = subdirs[i]
        subflolder_name = subfolder_names[i]
        upload_multiple_files_to_s3_bucket(subdir, bucket_name, f"{s3_bucket_path}/{subflolder_name}/", aws_connection, 'parquet')
    return files_columns


def create_emr_cluster(emr_params, aws_connection, applications):
    """
    Cretates an emr cluster with the specified apps
    Returns only if the cluster is fully initialized or terminated
    """
    emr_params['Applications'] = [{'Name':i.lower().title()} for i in applications]
    aws_emr_client = create_aws_client_connection('emr', aws_connection)
    try:
        response = aws_emr_client.run_job_flow(**emr_params)
        cluster_id = response['JobFlowId']
        logging.info(f"\n\tEMR Cluster {cluster_id} is being created.\n")
        while True:
            response = aws_emr_client.describe_cluster(ClusterId=cluster_id)
            state = response['Cluster']['Status']['State']
            if state.upper() in ['WAITING','RUNNING']:
                break
            elif state.upper() in ['TERMINATED','TERMINATED_WITH_ERRORS']:
                logging.info(f"\n\tEMR Cluster {cluster_id} was terminated. Please check AWS Console for the reason.\n")
                sys.exit()
            logging.info(f"Cluster {cluster_id} is still being created::: Current state: {state}")
            time.sleep(30)  # Wait for 30 seconds before checking again


        # Get the master node public DNS
        master_node_dns = response['Cluster']['MasterPublicDnsName']
        logging.info(f"EMR cluster {cluster_id} is ready.")
        logging.info(f"Master node DNS: {master_node_dns}\n")
    except Exception as ex:
        logging.info(ex)
        sys.exit(1)
    return cluster_id, master_node_dns


def execute_hive_query(cluster_id, aws_connection, query):
    """
    Builds a hive execution statement
    """
    logging.info(f"\n\tExecuting Hive Query: {query} on EMR")
    hive_step = {
        'Name': 'HiveCommand',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['hive', '-e', query]
        }
    }
    execute_emr_command(cluster_id, aws_connection, hive_step)

def execute_emr_command(cluster_id, aws_connection, cmd_steps):
    """
    Executes a command on the cluster CLI
    """
    try:
        aws_emr_client = create_aws_client_connection('emr', aws_connection)
        response = aws_emr_client.add_job_flow_steps(JobFlowId=cluster_id, Steps=[cmd_steps])
        # Wait for the step to complete
        step_id = response['StepIds'][0]
        aws_emr_client.get_waiter('step_complete').wait(ClusterId=cluster_id, StepId=step_id)
        print(f'Command executed on EMR cluster {cluster_id}')
    except Exception as ex:
        logging.exception(ex)
        sys.exit(1)


def copy_parquets_to_hdfs(cluster_id, s3_bucket_name, s3_bucket_path, aws_connection, hdfs_folder_path=None):
    """

    Copy filles from the specified s3 bucket to specified hdfs location
    """
    logging.info("\n")
    if hdfs_folder_path is None:
        hdfs_folder_path = f"/user/hadoop/{s3_bucket_path}".replace("//", '/')
    if 'hdfs://' not in hdfs_folder_path:
        hdfs_folder_path = f"hdfs://{hdfs_folder_path}"
    s3_location = f"s3://{s3_bucket_name}/{s3_bucket_path.replace('//', '/')}"
    logging.info(f"Copying parquet files from s3 location {s3_location} to HDFS folder: {hdfs_folder_path}")
    steps = {
        'Name': 'CLICommand',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['s3-dist-cp', '--src', s3_location, '--dest', hdfs_folder_path]
        }
    }
    execute_emr_command(cluster_id, aws_connection, steps)
    return hdfs_folder_path


def execute_table_create_statements(cluster_id, aws_connection, files_column_types, schema, hdfs_files_path):
    """
    Builds a executes a hive table create statement for various tables in hive
    """
    create_statements = ""
    for file_ in files_column_types:
        create_table_sql = f"create table {schema}.{file_.get('table_name')} ({file_.get('columns')}) " + \
                           "row format delimited fields terminated by '\\t' " + \
                           "lines terminated by '\\n' stored as parquet" + \
                           f" location '{hdfs_files_path}{file_.get('table_name')}'; "
        create_statements+= create_table_sql
    try:
        execute_hive_query(cluster_id, aws_connection, create_statements)
        logging.info(f"Tables Created successfully.\n{[file_.get('table_name') for file_ in files_column_types]}\n")

    except Exception as e:
        logging.error(e)
        sys.exit(1)


def delete_s3_bucket(bucket_name, aws_connection):
    """
    Deletes an S3 bucket
    """
    # Create a Boto3 S3 client
    aws_s3_client = create_aws_client_connection('s3', aws_connection)
    # Empty the bucket by deleting all objects
    objects = aws_s3_client.list_objects(Bucket=bucket_name).get('Contents', [])
    for obj in objects:
        aws_s3_client.delete_object(Bucket=bucket_name, Key=obj['Key'])
    # Delete the empty bucket
    aws_s3_client.delete_bucket(Bucket=bucket_name)
    logging.info(f"The bucket {bucket_name} has been deleted.")

class CustomThread(Thread):
    """
    A custom thread class that extends the functionality of the default Thread class.

    Parameters:
    - group: None - Unused parameter from the parent class.
    - target: callable - The function to be called when the thread is started.
    - name: str - A name for the thread.
    - args: tuple - The arguments passed to the target function.
    - kwargs: dict - The keyword arguments passed to the target function.
    - Verbose: None - Unused parameter from the parent class.

    Example Usage:
    thread = CustomThread(target=my_function, name='Thread 1', args=(arg1, arg2), kwargs={'key': 'value'})
    thread.start()
    thread.join()
    result = thread._return
    """
    def __init__(self, group=None, target=None, name=None, args=(), kwargs={}, Verbose=None):
        Thread.__init__(self, group, target, name, args, kwargs)

    def run(self):
        """
        Overrides the run method in the Thread class.
        Invokes the target function with specified arguments when the thread is started.
        """
        if self._target is not None:
            self._return = self._target(*self._args, **self._kwargs)

    def join(self):
        """
        Overrides the join method in the Thread class.
        Waits for the thread to complete and returns the result from the target function.
        """
        Thread.join(self,)
        return self._return


