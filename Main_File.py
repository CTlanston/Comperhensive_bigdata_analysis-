# Importing Neccessary libraries
import os
import sys
path_ = os.path.dirname(__file__)
sys.path.append(path_)
from libraries import aws_utils as aws
from libraries import sql_utils as sql
from libraries import helper_utils as helper
os.chdir(path_)

# Read the json files
json_load = helper.read_jsons()
# Specifying the schema, aws_connection and emr cluster parameters
schema = "project_schema_001"
aws_connection = json_load['aws_connection']
emr_params = json_load['emr_parameters']

################ CREATING S3 BUCKET ####################
bucket_name = f"isom-671-23-team15-bigdata-project-bucket"
aws.create_s3_bucket(bucket_name, aws_connection)
########################################################

########## Using Spark to load the files ##########
# Use Spark to load the files
# Save them to a new folder as parquet files
# then load the parquet files into s3 bucket
files_folder = "./project_datasets/"
s3_bucket_path = "project_data/parquets/"
# Using Threading to make the process faster
# Basically running one thread in parallel with another
# One thread is responsible for changing csv files to parquets and uploading them to s3 bucket
# The other thread is responsible for creating the emr cluster
# These two processes can run concurrently
thread1 = aws.CustomThread(target=aws.upload_multiple_files_to_s3_bucket_as_parquet, args=(files_folder, bucket_name, s3_bucket_path, aws_connection))
thread1.start()
########################################################

################ CREATE EMR INSTANCE ####################
# Specifying applications
applications = ['hadoop', 'hive', 'hue', 'presto']
# This the second thread to create the cluster
thread2 = aws.CustomThread(target=aws.create_emr_cluster, args=(emr_params, aws_connection, applications))
thread2.start()
cluster_id, cluster_url = thread2.join()
files_column_types = thread1.join()
# Write the connection URL to the auth credentials file
# Here the other analysts do not have to edit any parameters
# To make a connection, the code will load the connection from the json file
helper.write_presto_connector_to_json_file(cluster_url, schema)
########################################################

################ copy the files to hdfs ####################
# default folder is hdfs:///user/hadoop/<parquets path>
hdfs_folder = aws.copy_parquets_to_hdfs(cluster_id, bucket_name, s3_bucket_path, aws_connection)
aws.delete_s3_bucket(bucket_name,aws_connection)
########################################################
################## WRITE HIVE QUERY ####################
## drop schema if exits
drop_schema_query = f"DROP SCHEMA IF EXISTS  {schema} CASCADE;"
aws.execute_hive_query(cluster_id, aws_connection, drop_schema_query)
## create schema
create_hive_schema_query = f"CREATE schema {schema};"
aws.execute_hive_query(cluster_id, aws_connection, create_hive_schema_query)
########################################################
aws.execute_table_create_statements(cluster_id, aws_connection, files_column_types, schema, hdfs_folder)

print(f"\nData can be accessed at this connection string: \n\tjdbc:presto://{cluster_url}:8889/hive")







