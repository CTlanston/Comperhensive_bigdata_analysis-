# Import Necessary Libraries
import os
import sys
sys.path.append(os.path.dirname(__file__))
import json
import warnings
warnings.simplefilter('ignore')
import pandas as pd
import logging
import re
from pyspark.sql import SparkSession

# Initialize Spark session
# Spark is used to load CSV and convert them to Parquets
spark = SparkSession.builder.appName("ConvertFiles").getOrCreate()
spark.conf.set("spark.sql.debug.maxToStringFields", 1000)
# Specify the logging format
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("debug.log"),  # This works only for .py files
        logging.StreamHandler()
    ]
)

def replace_in_string(value):
    """
    :param value:
    :return value:
    Replace any non-alphanumeric characters with an underscore
    Replace two consecutive underscores with a single underscore
    """
    value = re.sub(r'[^a-zA-Z0-9_]', '_', value)
    value = value.replace("__", '_')
    return value

def build_insert_query(dt):
    """
    :param dt:
    :return rows:
    Builds an insert row based on the fields on the row
    Replace any NaN cell field with Null
    """
    rows = dt.values
    rows = ["'" + "', '".join([str(a) for a in row]) + "'" for row in rows]
    rows = "(" + "), (".join(rows) + ")"
    rows = rows.replace("'NaN'", 'NULL')
    rows = rows.replace("'nan'", 'NULL')
    return rows

def get_file_name(file_path):
    """
    :param file_path:
    :return file_name:
    Get a file name given the file path
    Remove the extension from the file name
    """
    last_slash = file_path.rfind("/")
    last_period = file_path.rfind(".")
    if last_slash < last_period:
        file_name = file_path[file_path.rfind("/")+1:file_path.rfind(".")]
    else:
        file_name = file_path[file_path.rfind("/")+1:]
    file_name = replace_in_string(file_name)
    return file_name

def get_file_type(file_path):
    """
    :param file_path:
    :return file_type or '':
    Returns the file_type given the file path
    """
    last_slash = file_path.rfind("/")
    file_name = file_path[last_slash+1:]
    if '.' in file_name:
        last_period = file_name.rfind(".")
        file_type = file_name[last_period+1:]
        return file_type
    else:
        return ''

def preprocess_file(init_df):
    """
    Formats column names to replace non-alphanumeric characters with underscores
    """
    init_df.columns = [col.strip().replace(' ', '_').replace('-','_').replace("__", '_') for col in init_df.columns]
    for col in init_df.columns:
        if init_df[col].isna().sum() == len(init_df):
            init_df.drop(col, axis=1, inplace=True)
        elif init_df.dtypes[col] == object:
            try:
                init_df[col] = pd.to_datetime(init_df[col])
            except:
                pass

def get_table_structure(init_df, dialect='nosql'):
    """
    Builds a table create statement with column names and data types
    Tailored for both SQL and NoSQL databases
    """
    sample_df = init_df.sample(10000, replace=True)
    dt2 = pd.DataFrame(sample_df.dtypes, columns=['Data_Types']).reset_index().rename(columns={'index': 'Column_Name'})
    for i in range(len(dt2)):
        if 'int' in str(dt2.Data_Types[i]):
            maxx, minn = max(sample_df[dt2.Column_Name[i]]), min(sample_df[dt2.Column_Name[i]])
            if maxx < 2**30 and minn > -2**30:  # max for int is 2**31 but we will go with 2**30
                dt2.Data_Types[i] = 'int'
            else:
                dt2.Data_Types[i] = 'bigint'
        elif 'float' in str(dt2.Data_Types[i]):
            dt2.Data_Types[i] = 'double'
        elif str(dt2.Data_Types[i]) == 'object':
            try:
                pd.to_datetime(sample_df[dt2.Column_Name[i]])
                dt2.Data_Types[i] = 'timestamp'
            except:
                maxx = sample_df[dt2.Column_Name[i]].str.len().max()
                if dialect == 'sql' and maxx+5 < 255:
                    dt2.Data_Types[i] = f'varchar({int(maxx+10)})'
                elif dialect == 'sql':
                    dt2.Data_Types[i] = f'text'
                else:
                    dt2.Data_Types[i] = f'string'
    return dt2

def sql_create_table_statement(df, schema_name, table_name):
    """
    Build table create statement
    """
    n = 10000
    sample_df = df.sample(withReplacement=True, fraction=n/df.count()).toPandas()
    tbl_structure = get_table_structure(sample_df, 'nosql')
    if isinstance(tbl_structure, pd.DataFrame):
        cols_ = tbl_structure[tbl_structure.columns[0]]
        dtypes_ = tbl_structure[tbl_structure.columns[1]]
    elif isinstance(tbl_structure, list) or isinstance(tbl_structure, tuple):
        cols_ = tbl_structure[0]
        dtypes_ = tbl_structure[1]
    else:
        raise Exception("Table Structure cannot be determined")
    if len(cols_) != len(dtypes_):
        raise Exception("Table Structure columns and datatypes should be of the same length")
    merged = [f"{cols_[i]} {dtypes_[i]}" for i in range(len(cols_))]
    qry = f"CREATE TABLE {schema_name}.{table_name} ({', '.join(merged)})"
    print(f"\n{qry}\n")
    return qry

def read_jsons():
    """
    Read the JSON files for the parameters
    """
    auth = json.load(open("auth/auth.json"))
    rds = json.load(open("auth/rds_params.json"))
    emr_paramaters = json.load(open("auth/emr_config.json"))
    emr_params = emr_paramaters['emr_cluster_config']
    emr_security_groups = emr_paramaters['emr_security_groups']
    aws_connection = auth['aws_connection']
    rds_parameters = rds['mysql-micro']
    return {
        "aws_connection": aws_connection,
        "emr_parameters": emr_params,
        "emr_security_groups": emr_security_groups,
        "rds_parameters": rds_parameters
    }

def convert_file_to_parquet(file_path):
    """
    Converts a file at the given file_path to a Parquet file
    """
    file_type = get_file_type(file_path)
    if os.path.isfile(file_path) and file_type == 'csv':
        df = spark.read.options(header=True, inferschema=True).load(file_path, format='csv')
        cols_dtypes = [replace_in_string(col[0])+" "+col[1] for col in df.dtypes]
        cols_dtypes = ', '.join(cols_dtypes)
        file_name = get_file_name(file_path)
        df.write.mode('overwrite').parquet(f"parquets/{file_name}")
        return {
            "table_name": file_name,
            "columns": cols_dtypes
        }
    else:
        logging.error((404, f"{file_path} is not a CSV file"))

def convert_multiple_files_to_parquet(local_folder_path):
    """
    Looks through a folder and creates a Parquet alternative of every CSV file
    It reads the file with PySpark and keeps a record of the file schema
    """
    files = os.listdir(local_folder_path)
    file_columns = []
    for file_name in files:
        file_path = os.path.join(local_folder_path, file_name)
        result = convert_file_to_parquet(file_path)
        if result is not None:
            file_columns.append(result)
    return file_columns

def write_presto_connector_to_json_file(cluster_url, schema):
    """
    Writes a cluster URL and the schema name to a JSON
    The analysis file reads these connections and builds
    the Presto connection automatically.
    """
    presto_connector = {
        "host": cluster_url,
        "port": 8889,
        "username": 'hadoop',
        "catalog": 'hive',
        "schema": schema
    }
    auth = json.load(open("auth/auth.json"))
    auth[f"presto_connector"] = presto_connector
    json_object = json.dumps(auth, indent=4)
    try:
        with open("auth/auth.json", "w") as outfile:
            outfile.write(json_object)
    except IOError as io:
        logging.error(io)
        sys.exit(1)
