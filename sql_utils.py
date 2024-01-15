import os
import sys
sys.path.append(os.path.dirname(__file__))
from random import randint
import warnings
warnings.simplefilter('ignore')
import pandas as pd
from sqlalchemy import create_engine, text
import helper_utils as helper
import logging
from urllib.parse import quote

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("debug.log"),
        logging.StreamHandler()
    ]
)

def create_db_engine(conn_parameters, conn_engine='mysql', conn_name='local', db_url='127.0.0.1'):
    """
    Create a database engine based on specified connection parameters.

    Parameters:
    - conn_parameters: list - List of dictionaries containing connection parameters.
    - conn_engine: str - Database engine type (default is 'mysql').
    - conn_name: str - Connection name (default is 'local').
    - db_url: str - Database URL (default is '127.0.0.1').

    Returns:
    - engine: SQLAlchemy engine - Created database engine.
    """
    params = [conn_parameters[i] for i in range(len(conn_parameters)) if conn_parameters[i]['Engine'] == conn_engine and conn_parameters[i]["name"] == conn_name][0]
    logging.info("Creating connection string")
    user = params['user']
    pasw = quote(params['password'])
    port = 3306
    connection_url = f"mysql+mysqlconnector://{user}:{pasw}@{db_url}:{port}"
    engine = create_engine(connection_url)
    return engine

def execute_query(sql, engine):
    """
    Execute a SQL query using the specified engine.

    Parameters:
    - sql: str - SQL query to be executed.
    - engine: SQLAlchemy engine - Database engine.

    Returns:
    - tuple - Result of the query, including status code and message.
    """
    sql = text(sql)
    with engine.connect() as connection:
        try:
            connection.execute(sql)
            connection.commit()
            return (200, "Query run successfully")
        except Exception as ex:
            logging.error(f"Query failed: {sql}\n{ex}")
            return (404, Exception(ex))

def create_schema(schema_name, engine):
    """
    Create a database schema if it does not exist.

    Parameters:
    - schema_name: str - Name of the schema to be created.
    - engine: SQLAlchemy engine - Database engine.
    """
    create_sql = f"CREATE SCHEMA IF NOT EXISTS {schema_name};"
    try:
        execute_query(create_sql, engine)
        execute_query("SET GLOBAL max_allowed_packet=1024*1024*1024;", engine)
        logging.info("Schema created successfully")
    except Exception as ex:
        logging.error(f"Schema was not created.\n{ex}")
        raise ex

def sql_to_df(sql, engine):
    """
    Execute a SQL query and return the result as a Pandas DataFrame.

    Parameters:
    - sql: str - SQL query to be executed.
    - engine: SQLAlchemy engine - Database engine.

    Returns:
    - pd.DataFrame - Result of the query in DataFrame format.
    """
    sql = text(sql)
    with engine.connect() as connection:
        try:
            logging.info("Created connection, running the SQL query.")
            df = pd.read_sql(sql, connection)
            logging.info("Got the result, returning the data")
            return df
        except Exception as ex:
            raise Exception(ex)

def check_table_existance(schema_name, tbl_name, engine):
    """
    Check if a table exists in the specified schema.

    Parameters:
    - schema_name: str - Name of the schema containing the table.
    - tbl_name: str - Name of the table to check.
    - engine: SQLAlchemy engine - Database engine.

    Returns:
    - tuple - Result of the check, including status code and message.
    """
    check_tbl = f"select 1 tbl_exists from {schema_name}.{tbl_name} limit 1;"
    try:
        df = sql_to_df(check_tbl, engine)
        return (200, "Table Exists")
    except:
        return (404, "Table does not exist")

def create_table(schema_name, tbl_name, tbl_str, engine):
    """
    Create a table in the specified schema if it does not exist.

    Parameters:
    - schema_name: str - Name of the schema to contain the table.
    - tbl_name: str - Name of the table to be created.
    - tbl_str: str - SQL statement defining the table structure.
    - engine: SQLAlchemy engine - Database engine.

    Returns:
    - tuple - Result of the table creation, including status code and message.
    """
    (tbl_exists, _) = check_table_existance(schema_name, tbl_name, engine)
    if tbl_exists == 200:
        return (404, "Table Already Exists")
    execute_query(tbl_str, engine)
    return (200, "Table has been created successfully!")

def insert_table_data(schema_name, table_name, df, engine):
    """
    Insert data into the specified table in the specified schema.

    Parameters:
    - schema_name: str - Name of the schema containing the table.
    - table_name: str - Name of the table to insert data into.
    - df: pd.DataFrame - DataFrame containing the data to be inserted.
    - engine: SQLAlchemy engine - Database engine.
    """
    df.to_sql(name=table_name, schema=schema_name, con=engine, if_exists='replace', index=False)

def create_table_and_insert_data(file_path, schema_name, engine, table_name=None):
    """
    Create a table, schema, and insert data from a CSV file into a database.

    Parameters:
    - file_path: str - Path to the CSV file.
    - schema_name: str - Name of the schema to contain the table.
    - engine: SQLAlchemy engine - Database engine.
    - table_name: str - Name of the table to be created (default is None).
    """
    try:
        df = pd.read_csv(file_path)
        helper.preprocess_file(df)
        if table_name is None:
            table_name = helper.get_file_name(file_path)
        tbl_struct = helper.get_table_structure(df)
        create_tbl_str = helper.sql_create_table_statement(tbl_struct, schema_name, table_name)
        create_schema(schema_name, engine)
        logging.info("Finished with schema")
        # if table exists, an error code of 404 with an error is returned, and termination should stop
        create_table_res = create_table(schema_name, table_name, create_tbl_str, engine)
        if create_table_res[0] == 404:
            logging.error(f"Cannot proceed with data insertion")
            logging.error(f"{create_table_res}")
            return
        logging.info(f"{create_table_res}")
        try:
            df.to_sql(name=table_name, schema=schema_name, con=engine, if_exists='replace', index=False)
        except Exception as ex:
            logging.error(ex)
            sys.exit(1)
        logging.info(f"Data inserted into {schema_name}.{table_name} table successfully.")
    except Exception as e:
        print(f"Error: {e}")
