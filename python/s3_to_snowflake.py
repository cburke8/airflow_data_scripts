import requests
import json
import snowflake.connector
import os
from datetime import datetime, timedelta

sf_user='Cjburke83'
sf_password='' # hidden password for public repo, must configure in environment before running
sf_account='mo00387.us-east-2.aws'
database='CITI_BIKE_BATCH_DB'
warehouse='COMPUTE_WH'
schema = 'PUBLIC'
role = 'SYSADMIN'

snow_conn = snowflake.connector.connect(
  user=sf_user,
  password=sf_password,
  account=sf_account
)

def run_query(connection, query):
    cursor = connection.cursor()
    cursor.execute(query)
    cursor.close()
    
def query_production():
    snowflakecursor = snow_conn.cursor()
    try:
        sql = 'use warehouse {}'.format(warehouse)
        run_query(snow_conn, sql) 
    except:
        pass
    try:
        sql = 'alter warehouse {} resume'.format(warehouse)
        run_query(snow_conn, sql)
    except:
        pass
    
    try:
        sql = 'use database {}'.format(database)
        run_query(snow_conn, sql)
        sql = 'use role {}'.format(role)
        run_query(snow_conn, sql)
        sql = 'use schema {}'.format(schema)
        run_query(snow_conn, sql)
        sql = 'COPY into CITI_BIKE_PARQUET_PROD from @citi_bike_s3_stage_prod on_error = skip_file file_format = (type = parquet);'
        run_query(snow_conn, sql)
        sql = 'SELECT * from CITI_BIKE_PARQUET_PROD limit 10;'
        run_query(snow_conn, sql)
        
    except Exception as e:
        print(e)
        
def query_uat():
    snowflakecursor = snow_conn.cursor()
    try:
        sql = 'use warehouse {}'.format(warehouse)
        run_query(snow_conn, sql) 
    except:
        pass
    try:
        sql = 'alter warehouse {} resume'.format(warehouse)
        run_query(snow_conn, sql)
    except:
        pass
    
    try:
        sql = 'use database {}'.format(database)
        run_query(snow_conn, sql)
        sql = 'use role {}'.format(role)
        run_query(snow_conn, sql)
        sql = 'use schema {}'.format(schema)
        run_query(snow_conn, sql)
        sql = 'COPY into CITI_BIKE_PARQUET_UAT from @citi_bike_s3_stage_uat on_error = skip_file file_format = (type = parquet);'
        run_query(snow_conn, sql)
        sql = 'SELECT * from CITI_BIKE_PARQUET_UAT limit 10;'
        run_query(snow_conn, sql)
        
    except Exception as e:
        print(e)

def query_sit():
    snowflakecursor = snow_conn.cursor()
    try:
        sql = 'use warehouse {}'.format(warehouse)
        run_query(snow_conn, sql) 
    except:
        pass
    try:
        sql = 'alter warehouse {} resume'.format(warehouse)
        run_query(snow_conn, sql)
    except:
        pass
    
    try:
        sql = 'use database {}'.format(database)
        run_query(snow_conn, sql)
        sql = 'use role {}'.format(role)
        run_query(snow_conn, sql)
        sql = 'use schema {}'.format(schema)
        run_query(snow_conn, sql)
        sql = 'COPY into CITI_BIKE_PARQUET_SIT from @citi_bike_s3_stage_sit on_error = skip_file file_format = (type = parquet);'
        run_query(snow_conn, sql)
        sql = 'SELECT * from CITI_BIKE_PARQUET_SIT limit 10;'
        run_query(snow_conn, sql)
        
    except Exception as e:
        print(e)
