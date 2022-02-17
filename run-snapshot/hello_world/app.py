import json
import requests
import boto3
import pandas as pd
import snowflake.connector
import logging
import os
import datetime
logger = logging.getLogger()
logger.setLevel(logging.INFO)
os.environ['PROD_DB'] = 'HIBOB_PROD_DB'
os.environ['SNAPSHOT_SCHEMA'] = 'DWH_SNAPSHOT'
os.environ['DEV_DB'] = 'HIBOB_DEV_DB'
os.environ['BUCKET_NAME'] = 'integrationbibob'


def get_secret_value(name, version=None):
    secrets_client = boto3.client("secretsmanager")
    kwargs = {'SecretId': name}
    if version is not None:
        kwargs['VersionStage'] = version
    response = secrets_client.get_secret_value(**kwargs)
    return response

def get_snowflake_con():
    snowflakedict = get_secret_value('snowflake')
    conn = snowflake.connector.connect(
        account=json.loads(snowflakedict['SecretString'])['account'],
        user=json.loads(snowflakedict['SecretString'])['user'],
        password=json.loads(snowflakedict['SecretString'])['password'],
        database=os.environ['DEV_DB'],
        schema=os.environ['SNAPSHOT_SCHEMA'],
        warehouse=json.loads(snowflakedict['SecretString'])['warehouse'],
        role=json.loads(snowflakedict['SecretString'])['role'],
        insecure_mode=True,
        numpy=True)
    return conn


def get_df_snowflake(conn):
    snf_query = 'SELECT distinct TABLE_NAME FROM INFORMATION_SCHEMA."TABLES" WHERE TABLE_SCHEMA = \'DWH_SNAPSHOT\' and last_altered > current_date - 1 and BYTES>1000000000'
    return pd.read_sql_query(snf_query, conn)


def get_df_snowflake_small(conn):
    snf_query = 'SELECT distinct TABLE_NAME FROM INFORMATION_SCHEMA."TABLES" WHERE TABLE_SCHEMA = \'DWH_SNAPSHOT\' and last_altered > current_date - 1 and BYTES<1000000000'
    return pd.read_sql_query(snf_query, conn)


def invoke_lambda(table_name,year,client):
    inputParams = {
    "table-name"   : table_name,
    "year"   : year
    }
    response = client.invoke(
    FunctionName = 'arn:aws:lambda:eu-west-1:486592736240:function:single-snapshot-SingleSnapshotFunction-xx0dlPDfdOUf',
    InvocationType = 'Event',
    Payload = json.dumps(inputParams))


def get_df_snowflake_date(conn,table):
    if table != 'EMP_ROLES':
        snf_query = 'SELECT distinct date_part(year,date_::date) as year from '+ table
        return pd.read_sql_query(snf_query, conn)
    else:
        snf_query = 'SELECT distinct date_part(year,UPDATE_DATE::date) as year from ' + table
        return pd.read_sql_query(snf_query, conn)


def lambda_handler(event, context):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(os.environ['BUCKET_NAME'])
    client = boto3.client('lambda')
    conn = get_snowflake_con()
    df_snowflake = get_df_snowflake(conn)
    for table_name in df_snowflake['TABLE_NAME']:
        #bucket.objects.filter(Prefix="backup-snapshot/current/"+table_name).delete()
        df = get_df_snowflake_date(conn,table_name)
        for index, row in df.iterrows():
            invoke_lambda(table_name,str(row.YEAR), client)
    df_snowflake_small = get_df_snowflake_small(conn)
    for table_name in df_snowflake_small['TABLE_NAME']:
        #bucket.objects.filter(Prefix="backup-snapshot/current/" + table_name).delete()
        file_name = table_name + '_' + str(datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S'))
        folder_name = str(datetime.datetime.now().strftime('%Y%m%d'))
        conn.cursor().execute(
            "copy into @bob_s3_stage55/backup-snapshot/current/" + folder_name + "/" + file_name + " from " + table_name + " file_format = (type = csv field_delimiter = ',' record_delimiter = '\n' skip_header = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '0x22');")

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "message": "hello world load",
            }
        ),
    }