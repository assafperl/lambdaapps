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
os.environ['BUCKET_NAME'] = 'integrationbobbi'


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


def push_to_s3(df):
    s3 = boto3.resource('s3')
    destination = "toggles/company_toggle_" + str(datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S')) + '.csv'
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    logger.info(destination + ' file loaded into the bucket ')
    s3.Bucket(os.environ['BUCKET_NAME']).put_object(Key=destination, Body=csv_buffer.getvalue())
    return destination


def get_df_snowflake(conn):
    snf_query = 'SELECT distinct TABLE_NAME FROM INFORMATION_SCHEMA."TABLES" WHERE TABLE_SCHEMA = \'DWH_SNAPSHOT\' and last_altered > current_date - 1'
    return pd.read_sql_query(snf_query, conn)


def backup_snapshot():
    s3 = boto3.resource('s3')
    conn = get_snowflake_con()
    df_snowflake = get_df_snowflake(conn)
    bucket = s3.Bucket(os.environ['BUCKET_NAME'])
    bucket.objects.filter(Prefix="backup-snapshot/current").delete()
    for table_name in df_snowflake['TABLE_NAME']:
        file_name = table_name +'_'+ str(datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S'))
        conn.cursor().execute("copy into @poc_s3_stage55/backup-snapshot/current/"+ file_name +" from "+ table_name +" file_format = (type = csv field_delimiter = ',' record_delimiter = '\n' skip_header = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '0x22');")
    #conn.cursor().execute("copy into " + os.environ['DEV_DB'] + "." + os.environ['SNAPSHOT_SCHEMA'] + ".PRDT_SITES_UTC_TEST from @poc_s3_stage55/backup-snapshot/ pattern = '.*PRDT_SITES_UTC*.*'  file_format = (type = csv field_delimiter = ',' record_delimiter = '\n' skip_header = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' COMPRESSION=GZIP);")

    today = datetime.datetime.now()
    if today.day == 1:
        bucket.objects.filter(Prefix="backup-snapshot/firstdayofmonth").delete()
        bucket = s3.Bucket(os.environ['BUCKET_NAME'])
        for file in bucket.objects.filter(Prefix="backup-snapshot/current/").all():
            copy_source = {
            'Bucket': os.environ['BUCKET_NAME'],
            'Key':  file.key
            }
            bucket.copy(copy_source,'backup-snapshot/firstdayofmonth/'+file.key[26:])


def lambda_handler(event, context):
    backup_snapshot()
    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "message": "hello world load",
            }
        ),
    }