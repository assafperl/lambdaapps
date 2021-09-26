import json
import boto3
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


def lambda_handler(event, context):
    table_name = event['table-name']
    year = event['year']
    file_name = table_name + '_' + str(datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S'))
    folder_name = str(datetime.datetime.now().strftime('%Y%m%d'))
    conn = get_snowflake_con()
    conn.cursor().execute(
        "copy into @poc_s3_stage55/backup-snapshot/current/"+ folder_name + "/" + file_name + " from (select * from " + table_name + " where date_part(year,date_::date) = " + year + ") file_format = (type = csv field_delimiter = ',' record_delimiter = '\n' skip_header = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '0x22');")

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "message": "hello world",
            }
        ),
    }