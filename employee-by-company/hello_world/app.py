import json
import boto3
import snowflake.connector
import logging
import os
import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)


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
        database=json.loads(snowflakedict['SecretString'])['prod_database'],
        warehouse=json.loads(snowflakedict['SecretString'])['warehouse'],
        role=json.loads(snowflakedict['SecretString'])['role'],
        insecure_mode=True,
        numpy=True)
    return conn


def call_sp(company, conn):
    snf_query = 'call STG.EMPLOYEE_BY_COMPANY_SP(' + str(company) + ')'
    logger.info(snf_query)
    conn.cursor().execute(snf_query)


def lambda_handler(event, context):
    company = event['company']
    conn = get_snowflake_con()
    call_sp(company, conn)
    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "message": "hello world",
            }
        ),
    }