import json
import boto3
import snowflake.connector
import logging
import pandas as pd

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


def invoke_lambda(company,client):
    inputParams = {
    "company"   : company
    }
    response = client.invoke(
    FunctionName = 'arn:aws:lambda:eu-west-1:486592736240:function:employee-by-company-EmployeeByCompanyFunction-bio914eq9tx2',
    InvocationType = 'Event',
    Payload = json.dumps(inputParams))


def get_companies(conn):
    snf_query = 'select distinct COMPANY_ID from HIBOB_PROD_DB.ODS_SNAPSHOT.BOB_HIBOB_SN_EMPLOYEE where date_::date = (select max(date_)::date from  HIBOB_PROD_DB.ODS_SNAPSHOT.BOB_HIBOB_SN_EMPLOYEE)'
    return pd.read_sql_query(snf_query, conn)

def lambda_handler(event, context):
    client = boto3.client('lambda')
    conn = get_snowflake_con()
    companies = get_companies(conn)
    for company in companies['COMPANY_ID']:
        invoke_lambda(company,client)
    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "message": "hello world load",
            }
        ),
    }