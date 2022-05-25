import json
import boto3
import pandas as pd
import snowflake.connector
#import logging
#logger = logging.getLogger()
#logger.setLevel(logging.INFO)


def get_secret_value(name, version=None):
    secrets_client = boto3.client("secretsmanager")
    kwargs = {'SecretId': name}
    if version is not None:
        kwargs['VersionStage'] = version
    response = secrets_client.get_secret_value(**kwargs)
    return response


def connect_snowflake():
    snowflakedict = get_secret_value('snowflake')
    conn = snowflake.connector.connect(
        account=json.loads(snowflakedict['SecretString'])['account'],
        user=json.loads(snowflakedict['SecretString'])['rivery_user'],
        password=json.loads(snowflakedict['SecretString'])['rivery_password'],
        #database=json.loads(snowflakedict['SecretString'])['database'],
        #schema=json.loads(snowflakedict['SecretString'])['schema'],
        warehouse=json.loads(snowflakedict['SecretString'])['warehouse'],
        role=json.loads(snowflakedict['SecretString'])['rivery_role'],
        numpy=True)
    return conn


def generate_active_integration_pivot_utc():
    conn = connect_snowflake()
    snf_query = "select distinct integration_name from HIBOB_PROD_DB.STG.active_integration_scd "
    df_snowflake = pd.read_sql_query(snf_query, conn)  # query call
    aa1 = ','.join("'{0}'".format(x) for x in df_snowflake.INTEGRATION_NAME.values)
    snf_query = "create or replace table HIBOB_PROD_DB.STG.active_integration_pivot_utc as (select * from HIBOB_PROD_DB.STG.active_integration_utc pivot ( count(integration_name) for integration_name in ( "  +aa1+"))as p);"
    output = conn.execute_string(snf_query)
    for el in aa1.split(','):
        snf_query = 'alter table HIBOB_PROD_DB.STG.active_integration_pivot_utc rename column "' + el + '" to "' + el.replace("\'","")+'"'
        output = conn.execute_string(snf_query)
    aa1 = '+'.join("'{0}'".format(x) for x in df_snowflake.INTEGRATION_NAME.values)
    snf_query ='create or replace table HIBOB_PROD_DB.STG.active_integration_pivot_utc as (select *,sum('+aa1+') over (partition by  DATE_DATE,COMPANY_ID ) as Total_Integrations from HIBOB_PROD_DB.STG.active_integration_pivot_utc)'
    snf_query = snf_query.replace('\'','\"')
    output = conn.execute_string(snf_query)


def lambda_handler(event, context):
    generate_active_integration_pivot_utc()
    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "message": "hello world load",
            }
        ),
    }