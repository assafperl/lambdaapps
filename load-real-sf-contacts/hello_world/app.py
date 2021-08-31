import json
import boto3
import pandas as pd
from simple_salesforce import Salesforce
import snowflake.connector
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def get_secret_value(name, version=None):
    secrets_client = boto3.client("secretsmanager")
    kwargs = {'SecretId': name}
    if version is not None:
        kwargs['VersionStage'] = version
    response = secrets_client.get_secret_value(**kwargs)
    return response


def gen_insert_update_json_contacts():
    snowflakedict = get_secret_value('snowflake')
    conn = snowflake.connector.connect(
        account=json.loads(snowflakedict['SecretString'])['account'],
        user=json.loads(snowflakedict['SecretString'])['user'],
        password=json.loads(snowflakedict['SecretString'])['password'],
        database=json.loads(snowflakedict['SecretString'])['database'],
        schema=json.loads(snowflakedict['SecretString'])['schema'],
        warehouse=json.loads(snowflakedict['SecretString'])['warehouse'],
        role=json.loads(snowflakedict['SecretString'])['role'],
        numpy=True)
    s3 = boto3.resource('s3')

    snf_table_insert = 'INTSF_INSERTADMINCONTACTS'  # table name from snowflake insert
    snf_table_update = 'INTSF_UPDATEADMINCONTACTS'  # table name from snowflake update
    snf_query_insert = "SELECT * FROM {}".format(snf_table_insert)
    snf_query_update = "SELECT * FROM {}".format(snf_table_update)
    df_snowflake_insert = pd.read_sql_query(snf_query_insert, conn)  # query call
    df_snowflake_update = pd.read_sql_query(snf_query_update, conn)  # query call


    if not df_snowflake_insert.empty:
        logger.info('## generating insert file')
        data_T_insert = list(df_snowflake_insert.T.to_dict().values())
        jdata = json.dumps(data_T_insert)
        s3.Bucket('integrationbobbi').put_object(Key='insert-real-contacts.json', Body=jdata)
        logger.info(str(len(data_T_insert)) + ' records inserted into insert-real-contacts.json')
        #logger.info(f'{data_T_insert} into insert-real-contacts.json')
    if not df_snowflake_update.empty:
        logger.info('## generating update file')
        data_T_update = list(df_snowflake_update.T.to_dict().values())
        jdata = json.dumps(data_T_update)
        s3.Bucket('integrationbobbi').put_object(Key='update-real-contacts.json', Body=jdata)
        logger.info(str(len(data_T_update)) + ' records been inserted into update-real-contacts.json')
        #logger.info(f'{data_T_update} into update-real-contacts.json')


def lambda_handler(event, context):
    """Sample pure Lambda function

    Parameters
    ----------
    event: dict, required
        API Gateway Lambda Proxy Input Format

        Event doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html#api-gateway-simple-proxy-for-lambda-input-format

    context: object, required
        Lambda Context runtime methods and attributes

        Context doc: https://docs.aws.amazon.com/lambda/latest/dg/python-context-object.html

    Returns
    ------
    API Gateway Lambda Proxy Output Format: dict

        Return doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html
    """
    gen_insert_update_json_contacts()
    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "message": "hello world",
            }
        ),
    }
