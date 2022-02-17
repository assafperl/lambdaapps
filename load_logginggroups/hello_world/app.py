from time import time
import hashlib
import hmac
import base64
import time
import json
import datetime
import requests
import boto3
import pandas as pd
import snowflake.connector
import logging
import os
from ast import literal_eval
import io
from requests.packages.urllib3.exceptions import InsecureRequestWarning
logger = logging.getLogger()
logger.setLevel(logging.INFO)
import pytz
pacific = pytz.timezone('UTC')


def get_secret_value(name, version=None):
    secrets_client = boto3.client("secretsmanager")
    kwargs = {'SecretId': name}
    if version is not None:
        kwargs['VersionStage'] = version
    response = secrets_client.get_secret_value(**kwargs)
    return response


def get_product_secret():
    product_secret = get_secret_value('product')
    return bytes(json.loads(product_secret['SecretString'])['product_api'], encoding='utf-8')


def get_snowflake_con():
    snowflakedict = get_secret_value('snowflake')
    conn = snowflake.connector.connect(
        account=json.loads(snowflakedict['SecretString'])['account'],
        user=json.loads(snowflakedict['SecretString'])['user'],
        password=json.loads(snowflakedict['SecretString'])['password'],
        database=os.environ['PROD_DB'],
        schema=os.environ['SNAPSHOT_SCHEMA'],
        warehouse=json.loads(snowflakedict['SecretString'])['warehouse'],
        role=json.loads(snowflakedict['SecretString'])['role'],
        insecure_mode=True,
        numpy=True)
    return conn


def get_df_snowflake(conn):
    snf_query = 'SELECT COMPANY_ID ,NAME FROM ' + os.environ[
        'DEV_DB'] + '.ods.prdt_hibob_public_company where  test_account = false'
    return pd.read_sql_query(snf_query, conn)


def get_headers(body, secret):
    epoch_time = int(time.time() * 1000)
    bodyAsString = str(json.dumps(body))
    bodyAndTime = '/api/billing-groups/admin/companies/query:'+ str(epoch_time) + ':application/json:' + bodyAsString
    sign = hmac.new(secret, bodyAndTime.encode('utf8'), hashlib.sha512).digest()
    encoded = base64.b64encode(sign)
    return {
        "Date": str(epoch_time),
        "Authentication": encoded,
        "Content-Type": "application/json; charset=UTF-8","Accept":"application/json" ,
    }


def get_normalized_df(result, df_snowflake):
    result = result.content.decode("utf-8")
    array_of_array = literal_eval(result)
    out_columns = ['bob_company', 'togle']
    nl = []
    for zz in array_of_array:
        aa = {}
        for key, val in zip(out_columns, zz):
            aa[key] = val
        nl.append(aa)
    df = pd.DataFrame(nl)
    split1 = df.apply(lambda x: pd.Series(x['togle']), axis=1).stack().reset_index(level=1, drop=True)
    df = df.merge(split1.to_frame(), left_index=True, right_index=True)
    df.columns = ['bob_company', 'togle', 'togle1']
    df_snowflake['Runtime_timestamp'] = str(datetime.datetime.now())
    df = df.merge(df_snowflake, left_on='bob_company', right_on='COMPANY_ID')[
        ['COMPANY_ID', 'NAME', 'togle1', 'Runtime_timestamp']]
    df['Date_'] = datetime.date.today() - datetime.timedelta(days=1)
    df = df[['Date_', 'COMPANY_ID', 'NAME', 'togle1', 'Runtime_timestamp']]
    df.columns = ['Date_', 'Company_ID', 'Company_name', 'Toggle_Name', 'Runtime_timestamp']
    df = df.replace('\n', '', regex=True)
    return df


def push_to_s3(df):
    s3 = boto3.resource('s3')
    destination = "billing-groups/billing_groups_" + str(datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S')) + '.csv'
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    logger.info(destination +' file loaded into the bucket ')
    s3.Bucket(os.environ['BUCKET_NAME']).put_object(Key=destination, Body=csv_buffer.getvalue())
    return destination




def upload_logging_groups_utc():
    secret = get_product_secret()
    conn = get_snowflake_con()
    df_snowflake = get_df_snowflake(conn)
    body = {"companyIds": df_snowflake.COMPANY_ID.tolist()}
    headers = get_headers(body, secret)
    result = requests.post('https://app.hibob.com/api/billing-groups/admin/companies/query',
                            data=json.dumps(body),json=json.dumps(body),
                               headers=headers, verify=False)
    result = result.content.decode("utf-8")
    array_of_array = literal_eval(result)
    column_names = ["company_id","billingGroupId", "snapshotId", "numberOfEmployees"]
    df_all = pd.DataFrame(columns = column_names)
    for e1 in array_of_array:
        for e in e1[1]:
            if df_all.empty:
                df_all = pd.json_normalize(e)
                df_all['company_id'] = e1[0]
            else:
                df = pd.json_normalize(e)
                df['company_id'] = e1[0]
                df_all = df_all.append(df, ignore_index=True)
    df_all['runtime'] = datetime.datetime.now(tz=pacific).strftime('%Y-%m-%dT%H:%M:%SZ')
    df_all = df_all[['company_id','billingGroupId','numberOfEmployees','runtime']]
    push_to_s3(df_all)


def lambda_handler(event, context):
    upload_logging_groups_utc()
    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "message": "hello world load",
            }
        ),
    }