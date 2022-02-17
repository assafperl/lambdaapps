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


def sf_record_list(sf_obj,sf):
    Q_query = "SELECT  Label, QualifiedApiName FROM FieldDefinition " \
              "WHERE EntityDefinition.QualifiedApiName IN ('{}')".format(sf_obj)
    df = pd.DataFrame(sf.query(Q_query))[["records"]]
    df = df.join(pd.DataFrame(df.records.tolist(), index=df.index).add_prefix('field_'))
    df = df[df["field_QualifiedApiName"].str.contains("__c")]
    return df


def uplaod_sf_snf_contacts():
    salesforcedict = get_secret_value('salesforce')
    sf = Salesforce(username=json.loads(salesforcedict['SecretString'])['username'],
                    password=json.loads(salesforcedict['SecretString'])['password'],
                    security_token=json.loads(salesforcedict['SecretString'])['security_token'])

    sf_object = 'BI_Contacts__c'
    snf_table = 'INTSF_CONTACTS'
    snf_sf_external_field = 'EXTERNALID__c'

    str_sf_query = 'SELECT id,{} FROM {} order by id asc'.format(snf_sf_external_field,sf_object)
    df_sf = pd.DataFrame(sf.query_all(str_sf_query)["records"])
    df_sf = df_sf.drop(columns = ["attributes"])

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

    df_sf_field = sf_record_list(sf_object, sf)  # returns df - sf label and field api name
    str_sf_field = ','.join(
        df_sf_field['field_Label'].tolist())  # only query custom fields that exist in salesforce obj
    snf_query = "SELECT {} FROM {}".format(str_sf_field, snf_table)
    df_snowflake = pd.read_sql_query(snf_query, conn)  # query call
    df_snowflake["runtime"] = df_snowflake["RUNTIME"].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    df_snowflake["emp_startdate"] = df_snowflake["EMP_STARTDATE"].dt.strftime('%Y-%m-%d')
    df_snowflake["pendo_last_nps_timestamp"] = df_snowflake["PENDO_LAST_NPS_TIMESTAMP"].dt.strftime(
        '%Y-%m-%dT%H:%M:%SZ')
    df_snowflake.columns = [str((col.upper())) + '__c' for col in
                            df_snowflake.columns]  # turn column names to __c for upload
    df_sf = df_sf.merge(df_snowflake, how='outer', on='EXTERNALID__c')
    df_sf_insert = df_sf[df_sf["Id"].isnull()].drop(columns=["Id"])
    s3 = boto3.resource('s3')
    if not df_sf_insert.empty:
        logger.info('## generating insert file')
        data_T_insert = list(df_sf_insert.T.to_dict().values())
        jdata = json.dumps(data_T_insert)
        s3.Bucket('integrationbibob').put_object(Key='insert-contacts.json', Body=jdata)
        logger.info(str(len(data_T_insert)) + 'records inserted into insert-contacts.json')

    df_sf_update = df_sf[df_sf["Id"].notnull()]  # should unify lines
    df_sf_update = df_sf_update[df_sf_update["EMAIL__c"].notnull()]  # should unify lines
    logger.info('## generating update file')
    data_T_update = list(df_sf_update.T.to_dict().values())
    jdata = json.dumps(data_T_update)
    s3.Bucket('integrationbibob').put_object(Key='update-contacts.json', Body=jdata)
    logger.info(str(len(data_T_update)) + 'records inserted into update-contacts.json')
    logger.info('## finished')


def lambda_handler(event, context):
    uplaod_sf_snf_contacts()
    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "message": "hello world load",
            }
        ),
    }
