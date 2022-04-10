import snowflake.connector
import json
import boto3
import pandas as pd
from simple_salesforce import Salesforce
import snowflake.connector
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def get_secret_value(name, version=None):
    """Gets the value of a secret.

    Version (if defined) is used to retrieve a particular version of
    the secret.

    """
    secrets_client = boto3.client("secretsmanager")
    kwargs = {'SecretId': name}
    if version is not None:
        kwargs['VersionStage'] = version
    response = secrets_client.get_secret_value(**kwargs)
    return response


def sf_record_list(sf_obj, sf):
    Q_query = "SELECT  Label, QualifiedApiName FROM FieldDefinition " \
              "WHERE EntityDefinition.QualifiedApiName IN ('{}')".format(sf_obj)
    df = pd.DataFrame(sf.query(Q_query))[["records"]]
    df = df.join(pd.DataFrame(df.records.tolist(), index=df.index).add_prefix('field_'))
    df = df[df["field_QualifiedApiName"].str.contains("__c")]
    l_drop_col = ['Last_Modified_Today__c']
    df = df[df["field_QualifiedApiName"].str.contains(','.join(l_drop_col)) == False]
    return df


def uplaod_sf_snf_account():
    salesforcedict = get_secret_value('salesforce')
    sf = Salesforce(username=json.loads(salesforcedict['SecretString'])['username'],
                    password=json.loads(salesforcedict['SecretString'])['password'],
                    security_token=json.loads(salesforcedict['SecretString'])['security_token'])

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

    ##### sf Delete Records
    ########
    # This code segment will delete data from production object inside salesforce, which is no longer relevant for an
    # integration
    ###########
    s3 = boto3.resource('s3')
    sf_object = 'Product_data__c'
    snf_table = 'INTSF_ACCOUNT'
    snf_sf_external_field = 'EXTERNALID__c'

    str_get_all_product_bi = "SELECT id FROM {} WHERE SF_ACCOUNTID__c = 'Unknown'".format(sf_object)
    df_sf_del_product_bi = pd.DataFrame(sf.query_all(str_get_all_product_bi)["records"])
    if (df_sf_del_product_bi.empty == False):
        logger.info('## generating delete file')
        df_sf_del_product_bi = df_sf_del_product_bi.drop(columns=["attributes"])
        data_T = list(df_sf_del_product_bi.T.to_dict().values())
        jdata = json.dumps(data_T)
        s3.Bucket('integrationbibob').put_object(Key='delete-accounts.json', Body=jdata)
        logger.info(str(len(data_T)) + 'records inserted into delete-accounts.json')
        # When empty (no items to delete) then skip

    #########################################

    df_sf_field = sf_record_list(sf_object, sf)
    str_sf_field = ','.join(
        df_sf_field['field_Label'].tolist())  # only query custom fields that exist in salesforce obj
    snf_query = "SELECT {} FROM {}".format(str_sf_field, snf_table)
    df_snowflake = pd.read_sql_query(snf_query, conn)  # query call
    df_snowflake["runtime"] = df_snowflake["RUNTIME"].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    df_snowflake["bob_last_billing_date"] = df_snowflake["bob_last_billing_date".upper()].dt.strftime(
        '%Y-%m-%dT%H:%M:%SZ')
    df_snowflake["bob_next_billing_date"] = df_snowflake["bob_next_billing_date".upper()].dt.strftime(
        '%Y-%m-%dT%H:%M:%SZ')
    df_snowflake["pm_churn_effective_date"] = df_snowflake["pm_churn_effective_date".upper()].dt.strftime(
        '%Y-%m-%dT%H:%M:%SZ')
    df_snowflake["sensitive_consent_enddate"] = df_snowflake["sensitive_consent_enddate".upper()].dt.strftime(
        '%Y-%m-%dT%H:%M:%SZ')
    df_snowflake["support_consent_enddate"] = df_snowflake["support_consent_enddate".upper()].dt.strftime(
        '%Y-%m-%dT%H:%M:%SZ')
    df_snowflake.columns = [str((col.upper())) + '__c' for col in
                            df_snowflake.columns]  # turn column names to __c for upload

    ## End of Snowflake list preparation

    str_sf_query = 'SELECT id,{} FROM {}'.format(snf_sf_external_field, sf_object)
    df_sf = pd.DataFrame(sf.query_all(str_sf_query)["records"])
    df_sf = df_sf.drop(columns=["attributes"])
    df_sf = df_sf.merge(df_snowflake, how='outer', on=snf_sf_external_field)


    df_sf_insert = df_sf[df_sf["Id"].isnull()].drop(columns=["Id"])

    if not df_sf_insert.empty:
        logger.info('## generating insert file')
        data_T_insert = list(df_sf_insert.T.to_dict().values())
        jdata = json.dumps(data_T_insert)
        s3.Bucket('integrationbibob').put_object(Key='insert-accounts.json', Body=jdata)
        logger.info(str(len(data_T_insert)) + 'records inserted into insert-accounts.json')

    df_sf_update = df_sf[df_sf["Id"].notnull()]  # should unify lines
    df_sf_update = df_sf_update[df_sf_update["LAST_NAME__c"].notnull()]  # should unify lines
    logger.info('## generating update file')
    data_T_update = list(df_sf_update.T.to_dict().values())
    jdata = json.dumps(data_T_update)
    s3.Bucket('integrationbibob').put_object(Key='update-accounts.json', Body=jdata)
    logger.info(str(len(data_T_update)) + 'records inserted into update-accounts.json')
    logger.info('## finished')

def lambda_handler(event, context):
    uplaod_sf_snf_account()
    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "message": "hello world load",
            }
        ),
    }
