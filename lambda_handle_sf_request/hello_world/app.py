import boto3
import json
import io
from simple_salesforce import Salesforce
import logging
import urllib
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


def get_sf_table(json_file_):
    return {
        "insert-contracts.json": "BI_Contract__c",
        "delete-accounts.json": "Product_data__c",
        "insert-contacts.json": "BI_Contacts__c",
        "update-contacts.json": "BI_Contacts__c",
        "update-contracts.json": "BI_Contract__c",
        "update-accounts.json": "Product_data__c",
        "insert-accounts.json": "Product_data__c",
        "insert-real-contacts.json": "Contact",
        "update-real-contacts.json": "Contact"
    }[json_file_]


def load_chunks(lst, n, json_file,sf):
    for i in range(0, len(lst), n):
        #sf.bulk.BI_Contacts__c.update(lst[i:i + n])
        logger.info(f'{json_file[:6]} ## statement')
        command_string = 'sf.bulk.' + get_sf_table(json_file) + '.'+json_file[:6] + '(lst[i:i + n])'
        logger.info(command_string)
        rtn = exec(command_string)
        logger.info(rtn)


def handle_salesforce(event):
    if event:
        bucket = event['Records'][0]['s3']['bucket']['name']
        json_file = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')

        salesforcedict = get_secret_value('salesforce')
        sf = Salesforce(username=json.loads(salesforcedict['SecretString'])['username'],
                        password=json.loads(salesforcedict['SecretString'])['password'],
                        security_token=json.loads(salesforcedict['SecretString'])['security_token'])

        s3 = boto3.resource('s3')
        data_in_bytes = s3.Object(bucket, json_file).get()['Body'].read()
        decoded_data = data_in_bytes.decode('utf-8')
        stringio_data = io.StringIO(decoded_data)
        data = stringio_data.readlines()

        logger.info(f'{json_file} ## data')

        json_data = list(map(json.loads, data))

        if ((len(json_data) != 0) and (json_data != '0')):
            lst = json_data[0]
            load_chunks(lst, 8000, json_file, sf)
            logger.info(f'{len(lst)}## executing ')
        else:
            logger.info('## nothing happened')

        s3.Object(bucket, 'history/' + json_file[:-5] + str(datetime.datetime.now()) + json_file[-5:]).copy_from(
            CopySource=bucket + '/' + json_file)


def lambda_handler(event, context):
    handle_salesforce(event)
    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "message": "hello world 1",
            }
        ),
    }