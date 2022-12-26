import boto3
import json
import tableauserverclient as TSC
import logging
import urllib3
logger = logging.getLogger()
logger.setLevel(logging.INFO)
urllib3.disable_warnings()

def get_secret_value(name, version=None):
    secrets_client = boto3.client("secretsmanager")
    kwargs = {'SecretId': name}
    if version is not None:
        kwargs['VersionStage'] = version
    response = secrets_client.get_secret_value(**kwargs)
    return response


def refresh_tableau_workbooks_datasources_by_tag(event):
    tableaudict = get_secret_value('hibob-mis-prod-eu-west-1-sm-prod-mis-tableau')
    name = json.loads(tableaudict['SecretString'])['srv_token_name']
    token = json.loads(tableaudict['SecretString'])['srv_token']
    url = json.loads(tableaudict['SecretString'])['srv_url']
    tag = event['queryStringParameters']['tag']
    if tag == '':
        return
    tableau_auth = TSC.PersonalAccessTokenAuth(name, token, '')
    server = TSC.Server(url)
    server.add_http_options({'verify': False})
    server.use_server_version()
    req_option = TSC.RequestOptions()
    req_option.filter.add(TSC.Filter(TSC.RequestOptions.Field.Tags,
                                     TSC.RequestOptions.Operator.Equals,
                                     tag))
    with server.auth.sign_in(tableau_auth):
        all_datasources, pagination_item = server.datasources.get(req_option)
        for datasource in all_datasources:
            if datasource.tags:
                if tag in [tag.lower() for tag in datasource.tags]:
                    resource = server.datasources.get_by_id(datasource.id)
                    try:
                        server.datasources.refresh(resource)
                        logger.info('refresh datasource' + datasource.name)
                    except Exception as e:
                        logger.info('error  datasource' + datasource.name + 'massage: ' + str(e))
        all_workbooks, pagination_item = server.workbooks.get(req_option)
        for workbook in all_workbooks:
            if workbook.tags:
                if tag in [tag.lower() for tag in workbook.tags]:
                    resource = server.workbooks.get_by_id(workbook.id)
                    try:
                        server.workbooks.refresh(resource)
                        logger.info('refresh workbook' + workbook.name)
                    except Exception as e:
                        logger.info('error  workbook' + workbook.name + 'massage: ' + str(e))


def lambda_handler(event, context):
    refresh_tableau_workbooks_datasources_by_tag(event)
    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "message": "hello world",
            }
        ),
    }