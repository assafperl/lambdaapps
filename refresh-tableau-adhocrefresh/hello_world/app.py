import boto3
import json
import tableauserverclient as TSC
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


def refresh_tableau_workbooks_datasources_by_tag():
    tableaudict = get_secret_value('tableau')
    user = json.loads(tableaudict['SecretString'])['user']
    password = json.loads(tableaudict['SecretString'])['password']
    url = json.loads(tableaudict['SecretString'])['url']
    site = json.loads(tableaudict['SecretString'])['site']

    tableau_auth = TSC.TableauAuth(user, password, site)
    server = TSC.Server(url, use_server_version=True)

    with server.auth.sign_in(tableau_auth):
        all_datasources, pagination_item = server.datasources.get()
        for datasource in all_datasources:
            if datasource.tags:
                if 'adhocrefresh' in [tag.lower() for tag in datasource.tags]:
                    resource = server.datasources.get_by_id(datasource.id)
                    try:
                        server.datasources.refresh(resource)
                        logger.info('refresh datasource' + datasource.name)
                        #print(datasource.name, datasource.tags, datasource.id)
                    except Exception as e:
                        logger.info('error  datasource' + datasource.name +'massage: ' + str(e))
                        #print(datasource.name, e)
        all_workbooks, pagination_item = server.workbooks.get()
        for workbook in all_workbooks:
            if workbook.tags:
                if 'adhocrefresh' in [tag.lower() for tag in workbook.tags]:
                    resource = server.workbooks.get_by_id(workbook.id)
                    try:
                        server.workbooks.refresh(resource)
                        logger.info('refresh workbook' + workbook.name)
                        #print(workbook.name, workbook.tags, workbook.id)
                    except Exception as e:
                        logger.info('error  workbook' + workbook.name + 'massage: ' + str(e))
                        #print(workbook.name, e)


def lambda_handler(event, context):
    refresh_tableau_workbooks_datasources_by_tag()
    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "message": "hello world",
            }
        ),
    }