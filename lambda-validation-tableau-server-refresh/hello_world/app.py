import pandas as pd
import base64
import pickle
import boto3
import json
import s3fs
import logging
import os
import io
import datetime
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from googleapiclient.discovery import build
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
import tableauserverclient as TSC
import mimetypes
import urllib3
urllib3.disable_warnings()

logger = logging.getLogger()
logger.setLevel(logging.INFO)
dateTimeObj = datetime.now()
yesterday = datetime.now() - timedelta(1)

def get_secret_value(name, version=None):
    secrets_client = boto3.client("secretsmanager")
    kwargs = {'SecretId': name}
    if version is not None:
        kwargs['VersionStage'] = version
    response = secrets_client.get_secret_value(**kwargs)
    return response

def get_df_snowflake_f(conn, snf_query):
    return pd.read_sql_query(snf_query, conn)


def push_to_s3(df, table, short):
    s3 = boto3.resource('s3')
    destination = "data_validation/" + table + "_" + short + "_" + str(
        datetime.now().strftime('%Y_%m_%d_%H_%M_%S')) + '.csv'
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    logger.info(destination + ' file loaded into the bucket ')
    s3.Bucket(os.environ['BUCKET_NAME']).put_object(Key=destination, Body=csv_buffer.getvalue())
    return destination


def get_gmail_service():
    path_to_pickle = "google-creds/token.pickle"
    s3client = boto3.client('s3')
    response = s3client.get_object(Bucket=os.environ['BUCKET_NAME'], Key=path_to_pickle)
    body = response['Body'].read()
    creds = pickle.loads(body)
    service = build('gmail', 'v1', credentials=creds)
    return service


def send_message(service, user_id, message):
    message = (service.users().messages().send(userId=user_id, body=message)
               .execute())
    print('Message Id: %s' % message['id'])
    return message


def get_all_user():
    request_options = TSC.RequestOptions(pagesize=1000)
    tableaudict = get_secret_value('tableau')
    name = json.loads(tableaudict['SecretString'])['srv_token_name']
    token = json.loads(tableaudict['SecretString'])['srv_token']
    url = json.loads(tableaudict['SecretString'])['srv_url']
    server = TSC.Server(url)
    server.add_http_options({'verify': False})
    server.use_server_version()
    tableau_auth = TSC.PersonalAccessTokenAuth(name, token, '')
    req_option = TSC.RequestOptions()
    with server.auth.sign_in(tableau_auth):
        all_users = list(TSC.Pager(server.users, request_options))
        return {k:v for element in [{user.id: user.name} for user in all_users] for k,v in element.items()}


def get_user_email(id_, userinfo):
    for user in userinfo:
        if set(user.keys()).pop() == id_:
            return (set(user.values()).pop())


def get_unrefreshed_prod_tableau_sources():
    user_list = get_all_user()
    column_names = ['resource_name', 'resource_type', 'project_name', 'owner', 'webpage_url', 'last_updated']
    df = pd.DataFrame(columns=column_names)
    tableaudict = get_secret_value('tableau')
    name = json.loads(tableaudict['SecretString'])['srv_token_name']
    token = json.loads(tableaudict['SecretString'])['srv_token']
    url = json.loads(tableaudict['SecretString'])['srv_url']
    tag = 'prod'
    tableau_auth = TSC.PersonalAccessTokenAuth(name, token, '')
    server = TSC.Server(url)
    server.add_http_options({'verify': False})
    server.use_server_version()
    req_option = TSC.RequestOptions()
    req_option.filter.add(TSC.Filter(TSC.RequestOptions.Field.Tags,
                                     TSC.RequestOptions.Operator.Equals,
                                     tag))
    with server.auth.sign_in(tableau_auth):
        all_workbooks, pagination_item = server.workbooks.get(req_option)
        for workbook in all_workbooks:
            if workbook.tags:
                if 'prod' in [tag.lower() for tag in workbook.tags]:
                    resource = server.workbooks.get_by_id(workbook.id)
                    if yesterday > datetime.strptime(str(resource.updated_at)[:-7], '%Y-%m-%d %H:%M:%S'):
                        df_slut = pd.DataFrame([[resource.name, 'workbook', resource.project_name,
                                                 user_list[workbook.owner_id], resource.webpage_url,
                                                 str(datetime.strptime(str(resource.updated_at)[:-7],
                                                                       '%Y-%m-%d %H:%M:%S'))]], columns=column_names)
                        df = pd.concat([df_slut, df], ignore_index=True)
        all_datasources, pagination_item = server.datasources.get(req_option)
        for datasource in all_datasources:
            if datasource.tags:
                if 'prod' in [tag.lower() for tag in datasource.tags]:
                    resource = server.datasources.get_by_id(datasource.id)
                    if yesterday > datetime.strptime(str(resource.updated_at)[:-7], '%Y-%m-%d %H:%M:%S'):
                        df_slut = pd.DataFrame([[resource.name, 'datasource', resource.project_name,
                                                 user_list[datasource.owner_id], resource.webpage_url,
                                                 str(datetime.strptime(str(resource.updated_at)[:-7],
                                                                       '%Y-%m-%d %H:%M:%S'))]], columns=column_names)
                        df = pd.concat([df_slut, df], ignore_index=True)
    return (df)


def run_tableau_refresh_validation():
    service = get_gmail_service()
    sender = "assaf.perl@hibob.io"
    user_id = "me"
    message = MIMEMultipart()
    message_text = 'Greetings, BI Team'
    message['to'] = 'atzmon.avidar@hibob.io'
    message['cc'] = 'eran.cohen@hibob.io,assaf.perl@hibob.io,Omer.biber@hibob.io,Omer.Lewy@hibob.io,atzmon.avidar@hibob.io,natalie.oron@hibob.io,shiran.deutch@hibob.io,naor.daga@hibob.io,tslil.gil@hibob.io,harel.yarrow@hibob.io,anna.polyakov@hibob.io,assaf.haim@hibob.io'
    message['from'] = sender
    message['subject'] = "Tableau Prod Server Refresh Validations " + str(dateTimeObj)
    attdf = get_unrefreshed_prod_tableau_sources()
    destination = push_to_s3(attdf, 'tablue', 'refreshed-validation')
    logger.info(destination)
    message_text = message_text + '\n' + str(
        attdf.shape[0]) + ' resources are not up to date.' + '\n' + 'Attached ' + destination + '\n'
    file = os.environ['BUCKET_NAME'] + '/' + destination
    content_type, encoding = mimetypes.guess_type(file)
    if content_type is None or encoding is not None:
        content_type = 'application/octet-stream'
    main_type, sub_type = content_type.split('/', 1)
    fs = s3fs.S3FileSystem()
    fp = fs.open(file, 'rb')
    msg = MIMEBase(main_type, sub_type)
    msg.set_payload(fp.read())
    fp.close()
    filename = os.path.basename(file)
    msg.add_header('Content-Disposition', 'attachment', filename=filename)
    message.attach(msg)
    message_text = message_text + 'Thank you,' + '\n' + ' Assaf P'
    msg = MIMEText(message_text)
    message.attach(msg)
    raw_text = {'raw': base64.urlsafe_b64encode(message.as_bytes()).decode()}
    send_message(service, user_id, raw_text)


def lambda_handler(event, context):
    run_tableau_refresh_validation()

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "message": "hello world",
            }
        ),
    }
