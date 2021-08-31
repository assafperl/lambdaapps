import pandas as pd
import snowflake.connector
import base64
import pickle
import boto3
import json
import s3fs
import logging
import os
import io
import datetime
from datetime import datetime
from email.mime.text import MIMEText
from googleapiclient.discovery import build
from google.oauth2 import service_account
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
import mimetypes

logger = logging.getLogger()
logger.setLevel(logging.INFO)
os.environ['BUCKET_NAME'] = 'integrationbobbi'
dateTimeObj = datetime.now()


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
        # database=os.environ['PROD_DB'],
        # schema=os.environ['SCHEMA'],
        warehouse=json.loads(snowflakedict['SecretString'])['warehouse'],
        role=json.loads(snowflakedict['SecretString'])['role'],
        numpy=True)
    return conn


def get_df_snowflake_f(conn, snf_query):
    return pd.read_sql_query(snf_query, conn)


def get_google_sheet_contacts(maxline):
    s3client = boto3.client('s3')
    path_to_json = "google-creds/keys.json"
    s3_clientobj = s3client.get_object(Bucket=os.environ['BUCKET_NAME'], Key=path_to_json)
    s3_clientdata = s3_clientobj['Body'].read().decode('utf-8')
    s3clientjson = json.loads(s3_clientdata)
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
    credentials = service_account.Credentials.from_service_account_info(s3clientjson, scopes=SCOPES)
    SPREADSHEET_ID = '1-DryCL1y5Z2Gy5paLbPXOlDqnsKYwniaQDi9CPksebM'
    service = build('sheets', 'v4', credentials=credentials)
    sheet = service.spreadsheets()
    results = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range="soft-launch!A1:D" + str(maxline)).execute()
    column_names = results['values'].pop(0)
    googlesheetdf = pd.DataFrame(results['values'], columns=column_names)
    return googlesheetdf


def push_to_s3(df, table, short):
    s3 = boto3.resource('s3')
    destination = "data_validation/" + table + "_" + short + "_" + str(
        datetime.now().strftime('%Y_%m_%d_%H_%M_%S')) + '.csv'
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    logger.info(destination + ' file loaded into the bucket ')
    s3.Bucket(os.environ['BUCKET_NAME']).put_object(Key=destination, Body=csv_buffer.getvalue())
    return destination


def get_google_sheet(maxline):
    s3client = boto3.client('s3')
    path_to_json = "google-creds/keys.json"
    s3_clientobj = s3client.get_object(Bucket=os.environ['BUCKET_NAME'], Key=path_to_json)
    s3_clientdata = s3_clientobj['Body'].read().decode('utf-8')
    s3clientjson = json.loads(s3_clientdata)
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
    credentials = service_account.Credentials.from_service_account_info(s3clientjson, scopes=SCOPES)
    SPREADSHEET_ID = '1-DryCL1y5Z2Gy5paLbPXOlDqnsKYwniaQDi9CPksebM'
    service = build('sheets', 'v4', credentials=credentials)
    sheet = service.spreadsheets()
    results = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range="Sheet1!A1:G" + str(maxline)).execute()
    column_names = results['values'].pop(0)
    googlesheetdf = pd.DataFrame(results['values'], columns=column_names)
    googlesheetdf['script'] = googlesheetdf['script'].str.replace('\n', ' ')
    googlesheetdf['script'] = googlesheetdf['script'].str.replace('\r', '')
    return googlesheetdf


def get_gmail_service():
    path_to_pickle = "google-creds/token.pickle"
    s3client = boto3.client('s3')
    response = s3client.get_object(Bucket='integrationbobbi', Key=path_to_pickle)
    body = response['Body'].read()
    creds = pickle.loads(body)
    service = build('gmail', 'v1', credentials=creds)
    return service


def loop_the_sheet():
    counter = 1
    service = get_gmail_service()
    sender = "assaf.perl@hibob.io"
    user_id = "me"
    conn = get_snowflake_con()
    df = get_google_sheet(40)
    df_contacts = get_google_sheet_contacts(30)
    df_contacts['Emails'] = df_contacts[['Group Name', 'Email']].groupby(['Group Name'])['Email'].transform(
        lambda x: ','.join(x))
    df_contacts = df_contacts[['Group Name', 'Owner email', 'Name', 'Emails']].drop_duplicates()
    df_contacts = df_contacts.reset_index(drop=True)
    df_contacts = df_contacts.rename(columns={"Group Name": "Contacts"})
    df = df[df['Contacts'] != 'assaf.perl@hibob.io']
    if dateTimeObj.hour > 12:
        df = df[df['frequency'] == '2']
    googlesheetdf = pd.merge(df, df_contacts, how='left', on=['Contacts'])
    for email_list in googlesheetdf['Owner email'].unique():
        group = googlesheetdf[googlesheetdf['Owner email'] == email_list]
        group = group.reset_index(drop=True)
        message = MIMEMultipart()

        message_text = 'Greetings, ' + str(group['Name'].unique()[0])
        message['to'] = email_list
        message['cc'] = str(group['Emails'].unique()[0])
        message['from'] = sender
        message['subject'] = "Data Validations - ***soft launch*** " + str(dateTimeObj)
        counter = 1
        for index, row in group.iterrows():
            attdf = get_df_snowflake_f(conn, group['script'].iloc[index])
            if attdf.shape[0] != 0 and attdf.values[0][0] != 'Success':
                destination = push_to_s3(attdf, row['table'], row['short'])
                logger.info(destination)
                message_text = message_text + '\n' + str(counter) + '. ' + str(attdf.shape[0]) + ' ' + row[
                    'description'] + '\n' + 'Attached ' + destination + '\n'
                counter = counter + 1
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
        message_data = send_message(service, user_id, raw_text)


def send_message(service, user_id, message):
    message = (service.users().messages().send(userId=user_id, body=message)
               .execute())
    print('Message Id: %s' % message['id'])
    return message


def lambda_handler(event, context):
    loop_the_sheet()

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "message": "hello world",
            }
        ),
    }
