import json
import boto3
import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import base64
import pickle
from googleapiclient.discovery import build

dateTimeObj = datetime.datetime.now()


def get_gmail_service():
    path_to_pickle = "google-creds/token.pickle"
    s3client = boto3.client('s3')
    response = s3client.get_object(Bucket='integrationbibob', Key=path_to_pickle)
    body = response['Body'].read()
    creds = pickle.loads(body)
    service = build('gmail', 'v1', credentials=creds)
    return service


def send_message(service, user_id, message):
    message = (service.users().messages().send(userId=user_id, body=message)
               .execute())
    print('Message Id: %s' % message['id'])
    return message


def alert_pipeline_not_updated():
    client = boto3.client('logs')
    logGroupNamePrefix = '/aws/lambda/load-sf-account-LoadSFAccountsFunction-N0frG3OOpnQh'
    log = client.describe_log_streams(logGroupName=logGroupNamePrefix, orderBy='LastEventTime', descending=True,
                                      limit=1)
    timestamp = str([l['lastEventTimestamp'] for l in log['logStreams']][0])
    your_dt = datetime.datetime.fromtimestamp(int(timestamp) / 1000)
    if your_dt.strftime("%Y-%m-%d") != datetime.datetime.today().strftime('%Y-%m-%d'):
        service = get_gmail_service()
        sender = "assaf.perl@hibob.io"
        user_id = "me"
        message = MIMEMultipart()
        message_text = 'Greetings, BI Team!\n'
        message['to'] = 'ohad.hallak@hibob.io'
        message['cc'] = 'eran.cohen@hibob.io,assaf.perl@hibob.io,Omer.biber@hibob.io,Omer.Lewy@hibob.io,natalie.oron@hibob.io,david.sussely@hibob.io,atzmon.avidar@hibob.io'
        message['from'] = sender
        message['subject'] = "Pipelines are not up to date" + str(dateTimeObj)
        message_text = message_text + '\n' + '\n' + 'Thank you,' + '\n' + ' Assaf P'
        msg = MIMEText(message_text)
        message.attach(msg)
        raw_text = {'raw': base64.urlsafe_b64encode(message.as_bytes()).decode()}
        message_data = send_message(service, user_id, raw_text)


def lambda_handler(event, context):
    alert_pipeline_not_updated()
    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "message": "hello world",
            }
        ),
    }