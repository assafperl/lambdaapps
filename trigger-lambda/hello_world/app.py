import json
import boto3
import os
import datetime
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def trigger_lambda(event):
    s3 = boto3.resource('s3')
    destination = event['resource'][1:] +'_'+ str(datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S')) + '.txt'
    s3.Bucket(os.environ['BUCKET_NAME']).put_object(Key=destination, Body=str(datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S')))

def lambda_handler(event, context):
    logger.info(event['resource'][1:])
    trigger_lambda(event)
    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "message": "hello world 1",
            }
        ),
    }
