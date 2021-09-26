import boto3
import os
import json

os.environ['BUCKET_NAME'] = 'integrationbobbi'


def lambda_handler(event, context):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(os.environ['BUCKET_NAME'])
    bucket.objects.filter(Prefix="backup-snapshot/firstdayofmonth").delete()
    bucket = s3.Bucket(os.environ['BUCKET_NAME'])
    for file in bucket.objects.filter(Prefix="backup-snapshot/current/").all():
        copy_source = {
            'Bucket': os.environ['BUCKET_NAME'],
            'Key': file.key
        }
        bucket.copy(copy_source, 'backup-snapshot/firstdayofmonth/' + file.key[24:])

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "message": "hello world load",
            }
        ),
    }