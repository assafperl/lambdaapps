import boto3
import requests
import json

def get_secret_value(name, version=None):
    secrets_client = boto3.client("secretsmanager")
    kwargs = {'SecretId': name}
    if version is not None:
        kwargs['VersionStage'] = version
    response = secrets_client.get_secret_value(**kwargs)
    return response

def check_run(event):
    rivery = get_secret_value('rivery')
    token = json.loads(rivery['SecretString'])['token']

    url = "https://eu-west-1.console.rivery.io/api/check_run?run_id="+event["Payload"]["run_id"]

    payload = {}
    headers = {
     'Authorization': token
    }
    response2 = requests.request("GET", url, headers=headers, data=payload)
    return json.loads(response2.text)["river_run_status"]


def lambda_handler(event, context):
    return {
        "status": check_run(event),
        "run_id": event["Payload"]["run_id"],
    }






