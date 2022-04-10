import requests
import json
import boto3
def get_secret_value(name, version=None):
    secrets_client = boto3.client("secretsmanager")
    kwargs = {'SecretId': name}
    if version is not None:
        kwargs['VersionStage'] = version
    response = secrets_client.get_secret_value(**kwargs)
    return response
def lambda_handler(event, context):
    url = "https://eu-west-1.console.rivery.io/api/rivers"
    rivery = get_secret_value('rivery')
    token = json.loads(rivery['SecretString'])['token']
    payload={}
    headers = {
     'Authorization': token
    }
    response = requests.request("GET", url, headers=headers, data=payload)
    return {'rivery_list':dict([(d['river_definitions']['river_name'],d['_id']) for d in json.loads(response.text)])}