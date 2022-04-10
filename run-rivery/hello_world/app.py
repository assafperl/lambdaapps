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

def run_Misc(event):

    rivery = get_secret_value('rivery')
    token = json.loads(rivery['SecretString'])['token']

    url = "https://eu-west-1.console.rivery.io/api/rivers"
    payload = {}
    headers = {
        'Authorization': token
    }
    response = requests.request("GET", url, headers=headers, data=payload)
    rivers =  dict([(d['river_definitions']['river_name'], d['_id']) for d in json.loads(response.text)])


    url = "https://eu-west-1.console.rivery.io/api/run"

    payload = json.dumps({
     "river_id": rivers[event['name']]
    })
    headers = {
     'Content-Type': 'application/json',
     'Authorization': token
    }

    response1 = requests.request("POST", url, headers=headers, data=payload)
    run_id = json.loads(response1.text)['run_id']
    return run_id


def lambda_handler(event, context):
    return  {
                "run_id": run_Misc(event),
            }




