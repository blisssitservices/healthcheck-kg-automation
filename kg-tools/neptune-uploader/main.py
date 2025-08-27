import json
import boto3
import requests
from botocore.awsrequest import AWSRequest
from botocore.auth import SigV4Auth
from botocore.endpoint import Endpoint
from botocore.httpsession import URLLib3Session

# todo: load from env vars
aws_region = 'us-west-1'
account_id = '542568752808'
neptune_endpoint = 'neptunedbcluster-owrhkx3r5wka.cluster-c5cwkiqiw1c5.us-west-1.neptune.amazonaws.com'
neptune_port = '8182'
iam_role_arn = 'arn:aws:iam::' + account_id + ':role/APP'
##########################

def get_session():
    session = boto3.Session(
        region_name=aws_region
    )
    return session


def check_status(load_id):
    session = get_session()
    neptune = session.client('neptunedata', region_name=aws_region)
    status_response = neptune.get_loader_job_status(loadId=load_id)
    print(json.dumps(status_response, indent=2))


def run_import():
    print('Import from S3 to nepture starts')

    try:
        loader_url = f"https://{neptune_endpoint}:{neptune_port}/loader"

        s3_data_url = 's3://hc-hkg-data/'

        session = get_session()
        neptune = session.client('neptunedata', region_name=aws_region)

        params = {
            'source': s3_data_url,
            'format': 'turtle',  # , 'json' etc.
            'iamRoleArn': iam_role_arn,
            'failOnError': True,
            'region': aws_region,
            'parallelism': 'HIGH',  # or 'LOW'
            'updateSingleCardinalityProperties': False,
            'queueRequest': True
        }

        request = AWSRequest(
            method='POST',
            url=loader_url,
            data=json.dumps(params),
            headers={'Content-Type': 'application/json'}
        )

        credentials = session.get_credentials().get_frozen_credentials()
        SigV4Auth(credentials, 'neptune-db', aws_region).add_auth(request)

        session = URLLib3Session()
        response = session.send(request.prepare())

        print(response.status_code)
        print(response.content.decode())

        print('Import complete')

    except Exception as e:
        error_message = str(e)
        print(f"{error_message}")


def run_sparql_query():
    print('run_sparql_query')
    endpoint = f"{neptune_endpoint}:{neptune_port}/sparql"

    # example query
    query = """
    SELECT ?s ?p ?o
    WHERE {
      ?s ?p ?o
    }
    LIMIT 10
    """

    response = requests.post(
        endpoint,
        data={'query': query},
        headers={'Accept': 'application/sparql-results+json'}
    )

    print(response.json())


# if this code runs inside lambda:

# def lambda_handler(event, context):
#
#     run_import()
#
#     return {
#         'statusCode': 200,
#         'body': json.dumps('OK')
#     }
