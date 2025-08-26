import boto3
import json

aws_region = 'us-west-1'


def check_status(load_id):

    neptune = boto3.client('neptunedata', region_name=aws_region)
    status_response = neptune.get_loader_job_status(loadId=load_id)
    print(json.dumps(status_response, indent=2))

def run_import():

    print('Import from S3 to nepture starts')

    try:

        # reader endpoint
        neptune_endpoint = 'neptunedbcluster-owrhkx3r5wka.cluster-ro-c5cwkiqiw1c5.us-west-1.neptune.amazonaws.com'
        neptune_port = '8182'

        s3_data_url = 's3://hc-hkg-data/'
        iam_role_arn = 'arn:aws:iam::${AWS::AccountId}:role/APP'

        neptune = boto3.client('neptunedata', region_name=aws_region)

        params = {
            'source': s3_data_url,
            'format': 'csv',  # 'csv', 'nquads', 'rdfxml', 'turtle', 'json' etc.
            'iamRoleArn': iam_role_arn,
            'region': 'us-east-1',
            'failOnError': True,
            'parallelism': 'HIGH',  # or 'LOW'
            'updateSingleCardinalityProperties': False,
            'queueRequest': True
        }

        response = neptune.start_loader_job(**params)

        print("Loader job started:")
        print(json.dumps(response, indent=2))


        load_id = response['payload']['loadId']
        status_response = neptune.get_loader_job_status(loadId=load_id)
        print(json.dumps(status_response, indent=2))

        print('Import complete')

    except Exception as e:
        print(f"Error: str(e)")

# run import:
run_import()
