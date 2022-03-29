import json
import urllib.parse
import boto3
from requests_aws4auth import AWS4Auth
from opensearchpy import OpenSearch, RequestsHttpConnection
import time
from datetime import datetime
import uuid

print('Loading function')
session = boto3.Session()



def detect_labels(photo, bucket):
    client=session.client('rekognition')
    response = client.detect_labels(Image={'S3Object':{'Bucket':bucket,'Name':photo}}, MaxLabels=10)
    labels = []
    for label in response['Labels']:
        labels.append(label['Name'])
    return labels


def get_customlabels(bucket, key):
    s3 = session.client('s3')
    try:
        response = s3.head_object(Bucket=bucket, Key=key)
        return response['Metadata']['customlabels']
    except Exception as exc:
        print(exc)
        return None


def store_es(index_name, document, id):
    region = 'us-east-1'
    service = 'es'
    credentials = session.get_credentials()
    awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)
    endpoint = 'search-photos-xxn7dfaw2j6jylhohlzkotcxji.us-east-1.es.amazonaws.com'
    client = OpenSearch(
            hosts=[{'host': endpoint, 'port': 443}],
            use_ssl=True,
            verify_certs=True,
            http_auth=awsauth,
            connection_class=RequestsHttpConnection)
    response = client.index(
            index = index_name,
            body = document,
            id = id,
            refresh = True)
    print(response)


def lambda_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))
    s3 = session.client('s3')

    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    # key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    
    customlabels_list = get_customlabels(bucket, key)
    labels_list = detect_labels(key, bucket)
    # now = datetime.now()
    # dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
    json_object = {
        'objectKey': key,
        'bucket': bucket,
        'x-amz-meta-customLabels': customlabels_list,
        'labels': labels_list
    }
    print(json_object)
    console.log(json_object)
    store_es('photos', json_object, uuid.uuid4())
    return {
        'statusCode': 200,
        'body': json.dumps('Success!')
    }
    
