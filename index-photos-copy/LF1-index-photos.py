import json
import boto3
import requests
from requests_aws4auth import AWS4Auth


def elastic_search(label):
    region = 'us-east-1'
    service = 'es'
    credentials = boto3.Session().get_credentials()
    awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)
    host = 'https://search-photos-xxn7dfaw2j6jylhohlzkotcxji.us-east-1.es.amazonaws.com'
    index = 'photos'
    url = host + '/' + index + '/_search'
    query = {
      'query': {
        'multi_match': {
          'query': label,
          'fields': ['labels', 'x-amz-meta-customLabels']
        }
      }
    }
    headers = { "Content-Type": "application/json" }
    
    try:
        r = requests.get(url, auth=awsauth, headers=headers, data=json.dumps(query))
        es_result = json.loads(r.text)
        print(es_result)
        es_res = es_result["hits"]["hits"]
        result = []
        for k in range(len(es_res)):
            result.append(es_res[k]['_source'])
        return result
    except Exception as axc:
        print(exc)
        return {
            'statusCode': 403,
            'headers': {
                'Access-Control-Allow-Origin': '*'
            },
            "isBase64Encoded": False,
            'body': json.dumps('No result from Elastic Search')
        }


def lambda_handler(event, context):
    print(event)
    query = event['queryStringParameters']['q']
    print(query)
    client = boto3.client('lex-runtime', region_name='us-east-1')
    response = client.post_text(
        botName = 'PhotoBot',
        botAlias = 'photos',
        userId = 'aadcsdeg',
        inputText = query)
    print(response)
    # response = {'slots': {'Query': 'cat'}}

    labels = []
    if 'slots' not in response:
        print("Cannot find photos")
    else:
        for key, value in response['slots'].items():
            if value:
                labels.append(value)
        print ("labels: ", labels)
    
    search_res = []
    for item in labels:
        esres = elastic_search(item)
        search_res.append(esres)
    print(search_res)
    
    # esres = [{
    #     'objectKey': 'testimg.jpg',
    #     'bucket': 'b2photos',
    #     'labels': ['dogs']
    # }]
    findphoto = []
    bucketurl = 'https://s3.amazonaws.com/b2photosbucket/'
    for typ in search_res:
        for item in typ:
            if item['objectKey'] not in findphoto:
                findphoto.append(bucketurl + item['objectKey'])
        print(findphoto)
    
    # return {
    #     'statusCode': 200,
    #     'body': json.dumps(photonaddr)
    # }

    return {
            'statusCode': 200,
            'headers': {
                'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'GET,OPTIONS'
            },
            'body': json.dumps(findphoto)
    }
