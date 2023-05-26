from botocore.vendored import requests
import time
import json
import uuid
import boto3
from datetime import datetime
from botocore.exceptions import ClientError


carnet_table = "testTable"
rekognition_table = "testR"
s3_client = boto3.client("s3")
dynamodb_client = boto3.client('dynamodb')
rekognition_client = boto3.client('rekognition')

def process_carnet(bucket, object_key, url):
    response = requests.post('https://carnet.ai/recognize-url', data=url)
    status_code = response.status_code
    print("Response:", response)
    if status_code == 200:
        data = response.json()
        print("Data:", data)
        save_carnet_info_to_dynamodb(data)
    elif status_code == 429:
        print("Bad API response: 429. Retrying after half a second...")
        time.sleep(0.5)
    elif status_code == 500:
        err = "Image doesn't contain a car"
        if response.json().get('error') == err:
            aws_rekognition_result = get_image_labels(bucket, object_key)
            print("Result from AWS Recognition:", aws_rekognition_result)
            save_aws_rekognition_info_to_dynamodb(aws_rekognition_result)
        else:
            print("Bad API response:", status_code)
    else:
        print("Bad API response:", status_code)


def save_aws_rekognition_info_to_dynamodb(data):
    timestamp = datetime.utcnow().replace(microsecond=0).isoformat()
    json_data = json.dumps(data)

    data_to_save = {
        'id': {'S': str(uuid.uuid1())},
        'createdAt': {'S': timestamp},
        'updatedAt': {'S': timestamp},
        'rekognition': {'S': json_data}
    }

    try:
        dynamodb_client.put_item(TableName=rekognition_table, Item=data_to_save)
    except ClientError as e:
        print("Error:", e.response['Error']['Message'])
        raise e


def save_carnet_info_to_dynamodb(data):
    timestamp = datetime.utcnow().replace(microsecond=0).isoformat()

    data_to_save = {
        'id': {'S': str(uuid.uuid1())},
        'createdAt': {'S': timestamp},
        'updatedAt': {'S': timestamp},
        'carnet': {'S': json.dumps(data)}
    }

    try:
        dynamodb_client.put_item(TableName=carnet_table, Item=data_to_save)
    except ClientError as e:
        print("Error:", e.response['Error']['Message'])
        raise e


def get_image_labels(bucket, key):
    response = rekognition_client.detect_labels(
        Image={'S3Object': {'Bucket': bucket, 'Name': key}},
        MaxLabels=10
    )

    return response


def lambda_handler(event, _):
    for record in event.get("Records"):
        bucket = record.get("s3").get("bucket").get("name")
        key = record.get("s3").get("object").get("key")
        location = s3_client.get_bucket_location(Bucket=bucket)['LocationConstraint']
        url = f'https://{bucket}.s3.{location}.amazonaws.com/{key}'

        process_carnet(bucket, key, url)

    return {"statusCode": 200, "body": "Done!"}
