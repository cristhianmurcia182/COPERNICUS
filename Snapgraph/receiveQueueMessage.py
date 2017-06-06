#  http://boto3.readthedocs.io/en/latest/guide/sqs-example-sending-receiving-msgs.html
import os
import json
import boto3


def connect(configuration):
    os.system("aws configure set AWS_ACCESS_KEY_ID " + configuration["AWS_ACCESS_KEY_ID"])
    os.system("aws configure set AWS_SECRET_ACCESS_KEY " + configuration["AWS_SECRET_ACCESS_KEY"])
    os.system("aws configure set default.region " + configuration["default.region"])


def getConfigurationFile(jsonPath):
    with open(jsonPath, 'r') as outfile:
        conf = json.load(outfile)
    return conf


def getNotificationIDAndResourceName():
    conf = getConfigurationFile("D:\Jeison\Github\COPERNICUS\Snapgraph\configuration.json")

    # connect(conf)

    # messageJson = os.system("aws sqs receive-message --queue-url https://sqs.eu-central-1.amazonaws.com/837005286527/preprocess_image_queue")

    sqs = boto3.client('sqs')

    queue_url = 'https://sqs.eu-central-1.amazonaws.com/837005286527/preprocess_image_queue'

    # Receive message from SQS queue
    response = sqs.receive_message(
        QueueUrl=queue_url,
        AttributeNames=[
            'SentTimestamp'
        ],
        MaxNumberOfMessages=1,
        MessageAttributeNames=[
            'All'
        ],
        VisibilityTimeout=0,
        WaitTimeSeconds=0
    )

    message = response['Messages'][0]
    receipt_handle = message['ReceiptHandle']
    records = json.loads(message["Body"])["Records"]

    record = records[0]
    print receipt_handle, record["s3"]["object"]["key"]
