#  http://boto3.readthedocs.io/en/latest/guide/sqs-example-sending-receiving-msgs.html
import json
import os
import subprocess

import boto3


def connect():
    configuration = getDefaultConfigurationFile()

    os.system("aws configure set AWS_ACCESS_KEY_ID " + configuration["AWS_ACCESS_KEY_ID"])
    os.system("aws configure set AWS_SECRET_ACCESS_KEY " + configuration["AWS_SECRET_ACCESS_KEY"])
    os.system("aws configure set default.region " + configuration["default.region"])


def getDefaultConfigurationFile():
    return getConfigurationFile("/home/ubuntu/COPERNICUS-master/Snapgraph/configuration.json")

def getConfigurationFile(jsonPath):
    with open(jsonPath, 'r') as outfile:
        conf = json.load(outfile)
    return conf


def getNotificationIDAndResourceName():
    sqs = boto3.client('sqs')

    queue_url = 'https://sqs.eu-central-1.amazonaws.com/837005286527/preprocess_image_queue'
    # water-detection-image-queue

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

    if 'Messages' not in response:
        return None, None

    message = response['Messages'][0]
    receipt_handle = message['ReceiptHandle']
    records = json.loads(message["Body"])["Records"]

    record = records[0]
    return receipt_handle, record["s3"]["object"]["key"]


def deleteMessage(receipt_handle):
    sqs = boto3.client('sqs')
    queue_url = 'https://sqs.eu-central-1.amazonaws.com/837005286527/preprocess_image_queue'

    sqs.delete_message(
        QueueUrl=queue_url,
        ReceiptHandle=receipt_handle
    )


def downloadFile(writeble_path, filename, bucket_path):
    # s3 = boto3.resource('s3')
    # bucket = s3.Bucket(bucket_name)
    #
    # with open(writeble_path + filename, "wb") as f:
    #     bucket.download_fileobj(filename, f)
    # s3_client = boto3.client('s3')
    # s3_client.download_file(bucket_name, filename, writeble_path + filename)

    download_command = "aws s3 cp s3://%s/%s %s" % (bucket_path, filename, writeble_path + filename)
    print download_command

    try:
        # os.system(download_command)
        p = subprocess.Popen(download_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (output, err) = p.communicate()

        # This makes the wait possible
        p_status = p.wait()
        print "Command output: " + output
    except:
        print "Error: unable to download"


def uploadFolder(writeble_path, folder_name, bucket_path):
    # s3 = boto3.resource('s3')
    # bucket = s3.Bucket(bucket_name)
    #
    # with open(writeble_path + filename, "wb") as f:
    #     bucket.download_fileobj(filename, f)
    # s3_client = boto3.client('s3')
    # s3_client.download_file(bucket_name, filename, writeble_path + filename)
    print "uploading folder"
    # upload_command = "aws s3 cp %s s3://%s/%s" % (bucket_path, writeble_path + filename, filename)
    upload_command = "aws s3 cp %s%s s3://%s/%s/ --recursive" % (writeble_path, folder_name, bucket_path, folder_name)
    print upload_command

    try:
        # os.system(upload_command)
        p = subprocess.Popen(upload_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (output, err) = p.communicate()

        # This makes the wait possible
        p_status = p.wait()
        print "Command output: " + output
    except:
        print "Error: unable to upload folder"


def uploadFile(file_path, bucket_path):
    # s3 = boto3.resource('s3')
    # bucket = s3.Bucket(bucket_name)
    #
    # with open(writeble_path + filename, "wb") as f:
    #     bucket.download_fileobj(filename, f)
    # s3_client = boto3.client('s3')
    # s3_client.download_file(bucket_name, filename, writeble_path + filename)
    print "uploading file"
    # upload_command = "aws s3 cp %s s3://%s/%s" % (bucket_path, writeble_path + filename, filename)
    upload_command = "aws s3 cp %s s3://%s/" % (file_path, bucket_path)
    print upload_command

    try:
        # os.system(upload_command)
        p = subprocess.Popen(upload_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (output, err) = p.communicate()

        # This makes the wait possible
        p_status = p.wait()
        print "Command output: " + output
    except:
        print "Error: unable to upload file"
