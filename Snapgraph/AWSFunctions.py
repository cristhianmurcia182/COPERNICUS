#  http://boto3.readthedocs.io/en/latest/guide/sqs-example-sending-receiving-msgs.html
import json
import os
import subprocess
import sys
import traceback
import urllib2

import boto3
import time

from enumerations import ProcessStatus


def connect():
    configuration = getDefaultConfigurationFile()

    os.system("/home/ubuntu/anaconda2/bin/aws configure set AWS_ACCESS_KEY_ID " + configuration["AWS_ACCESS_KEY_ID"])
    os.system(
        "/home/ubuntu/anaconda2/bin/aws configure set AWS_SECRET_ACCESS_KEY " + configuration["AWS_SECRET_ACCESS_KEY"])
    os.system("/home/ubuntu/anaconda2/bin/aws configure set default.region " + configuration["default.region"])


def getDefaultConfigurationFile():
    return getConfigurationFile("/home/ubuntu/COPERNICUS/Snapgraph/configuration.json")
    # return getConfigurationFile("D:\Jeison\Github\COPERNICUS\Snapgraph\configuration.json")


def getConfigurationFile(jsonPath):
    with open(jsonPath, 'r') as outfile:
        conf = json.load(outfile)
    return conf


def getNotificationIDAndResourceName():
    sqs = boto3.client('sqs')

    configuration = getDefaultConfigurationFile()

    queue_url = configuration["queue_url"]
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
    filename = record["s3"]["object"]["key"]

    attempts = 0

    if "attempts" in record["s3"]["object"]:
        attempts = int(record["s3"]["object"]["attempts"])
    return receipt_handle, filename, attempts


def getNextFilename(bucketName):
    configuration = getDefaultConfigurationFile()
    META_DATA_STATUS_KEY = configuration["META_DATA_STATUS_KEY"]
    META_DATA_ATTEMPTS_KEY = configuration["META_DATA_ATTEMPTS_KEY"]
    MAX_PREPROCESSING_ATTEMPTS = int(configuration["MAX_PREPROCESSING_ATTEMPTS"])

    s3 = boto3.resource("s3")

    bucket = s3.Bucket(bucketName)

    for key in bucket.objects.all():
        # print(key.key)
        file = s3.Object(bucketName, key.key)
        dateOfFile = extractDateFromFileName(file.key)
        year = dateOfFile[0:4]
        month = dateOfFile[4:6]
        if year == "2017" and month == "06":
            if META_DATA_STATUS_KEY not in file.metadata:
                return file.key
            elif file.metadata[META_DATA_STATUS_KEY] == ProcessStatus.ERROR:
                if META_DATA_ATTEMPTS_KEY in file.metadata:
                    attempts = int(file.metadata[META_DATA_ATTEMPTS_KEY])
                    if attempts < MAX_PREPROCESSING_ATTEMPTS:
                        return file.key
    return None


def extractDateFromFileName(key):
    return key[17:25]


def isPreprocessPendingFile(bucketName):
    configuration = getDefaultConfigurationFile()
    META_DATA_STATUS_KEY = configuration["META_DATA_STATUS_KEY"]
    META_DATA_ATTEMPTS_KEY = configuration["META_DATA_ATTEMPTS_KEY"]
    MAX_PREPROCESSING_ATTEMPTS = int(configuration["MAX_PREPROCESSING_ATTEMPTS"])

    s3 = boto3.resource("s3")

    bucket = s3.Bucket(bucketName)

    for key in bucket.objects.all():
        # print(key.key)
        file = s3.Object(bucketName, key.key)
        dateOfFile = extractDateFromFileName(file.key)
        year = dateOfFile[0:4]
        month = dateOfFile[4:6]
        if year == "2017" and month == "06":
            if META_DATA_STATUS_KEY not in file.metadata:
                return True
            elif file.metadata[META_DATA_STATUS_KEY] == ProcessStatus.ERROR:
                if META_DATA_ATTEMPTS_KEY in file.metadata:
                    attempts = int(file.metadata[META_DATA_ATTEMPTS_KEY])
                    if attempts < MAX_PREPROCESSING_ATTEMPTS:
                        return True

    return False


def deleteMessage(receipt_handle):
    sqs = boto3.client('sqs')
    configuration = getDefaultConfigurationFile()

    queue_url = configuration["queue_url"]

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

    # download_command = "aws s3 cp s3://%s/%s %s" % (bucket_path, filename, writeble_path + filename)
    download_command = ["/home/ubuntu/anaconda2/bin/aws", "s3", "cp", "s3://%s/%s" % (bucket_path, filename),
                        writeble_path + filename]
    print download_command

    try:
        # os.system(download_command)
        p = subprocess.Popen(download_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (output, err) = p.communicate()

        # This makes the wait possible
        p_status = p.wait()
        print "Command output: " + output
    except Exception as err:
        print "Error: unable to download"
        traceback.print_exc(file=sys.stdout)
        raise err


def uploadFolder(writeble_path, folder_name, bucket_path):
    # s3 = boto3.resource('s3')
    # bucket = s3.Bucket(bucket_name)
    #
    # with open(writeble_path + filename, "wb") as f:
    #     bucket.download_fileobj(filename, f)
    # s3_client = boto3.client('s3')
    # s3_client.download_file(bucket_name, filename, writeble_path + filename)
    print "uploading folder"
    # upload_command = "aws s3 cp %s%s s3://%s/%s/ --recursive" % (writeble_path, folder_name, bucket_path, folder_name)
    upload_command = ["/home/ubuntu/anaconda2/bin/aws", "s3", "cp", writeble_path + folder_name,
                      "s3://%s/%s/" % (bucket_path, folder_name),
                      "--recursive"]
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
        traceback.print_exc(file=sys.stdout)


def uploadFile(file_path, bucket_path):
    # s3 = boto3.resource('s3')
    # bucket = s3.Bucket(bucket_name)
    #
    # with open(writeble_path + filename, "wb") as f:
    #     bucket.download_fileobj(filename, f)
    # s3_client = boto3.client('s3')
    # s3_client.download_file(bucket_name, filename, writeble_path + filename)
    print "uploading file"
    # upload_command = "aws s3 cp %s s3://%s/" % (file_path, bucket_path)
    upload_command = ["/home/ubuntu/anaconda2/bin/aws", "s3", "cp", file_path, "s3://%s/" % bucket_path]
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
        traceback.print_exc(file=sys.stdout)


def sendNotificationMessage(filename, attempts=0):
    sqs = boto3.client('sqs')
    configuration = getDefaultConfigurationFile()

    queue_url = configuration["queue_url"]

    messageBody = {"Records": [{"eventVersion": "2.0", "eventSource": "aws:s3", "awsRegion": "eu-central-1",
                                "eventTime": "%s" % time.ctime(time.time()),
                                "eventName": "ObjectCreated:CompleteMultipartUpload",
                                "userIdentity": {"principalId": "AWS:AIDAIBWB3PWZTGHKL35A6"},
                                # "requestParameters": {"sourceIPAddress": "54.239.6.71"},
                                # "responseElements": {"x-amz-request-id": "5BDEDA8E5D4F1E01",
                                #                     "x-amz-id-2": "UEJY/YjeF7OGDp1HMvYvceYE2UQu0WkY8EAHxGVSCNQnJsSg79yeqHwMkn5a6Ze1dxNJFisYqgg="},
                                "s3": {"s3SchemaVersion": "1.0", "configurationId": "AddRawImageEvent",
                                       "bucket": {"name": "s1-datastore",
                                                  "ownerIdentity": {"principalId": "A26CMLUIHJTH3P"},
                                                  "arn": "arn:aws:s3:::s1-datastore"},
                                       "object": {
                                           "key": filename,
                                           "attempts": "%s" % attempts
                                           # ,"eTag": "4e3f9b6694bb7dbef1a18f7e85e19e90-61",
                                           # "sequencer": "00595517ED16853079"
                                       }}}]}

    response = sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(messageBody),
        DelaySeconds=0,
        MessageAttributes={
            # 'string': {
            #     'StringValue': 'string',
            #     'BinaryValue': b'bytes',
            #     'StringListValues': [
            #         'string',
            #     ],
            #     'BinaryListValues': [
            #         b'bytes',
            #     ],
            #     'DataType': 'string'
            # }
        }
        # MessageDeduplicationId='string',
        # MessageGroupId='string'
    )


def getFileMetadata(bucketName, filename, key):
    s3 = boto3.resource("s3")

    file = s3.Object(bucketName, filename)

    if key is not file.metadata:
        return None

    return file.metadata[key]


def updateFileMetadata(bucketName, filename, metadata={}):
    s3 = boto3.resource("s3")

    file = s3.Object(bucketName, filename)

    file.metadata.update(metadata)
    file.copy_from(CopySource={"Bucket": bucketName, "Key": filename}, Metadata=file.metadata,
                   MetadataDirective="REPLACE")


def stopInstance():
    ec2 = boto3.resource('ec2')
    instanceId = urllib2.urlopen('http://169.254.169.254/latest/meta-data/instance-id').read()
    ids = [instanceId]
    ec2.instances.filter(InstanceIds=ids).stop()


def terminateInstance():
    ec2 = boto3.resource('ec2')
    instanceId = urllib2.urlopen('http://169.254.169.254/latest/meta-data/instance-id').read()
    ids = [instanceId]
    ec2.instances.filter(InstanceIds=ids).terminate()
