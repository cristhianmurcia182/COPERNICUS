import argparse
import sys
import time
from datetime import datetime
import urllib

import boto3
from botocore.compat import total_seconds

from Snapgraph.AWSFunctions import getDefaultConfigurationFile

configuration = getDefaultConfigurationFile()
BUCKET_NAME_RAW_IMAGES = configuration["BUCKET_NAME_RAW_IMAGES"]
PREPROCESS_STATUS_METADATA_KEY = "x-amz-meta-preprocessing-status"
PREPROCESS_STATUS_PROCESSED = "PROCESSED"
PREPROCESS_STATUS_ERROR = "ERROR"


def createJobArgument(filename):
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    filename_without_extention = filename.replace(".SAFE", "")
    jobName = filename_without_extention + "_job"

    jobQueue = configuration["job_queue_name"]
    jobDefinition = configuration["job_definition_name"]
    command = "/home/ubuntu/anaconda2/bin/python /home/ubuntu/COPERNICUS/Snapgraph/preprocessOneByOne.py -i %s" % filename

    parser.add_argument("--name", help="name of the job", type=str, default=jobName)
    parser.add_argument("--job-queue", help="name of the job queue to submit this job", type=str, default=jobQueue)
    parser.add_argument("--job-definition", help="name of the job job definition", type=str, default=jobDefinition)
    parser.add_argument("--command", help="command to run", type=str,
                        default=command)
    parser.add_argument("--wait", help="block wait until the job completes", action='store_true')

    args = parser.parse_args()
    return args


def cancelJob(batchClient, jobId):
    response = batchClient.cancel_job(
        jobId=jobId,
        reason="Cancelling job, preprocess complete"
    )
    return response


def endJobError(batchClient, jobId):
    response = batchClient.terminate_job(
        jobId=jobId,
        reason="Terminating job given a preprocess error"
    )
    return response


def submitJob(args, filename):
    jobName = args.name
    jobQueue = args.job_queue
    jobDefinition = args.job_definition
    command = args.command.split()
    wait = False

    batch = boto3.client(
        service_name='batch',
        region_name='eu-central-1',
        endpoint_url='https://batch.eu-central-1.amazonaws.com')

    submitJobResponse = batch.submit_job(
        jobName=jobName,
        jobQueue=jobQueue,
        jobDefinition=jobDefinition,
        containerOverrides={'command': command}
    )

    spin = ['-', '/', '|', '\\', '-', '/', '|', '\\']
    spinner = 0

    jobId = submitJobResponse['jobId']
    print 'Submitted job [%s - %s] to the job queue [%s]' % (jobName, jobId, jobQueue)
    while wait:
        time.sleep(1)
        describeJobsResponse = batch.describe_jobs(jobs=[jobId])
        status = describeJobsResponse['jobs'][0]['status']
        if status == 'SUCCEEDED' or status == 'FAILED':
            print '%s' % ('=' * 80)
            print 'Job [%s - %s] %s' % (jobName, jobId, status)
            break
        elif status == 'RUNNING':
            print '\rJob [%s - %s] is RUNNING.' % (jobName, jobId)
        else:
            print '\rJob [%s - %s] is %-9s... %s' % (jobName, jobId, status, spin[spinner % len(spin)]),
            sys.stdout.flush()
            spinner += 1


def getFileMetadata(bucketName, filename, key):
    s3 = boto3.resource("s3")

    file = s3.Object(bucketName, filename)

    if key is not file.metadata:
        return None

    return file.metadata[key]


def lamda_function(event, context):
    try:
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = urllib.unquote_plus(event['Records'][0]['s3']['object']['key'].encode('utf8'))

        args = createJobArgument(key)
        submitJob(args, key)
    except Exception as e:
        print(e)
        print(
            'Error starting batch job to preprocess the image: {} '.format(
                key))
        raise e
