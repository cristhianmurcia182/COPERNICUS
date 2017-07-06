import json
import os
import subprocess
import sys
import thread
import time
import traceback
import zipfile

import catch_errors as ce
from AWSFunctions import connect, uploadFile, \
    getDefaultConfigurationFile, sendNotificationMessage, updateFileMetadata, getFileMetadata, getNextFilename, \
    downloadFile
from Snapgraph.enumerations import ProcessStatus
from Snapgraph.exceptions import OrbitNotIncludedException, VVBandNotIncludedException, VHBandNotIncludedException, \
    PreprocessedCommandException

configuration = getDefaultConfigurationFile()

processXMLPath = configuration["processXMLPath"]
inputPath = configuration["inputPath"]
outputPath = configuration["outputPath"]
processStatusPath = configuration["processStatusPath"]
inputFileExtension = configuration["inputFileExtension"]
outputFileExtension = configuration["outputFileExtension"]
exportFileExtension = configuration["exportFileExtension"]
BUCKET_NAME_RAW_IMAGES = configuration["BUCKET_NAME_RAW_IMAGES"]
BUCKET_NAME_PROCESSED_IMAGES = configuration["BUCKET_NAME_PROCESSED_IMAGES"]
BUCKET_FOLDER_NAME_PREPROCESSED_IMAGES = configuration["BUCKET_FOLDER_NAME_PREPROCESSED_IMAGES"]
MAX_PREPROCESSING_ATTEMPTS = int(configuration["MAX_PREPROCESSING_ATTEMPTS"])
META_DATA_STATUS_KEY = configuration["META_DATA_STATUS_KEY"]
META_DATA_ATTEMPTS_KEY = configuration["META_DATA_ATTEMPTS_KEY"]


def writeProcessStatusFiles():
    processStatusJSon = processStatusPath + "processing.json"
    with open(processStatusJSon, 'wb') as outfile:
        json.dump({}, outfile)

    processStatusJSon = processStatusPath + "processed.json"
    with open(processStatusJSon, 'wb') as outfile:
        json.dump({}, outfile)

    processStatusJSon = processStatusPath + "errors.json"
    with open(processStatusJSon, 'wb') as outfile:
        json.dump({}, outfile)


writeProcessStatusFiles()


def readFiles():
    try:
        connect()
        #receipt_handle, filename, attempts = getNotificationIDAndResourceName()

        filename = getNextFilename(BUCKET_NAME_RAW_IMAGES)

    except:
        error_message = "Error: unable to read next file"
        print error_message
        traceback.print_exc(file=sys.stdout)
        sendNotification(Exception(), error_message, None, 0)

    try:
        if filename is None:
            return

        # if receipt_handle is None:
        #     return
        # if attempts >= MAX_PREPROCESSING_ATTEMPTS:
        #     return

        # deleteMessage(receipt_handle)

        print "downloading raw image %s" % filename
        attemptsString = getFileMetadata(BUCKET_NAME_RAW_IMAGES, filename, META_DATA_ATTEMPTS_KEY)
        if attemptsString is None:
            attempts = 0
        else:
            attempts = int(attemptsString)

        updateFileMetadata(BUCKET_NAME_RAW_IMAGES, filename,
                           {
                               META_DATA_STATUS_KEY: ProcessStatus.PROCESSING,
                               META_DATA_ATTEMPTS_KEY: "%s" % (attempts + 1)
                           })
        downloadFile(inputPath, filename, BUCKET_NAME_RAW_IMAGES)

        print "starting preprocessing"

        # listdir = os.listdir(inputPath)
        # for filename in listdir:

        if ce.checkMissingFiles(inputPath, filename):
            startTime = time.time()
            thread.start_new_thread(preprocessImage, (filename))
            pid = getProcessID(filename)
            processStatusJSon = processStatusPath + "processing.json"
            data = readProcessStatusInJson(processStatusJSon)
            jsonData = {
                "pid": pid,
                "status: ": ProcessStatus.PROCESSING,
                "starttime": startTime
            }

            addProcessStatusDataToJson(filename, data, jsonData)
            writeProcessStatusInJson(processStatusJSon, data)
    except OrbitNotIncludedException as err:
        error_message = "Orbit error: {0}".format(err)
        print(error_message)
        sendNotification(err, error_message, filename, attempts)
        updateFileMetadata(BUCKET_NAME_RAW_IMAGES, filename,
                           {META_DATA_STATUS_KEY: ProcessStatus.ERROR})
    except VVBandNotIncludedException as err:
        error_message = "VV Band error: {0}".format(err)
        print(error_message)
        sendNotification(err, error_message, filename, attempts)
        updateFileMetadata(BUCKET_NAME_RAW_IMAGES, filename,
                           {META_DATA_STATUS_KEY: ProcessStatus.ERROR})
    except VHBandNotIncludedException as err:
        error_message = "VH error: {0}".format(err)
        print(error_message)
        sendNotification(err, error_message, filename, attempts)
        updateFileMetadata(BUCKET_NAME_RAW_IMAGES, filename,
                           {META_DATA_STATUS_KEY: ProcessStatus.ERROR})
    except PreprocessedCommandException as err:
        error_message = "Preprocessing command error: {0}".format(err)
        print(error_message)
        sendNotification(err, error_message, filename, attempts)
        updateFileMetadata(BUCKET_NAME_RAW_IMAGES, filename,
                           {META_DATA_STATUS_KEY: ProcessStatus.ERROR})
    except:
        error_message = "Error: unable to preprocess"
        print error_message
        traceback.print_exc(file=sys.stdout)
        sendNotification(Exception(), error_message, filename, attempts)
        updateFileMetadata(BUCKET_NAME_RAW_IMAGES, filename,
                           {META_DATA_STATUS_KEY: ProcessStatus.ERROR})


def preprocessImage(filename):
    print "Star time: %s" % time.ctime(time.time())
    noneExtensionFilename = filename.replace(inputFileExtension, "")
    print "output:" + noneExtensionFilename + outputFileExtension
    command = "gpt %sPreProcess.xml -Pfilename=\"%s%s\" -Poutputfilename=\"%s%s%s\"" % (
        processXMLPath, inputPath, filename, outputPath + noneExtensionFilename + "/", noneExtensionFilename,
        outputFileExtension)
    print command

    try:
        os.system(command)
    except:
        print "Error: unable to start preprocessing command, preprocessImage()"
        traceback.print_exc(file=sys.stdout)
        raise PreprocessedCommandException(
            "Error trying to run the preprocessing GPT command for the file %s" % filename)
        # here is necessary to add the error management procedure

    endTime = time.time()
    print "End time: %s" % time.ctime(time.time())

    # start to upload the preprocessed resulting image
    outputFileName = filename.replace(inputFileExtension, "")

    zip_filename = zipDirectory(outputPath, outputFileName)

    uploadFile(outputPath + zip_filename,
               BUCKET_NAME_PROCESSED_IMAGES + "/" + BUCKET_FOLDER_NAME_PREPROCESSED_IMAGES)
    # end of uploading

    processStatusJSon = processStatusPath + "processing.json"
    data = readProcessStatusInJson(processStatusJSon)
    processingStatus = data[filename]

    deleteElementJson(processStatusJSon, data, filename)
    writeProcessStatusInJson(processStatusJSon, data)

    processStatusJSon = processStatusPath + "processed.json"
    data = readProcessStatusInJson(processStatusJSon)

    jsonData = {
        "pid": processingStatus["pid"],
        "status: ": ProcessStatus.PROCESSED,
        "starttime": processingStatus["starttime"],
        "endtime": endTime
    }
    addProcessStatusDataToJson(filename, data, jsonData)
    writeProcessStatusInJson(processStatusJSon, data)

    updateFileMetadata(BUCKET_NAME_RAW_IMAGES, filename, {META_DATA_STATUS_KEY: ProcessStatus.PROCESSED})


def getProcessID(filename):
    delay = 2
    time.sleep(delay)

    wmic_cmd = """wmic process where "name='gpt.exe'" get commandline,processid"""  # or name='snap64.exe'
    wmic_prc = subprocess.Popen(wmic_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    wmic_out, wmic_err = wmic_prc.communicate()
    pythons = [item.rsplit(None, 1) for item in wmic_out.splitlines() if item][1:]
    pythons = [[cmdline, int(pid)] for [cmdline, pid] in pythons]
    print pythons
    for process in pythons:
        if filename in process[0]:
            return process[1]

    return ""


def addProcessStatusDataToJson(filename, data, jsonData):
    if filename not in data:
        statusData = {}

        for key in jsonData:
            statusData[key] = jsonData[key]

        data[filename] = statusData

        print data


def deleteElementJson(jsonPath, data, filename):
    if filename in data:
        del data[filename]


def writeProcessStatusInJson(jsonPath, data):
    with open(jsonPath, 'w') as outfile:
        json.dump(data, outfile, sort_keys=True)


def readProcessStatusInJson(jsonPath):
    with open(jsonPath, 'r') as outfile:
        data = json.load(outfile)
    return data


def zipDirectory(path, folder_name):
    zip_filename = folder_name + ".zip"
    zipf = zipfile.ZipFile(path + zip_filename, "w", zipfile.ZIP_DEFLATED, allowZip64=True)

    for root, dirs, files in os.walk(path):
        for file in files:
            if zip_filename != file:
                absolute_path = os.path.join(root, file)
                relative_path = absolute_path.replace(path, "")
                zipf.write(os.path.join(root, file), relative_path)

    zipf.close()

    return zip_filename


def sendNotification(error, error_message, filename, attempts):
    try:
        sendNotificationMessage(filename, attempts)
    except:
        print "Error: unable to send message"
        traceback.print_exc(file=sys.stdout)


try:
    readFiles()
except:
    print "Error: unable to start thread"
    traceback.print_exc(file=sys.stdout)
    sendNotification(Exception(), "Unknown error")


def isProcessing():
    time.sleep(10)
    processStatusJSon = processStatusPath + "processing.json"
    data = readProcessStatusInJson(processStatusJSon)
    if data:
        return True

    return False


while isProcessing():
    pass
