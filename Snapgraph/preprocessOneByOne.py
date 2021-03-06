import getopt
import json
import os
import subprocess
import sys
import thread
import threading
import time
import traceback
import zipfile
import shutil

import catch_errors as ce
from AWSFunctions import connect, uploadFile, \
    getDefaultConfigurationFile, sendNotificationMessage, updateFileMetadata, getFileMetadata, getNextFilename, \
    downloadFile, isPreprocessPendingFile, terminateInstance, stopInstance
from enumerations import ProcessStatus
from preprocessExceptions import OrbitNotIncludedException, VVBandNotIncludedException, VHBandNotIncludedException, \
    PreprocessedCommandException, ZipException, FilenameNotFoundException
from MailSend import *
import logging

# logging.debug('This message should go to the log file')
# logging.info('So should this')
# logging.warning('And this, too')
# logging.error('And this, too')

configuration = getDefaultConfigurationFile()

processXMLPath = configuration["processXMLPath"]
inputPath = configuration["inputPath"]
outputPath = configuration["outputPath"]
processStatusPath = configuration["processStatusPath"]
safeFileExtension = configuration["safeFileExtension"]
zipFileExtension = configuration["zipFileExtension"]
outputFileExtension = configuration["outputFileExtension"]
exportFileExtension = configuration["exportFileExtension"]
BUCKET_NAME_RAW_IMAGES = configuration["BUCKET_NAME_RAW_IMAGES"]
BUCKET_NAME_PROCESSED_IMAGES = configuration["BUCKET_NAME_PROCESSED_IMAGES"]
BUCKET_FOLDER_NAME_PREPROCESSED_IMAGES = configuration["BUCKET_FOLDER_NAME_PREPROCESSED_IMAGES"]
MAX_PREPROCESSING_ATTEMPTS = int(configuration["MAX_PREPROCESSING_ATTEMPTS"])
META_DATA_STATUS_KEY = configuration["META_DATA_STATUS_KEY"]
META_DATA_ATTEMPTS_KEY = configuration["META_DATA_ATTEMPTS_KEY"]
MAIL_ACCOUNT_SENDER = configuration["MAIL_ACCOUNT_SENDER"]
MAIL_ACCOUNT_PASSWORD = configuration["MAIL_ACCOUNT_PASSWORD"]
MAIL_ACCOUNT_RECIVER = configuration["MAIL_ACCOUNT_RECIVER"]
SNAP_TEMP_PATH = configuration["SNAP_TEMP_PATH"]
LOG_PATH = configuration["LOG_PATH"]
ORBIT_PATH = configuration["ORBIT_PATH"]
META_DATA_ORBIT_USED_KEY = configuration["META_DATA_ORBIT_USED_KEY"]

logging.basicConfig(filename=LOG_PATH, level=logging.DEBUG)

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


# writeProcessStatusFiles()


def deleteFile(path, filename):
    print "Deleting FILE %s" % filename
    logging.info("Deleting FILE %s" % filename)
    path_filename = path + filename
    if os.path.exists(path_filename):
        os.remove(path_filename)
    print "The FILE %s was deleted" % path_filename
    logging.info("The FILE %s was deleted" % path_filename)


def deleteFolder(path, foldername):
    print "Deleting FOLDER %s" % foldername
    logging.info("Deleting FOLDER %s" % foldername)
    path_foldername = path + foldername
    if os.path.exists(path_foldername):
        shutil.rmtree(path_foldername)
    print "The FOLDER %s was deleted" % path_foldername
    logging.info("The FOLDER %s was deleted" % path_foldername)


def createFolder(path):
    if not os.path.exists(path):
        os.makedirs(path)
        print "The FOLDER %s was created" % path
        logging.info("The FOLDER %s was created" % path)


def readFiles(input_filename):
    attempts = 0
    try:
        if input_filename is None:
            return

        deleteFolder(inputPath, "")
        deleteFolder(outputPath, "")
        deleteFolder(SNAP_TEMP_PATH, "")
        createFolder(inputPath)
        createFolder(outputPath)
        createFolder(SNAP_TEMP_PATH + "gpt")

        filename = input_filename

        META_DATA_STATUS = getFileMetadata(BUCKET_NAME_RAW_IMAGES, input_filename, "x-amz-meta-preprocessing-status")
        META_DATA_ATTEMPTS = getFileMetadata(BUCKET_NAME_RAW_IMAGES, input_filename, "x-amz-meta-preprocessing-attempts")
        attemptsString = getFileMetadata(BUCKET_NAME_RAW_IMAGES, input_filename, META_DATA_ATTEMPTS_KEY)
        if attemptsString is None:
            attempts = 0
        else:
            attempts = int(attemptsString)

        continuePreprocessing = False
        if META_DATA_STATUS is None:
            continuePreprocessing = True
        elif META_DATA_STATUS == "ERROR":
            if META_DATA_ATTEMPTS is not None:
                attempts = int(META_DATA_ATTEMPTS)

                if attempts < MAX_PREPROCESSING_ATTEMPTS:
                    continuePreprocessing = True

        if not continuePreprocessing:
            print "Image %s already preprocessed with status %s" % (input_filename, META_DATA_STATUS)
            logging.info("Image %s already preprocessed with status %s" % (input_filename, META_DATA_STATUS))
            return

        attempts = attempts + 1

        # deleteMessage(receipt_handle)

        print "downloading raw image %s" % input_filename
        logging.info("downloading raw image %s" % input_filename)

        updateFileMetadata(BUCKET_NAME_RAW_IMAGES, input_filename,
                           {
                               META_DATA_STATUS_KEY: ProcessStatus.PROCESSING,
                               META_DATA_ATTEMPTS_KEY: "%s" % attempts
                           })

        downloadFile(inputPath, input_filename, BUCKET_NAME_RAW_IMAGES)

        if input_filename.endswith(".SAFE"):
            # filename = zipImageFile(inputPath, input_filename.replace(".SAFE", ""), ".SAFE")
            filename = input_filename.replace(".SAFE", ".zip")
            os.rename(inputPath + input_filename, inputPath + filename)
            # deleteFile(inputPath, input_filename)

        print "starting preprocessing"
        logging.info("starting preprocessing")

        if ce.checkMissingFiles(inputPath, filename):
            startTime = time.time()

            processStatusJSon = processStatusPath + "processing.json"
            data = readProcessStatusInJson(processStatusJSon)
            jsonData = {
                "status: ": ProcessStatus.PROCESSING,
                "starttime": startTime
            }
            addProcessStatusDataToJson(filename, data, jsonData)
            writeProcessStatusInJson(processStatusJSon, data)

            outputFilename = preprocessImage(input_filename, filename)

            delay = 60
            time.sleep(delay)
    except OrbitNotIncludedException as err:
        error_message = "Orbit error: {0}".format(err)
        print(error_message)
        logging.error(error_message)
        sendNotification(err, error_message, input_filename, attempts)
        updateFileMetadata(BUCKET_NAME_RAW_IMAGES, input_filename,
                           {META_DATA_STATUS_KEY: ProcessStatus.ORBIT_MISSING_ERROR})
        send_email(MAIL_ACCOUNT_SENDER, MAIL_ACCOUNT_PASSWORD, MAIL_ACCOUNT_RECIVER, "Copernicus Processing - ERROR",
                   filename,
                   err.message,
                   filename[17:21] + "/" + filename[21:23] + "/" + filename[23:25], "errors.json")
        raise err
    except VVBandNotIncludedException as err:
        error_message = "VV Band error: {0}".format(err)
        print(error_message)
        logging.error(error_message)
        sendNotification(err, error_message, input_filename, attempts)
        updateFileMetadata(BUCKET_NAME_RAW_IMAGES, input_filename,
                           {META_DATA_STATUS_KEY: ProcessStatus.ERROR})
        send_email(MAIL_ACCOUNT_SENDER, MAIL_ACCOUNT_PASSWORD, MAIL_ACCOUNT_RECIVER, "Copernicus Processing - ERROR",
                   filename,
                   err.message,
                   filename[17:21] + "/" + filename[21:23] + "/" + filename[23:25], "errors.json")
        raise err
    except VHBandNotIncludedException as err:
        error_message = "VH error: {0}".format(err)
        print(error_message)
        logging.error(error_message)
        sendNotification(err, error_message, input_filename, attempts)
        updateFileMetadata(BUCKET_NAME_RAW_IMAGES, input_filename,
                           {META_DATA_STATUS_KEY: ProcessStatus.ERROR})
        send_email(MAIL_ACCOUNT_SENDER, MAIL_ACCOUNT_PASSWORD, MAIL_ACCOUNT_RECIVER, "Copernicus Processing - ERROR",
                   filename,
                   err.message,
                   filename[17:21] + "/" + filename[21:23] + "/" + filename[23:25], "errors.json")
        raise err
    except PreprocessedCommandException as err:
        error_message = "Preprocessing command error: {0}".format(err)
        print(error_message)
        logging.error(error_message)
        sendNotification(err, error_message, input_filename, attempts)
        updateFileMetadata(BUCKET_NAME_RAW_IMAGES, input_filename,
                           {META_DATA_STATUS_KEY: ProcessStatus.ERROR})
        send_email(MAIL_ACCOUNT_SENDER, MAIL_ACCOUNT_PASSWORD, MAIL_ACCOUNT_RECIVER, "Copernicus Processing - ERROR",
                   filename,
                   err.message,
                   filename[17:21] + "/" + filename[21:23] + "/" + filename[23:25], "errors.json")
        raise err
    except Exception as err:
        error_message = "Error: unable to preprocess"
        print error_message
        logging.error(error_message)
        traceback.print_exc(file=sys.stdout)
        sendNotification(Exception(), error_message, input_filename, attempts)
        updateFileMetadata(BUCKET_NAME_RAW_IMAGES, input_filename,
                           {META_DATA_STATUS_KEY: ProcessStatus.ERROR})
        send_email(MAIL_ACCOUNT_SENDER, MAIL_ACCOUNT_PASSWORD, MAIL_ACCOUNT_RECIVER, "Copernicus Processing - ERROR",
                   filename,
                   error_message,
                   filename[17:21] + "/" + filename[21:23] + "/" + filename[23:25], "errors.json")
        raise err
    finally:
        print "Finally"
        # try:
        #     deleteFile(outputPath, input_filename)
        # except:
        #     print "The FILE %s couldn't be deleted" % input_filename
        #
        # try:
        #     deleteFile(outputPath, filename)
        # except:
        #     print "The FILE %s couldn't be deleted" % filename
        #
        # try:
        #     deleteFile(outputPath, outputFilename)
        # except:
        #     print "The FILE %s couldn't be deleted" % outputFilename
        #
        # try:
        #     deleteFolder(outputPath, outputFilename.replace(outputFileExtension, ""))
        # except:
        #     print "The FOLDER %s couldn't be deleted" % outputFilename.replace(outputFileExtension, "")


def preprocessImage(input_filename, filename):
    print "Star time: %s" % time.ctime(time.time())
    logging.info("Star time: %s" % time.ctime(time.time()))
    noneExtensionFilename = filename.replace(zipFileExtension, "")
    print "output:" + noneExtensionFilename + outputFileExtension
    logging.info("output:" + noneExtensionFilename + outputFileExtension)
    command = "sh /home/ubuntu/snap/bin/gpt %sPreProcess.xml -Pfilename=\"%s%s\" -Poutputfilename=\"%s%s%s\"" % (
        # linux
        # command = "gpt %sPreProcess.xml -Pfilename=\"%s%s\" -Poutputfilename=\"%s%s%s\"" % (  # windows
        processXMLPath, inputPath, filename, outputPath + noneExtensionFilename + "/", noneExtensionFilename,
        outputFileExtension)
    print command
    logging.info(command)

    ##MAIL STARTING PROCESSING

    send_email(MAIL_ACCOUNT_SENDER, MAIL_ACCOUNT_PASSWORD, MAIL_ACCOUNT_RECIVER, "Copernicus Processing - Starting",
               filename,
               "\"STARTING PROCESSING\"",
               filename[17:21] + "/" + filename[21:23] + "/" + filename[23:25], "processing.json")

    try:
        os.system(command)
    except:
        print "Error: unable to start preprocessing command, preprocessImage()"
        logging.error("Error: unable to start preprocessing command, preprocessImage()")
        traceback.print_exc(file=sys.stdout)
        raise PreprocessedCommandException(
            "Error trying to run the preprocessing GPT command for the file %s" % filename)
        # here is necessary to add the error management procedure

    endTime = time.time()
    print "End time: %s" % time.ctime(time.time())
    logging.info("End time: %s" % time.ctime(time.time()))

    ##MAIL COMPLETED

    send_email(MAIL_ACCOUNT_SENDER, MAIL_ACCOUNT_PASSWORD, MAIL_ACCOUNT_RECIVER, "Copernicus Processing - Ending",
               filename,
               "\"COMPLETED\"",
               filename[17:21] + "/" + filename[21:23] + "/" + filename[23:25], "processed.json")

    # start to upload the preprocessed resulting image
    outputFileName = filename.replace(zipFileExtension, "")

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
        "status: ": ProcessStatus.PROCESSED,
        "starttime": processingStatus["starttime"],
        "endtime": endTime
    }
    addProcessStatusDataToJson(filename, data, jsonData)
    writeProcessStatusInJson(processStatusJSon, data)

    orbitUsed = ce.imageOrbit(inputPath + input_filename, ORBIT_PATH)

    updateFileMetadata(BUCKET_NAME_RAW_IMAGES, input_filename, {META_DATA_STATUS_KEY: ProcessStatus.PROCESSED, META_DATA_ORBIT_USED_KEY: orbitUsed})

    return noneExtensionFilename + outputFileExtension


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

    try:
        for root, dirs, files in os.walk(path + folder_name):
            for file in files:
                absolute_path = os.path.join(root, file)
                relative_path = absolute_path.replace(path, "")
                zipf.write(os.path.join(root, file), relative_path)

    except:
        error_message = "ERROR compressing directory: %" % (path + folder_name)
        print error_message
        logging.error(error_message)
        traceback.print_exc(file=sys.stdout)
        raise ZipException(error_message)
    finally:
        zipf.close()

    return zip_filename


def zipImageFile(path, file_name, extension):
    zip_filename = file_name + ".zip"
    zipf = zipfile.ZipFile(path + zip_filename, "w", zipfile.ZIP_DEFLATED, allowZip64=True)
    complete_file_name = file_name + extension

    try:
        for root, dirs, files in os.walk(path):
            for file in files:
                if (root == path) & (complete_file_name == file):
                    absolute_path = os.path.join(root, file)
                    relative_path = absolute_path.replace(path, "")
                    zipf.write(os.path.join(root, file), relative_path)



    except:
        error_message = "ERROR compressing image file: %" % complete_file_name
        print error_message
        logging.error(error_message)
        traceback.print_exc(file=sys.stdout)
        raise ZipException(error_message)
    finally:
        zipf.close()

    return zip_filename


def sendNotification(error, error_message, filename, attempts):
    try:
        sendNotificationMessage(filename, attempts)
    except:
        print "Error: unable to send message"
        logging.error("Error: unable to send message")
        traceback.print_exc(file=sys.stdout)


# def isProcessing():
#     time.sleep(10)
#     processStatusJSon = processStatusPath + "processing.json"
#     data = readProcessStatusInJson(processStatusJSon)
#     if data:
#         return True
#
#     return False

def main():
    counter = 0
    try:
        # connect()

        filenames = [
            # "S1A_IW_GRDH_1SDV_20170103T053318_20170103T053343_014663_017DA3_3AAF.SAFE",
            # "S1A_IW_GRDH_1SDV_20170206T055029_20170206T055054_015159_018CD3_FF13.SAFE",
            # "S1A_IW_GRDH_1SDV_20170304T053317_20170304T053342_015538_01988A_86A6.SAFE",
            # "S1A_IW_GRDH_1SDV_20170228T171630_20170228T171655_015487_0196F9_00AF.SAFE"
        ]

        for filename in filenames:
            try:
                if counter > 200:
                    break

                counter = counter + 1

                readFiles(filename)

            except:
                print "Error: unable to start reading files 1"
                logging.error("Error: unable to start reading files 1")
                traceback.print_exc(file=sys.stdout)

        # while isPreprocessPendingFile(BUCKET_NAME_RAW_IMAGES):
        #     try:
        #         if counter > 200:
        #             break
        #
        #         counter = counter + 1
        #
        #         filename = getNextFilename(BUCKET_NAME_RAW_IMAGES)
        #         readFiles(filename)
        #
        #     except:
        #         print "Error: unable to start reading files"
        #         traceback.print_exc(file=sys.stdout)
        argv = sys.argv[1:]
        opts, args = getopt.getopt(argv, "hi:o:", ["ifile=", "ofile="])

        filename = None

        for opt, arg in opts:
            if opt == '-h':
                print 'preprocessOneByOne.py -i <inputfile>'
                raise FilenameNotFoundException("")
            elif opt in ("-i", "--ifile"):
                filename = arg
                break

        if filename is None:
            raise FilenameNotFoundException("")

        readFiles(filename)
    except Exception as e:
        error_message = "General error: {0}".format(e)
        print "Error: unable to connect %s" % error_message
        logging.error(error_message)
        traceback.print_exc(file=sys.stdout)
        raise e
    finally:
        # stopInstance()
        pass


# try:
#     # thread.start_new_thread(main, ())
#     main()
# except:
#     print "Error: unable to start thread"
#     traceback.print_exc(file=sys.stdout)


class preprocessingThread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        print "Starting preprocess"
        try:
            main()
            print "Exiting preprocess"
        except Exception as e:
            error_message = "Peprocessing Thread error: {0}".format(e)
            print "Error: unable to start thread"
            logging.error(error_message)
            traceback.print_exc(file=sys.stdout)
            raise e


# thread1 = preprocessingThread()
# thread1.start()

print "Starting preprocess"
main()
print "Exiting preprocess"

