import json
import os
import thread
import time
import subprocess
import catch_errors as ce

#processXMLPath = "D:\ifgi\Copernicus\SnapAutomation\\batch\\"
#inputPath = "D:\ifgi\Copernicus\SnapAutomation\Input\\"
#outputPath = "D:\ifgi\Copernicus\SnapAutomation\Output\\"
#processStatusPath = "D:\ifgi\Copernicus\SnapAutomation\ProcessStatus\\"

processXMLPath = "D:\COPERNICUS\\12_Preprocessing\\"
inputPath = "D:\COPERNICUS\\12_Preprocessing\input\\"
outputPath = "D:\COPERNICUS\\12_Preprocessing\output\\"
processStatusPath = "D:\COPERNICUS\\12_Preprocessing\processStatus\\"


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


def enum(**enums):
    return type('Enum', (), enums)


ProcessStatus = enum(PROCESSING='PROCESSING', PROCESSED='PROCESSED', ERROR='ERROR')


def readFiles():
    listdir = os.listdir(inputPath)
    for filename in listdir:
        if ce.checkMissingFiles(inputPath, filename):
            startTime = time.time()
            thread.start_new_thread(processImage, (filename,))
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

def processImage(filename):
    print "Star time: %s" % time.ctime(time.time())
    command = "gpt %sPreProcess.xml -Pfilename=\"%s%s\" -Poutputfilename=\"%s%s.dim\"" % (
        processXMLPath, inputPath, filename, outputPath, filename)
    print command

    try:
        os.system(command)
    except:
        print "Error: unable to start thread"
        # here is necessary to add the error management procedure

    endTime = time.time()

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

    print "End time: %s" % time.ctime(time.time())


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

    return None


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
        print "JSON Status"
        data = json.load(outfile)
    return data



try:
    thread.start_new_thread(readFiles, ())
except:
    print "Error: unable to start thread"


def isProcessing():
    time.sleep(10)
    processStatusJSon = processStatusPath + "processing.json"
    data = readProcessStatusInJson(processStatusJSon)
    if data:
        return True

    return False


while isProcessing():
    pass
