import json
import os
import thread
import time
import subprocess

processXMLPath = "D:\ifgi\Copernicus\SnapAutomation\\batch\\"
inputPath = "D:\ifgi\Copernicus\SnapAutomation\Input\\"
outputPath = "D:\ifgi\Copernicus\SnapAutomation\Onput\\"


def readFiles():
    listdir = os.listdir(inputPath)
    for filename in listdir:
        startTime = time.time()
        thread.start_new_thread(processImage, (filename,))
        pid = getPID(filename)
        metadata = json.dumps({
            "name": filename,
            "pid": pid,
            "status: ": "STATUS",
            "starttime": time.time()
        })

        print metadata


def processImage(filename):
    print "Star time: %s" % time.ctime(time.time())
    command = "gpt %sPreProcess.xml -Pfilename=\"%s%s\" -Poutputfilename=\"%s%s.dim\"" % (
        processXMLPath, inputPath, filename, outputPath, filename)
    print command
    os.system(command)
    print "End time: %s" % time.ctime(time.time())


def getPID(filename):
    delay = 4
    time.sleep(delay)
    print "getPID"
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


readFiles()
