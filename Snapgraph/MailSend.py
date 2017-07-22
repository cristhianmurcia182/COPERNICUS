import smtplib
import time
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from AWSFunctions import connect, uploadFile, \
    getDefaultConfigurationFile, sendNotificationMessage, updateFileMetadata, getFileMetadata, getNextFilename, \
    downloadFile, isPreprocessPendingFile
configuration = getDefaultConfigurationFile()

SERVER_ACCESS = configuration["SERVER_ACCESS"]
SERVER_ACCESS_PORT = configuration["SERVER_ACCESS_PORT"]
PROCESS_STATUS = configuration["processStatusPath"]
BODY = open("/home/ubuntu/COPERNICUS/Snapgraph/TComplated.txt", 'r').read()


    
    
def send_email(user, pwd, recipient, subject, ProcessState, imageName, ImageDate, AttachedFile):
    try:
        ##TO CHANGE THE MASSAGE TO SEND
        WordSelection = {'@ProcessState@': ProcessState, '@imageName@': imageName, '@imageDate@': ImageDate, '@DateofProcess@': (time.strftime("%d/%m/%Y")), \
                         '@HourProcess@': (time.strftime("%H:%M:%S"))}
        msg = MIMEMultipart('alternative')
        mail_user = user
        mail_pwd = pwd
        FROM = user
        TO = recipient if type(recipient) is list else [recipient]
        msg['Subject'] = subject
        filename = AttachedFile
        ######HAS TO BE CHANGED
        f = file(PROCESS_STATUS + AttachedFile)
        #########
        attachment = MIMEText(f.read())
        attachment.add_header('Content-Disposition', 'attachment', filename=filename)
        msg.attach(attachment)

        # Prepare actual message
        textfile = BODY
        HTML = Metadata(textfile, WordSelection.copy())
        HTML_BODY = MIMEText(HTML, 'html')
        msg.attach(HTML_BODY)
        TEXT = msg
        server = smtplib.SMTP_SSL(SERVER_ACCESS, SERVER_ACCESS_PORT)
        server.ehlo()
        server.login(mail_user, mail_pwd)
        server.sendmail(FROM, TO, msg.as_string())
        server.quit()
    except:
        print "Error sending email"
    
## create the html according to the real information
def Metadata(html, MetadaInformation):
    if len(MetadaInformation) == 0:
        return html
    else:
        for i in MetadaInformation:
            x = (html.replace(i, MetadaInformation[i]))
            MetadaInformation.pop(i)
            return Metadata(x, MetadaInformation)
        





