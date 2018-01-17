# To change this license header, choose License Headers in Project Properties.
# To change this template file, choose Tools | Templates
# and open the template in the editor.
from RestoreAttachmentUtility import RestoreAttachmentUtility
import beatbox
import json

with open('settings.json') as data_file:
    projectSettings = json.load(data_file)
service = beatbox.PythonClient() # instantiate the object
service.serverUrl = str(projectSettings["SF_SERVER_URL"]) # login using your sf credentials
service.login(str(projectSettings["SF_ORG_USER"]), str(projectSettings["SF_ORG_PASSWORD"]))

startPoint = RestoreAttachmentUtility()
appFiles = startPoint.getAppFiles(service)
if len(appFiles) > 0:
    startPoint.processAppFiles(appFiles, service, projectSettings)