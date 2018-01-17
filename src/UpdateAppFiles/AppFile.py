import beatbox
import json
import traceback

class AppFile:
    
    def updateApplicationFiles(self):
        with open('../settings.json') as data_file:    
            projectSettings = json.load(data_file)
        service = beatbox.PythonClient() # instantiate the object
        service.serverUrl = str(projectSettings["SF_SERVER_URL"]) # login using your sf credentials
        service.login(str(projectSettings["SF_ORG_USER"]), str(projectSettings["SF_ORG_PASSWORD"])) # save the sub applications to be updated
        
        # Map appFileId--> Attachment Id
        attsByParentIds = {}
        # App files with inserted attachments
        appFilesToUpdate = []
        
        # Read attachments.txt in order to get the parent object associated to the attachments.
        with open("../atts.txt", "r") as f:
            for line in f:
                # You may also want to remove whitespace characters like `\n` at the end of each line.
                aux = line.strip().split(",")
                attsByParentIds[aux[0]] = aux[1]
                appFilesToUpdate.append({"Id": aux[0], "S3_File_Name__c": None, "S3_Uploaded__c": False, "type": "Application_File__c"})
        
        corruptAtts = []
        with open("../attsCorrupt.txt", "r") as f:
            for line in f:
                # You may also want to remove whitespace characters like `\n` at the end of each line.
                corruptAtts.append(line.strip())
        
        # Remove app files with corrupt attachments 'cause we do not want to update them.
        for appFile in appFilesToUpdate:
            for corruptF in corruptAtts:
                if appFile["Id"] == corruptF:
                    appFilesToUpdate.remove(appFile)

        # Update app files.
        if appFilesToUpdate > 0:
            self.dmlOperations(appFilesToUpdate, attsByParentIds, service)

    def dmlOperations(self, records, attsByParentIds, service):
        # success app files
        failAppFiles = set()
        logFile = open("appFiles.txt", "w")
        logFile2 = open("appFileErrors.txt", "w")
        recordsSize = len(records)
        attsToDelete = []
        
        if recordsSize > 0:
            # commit changes to SF
            counter = 0
            helpList = []
            # we can't update more than 200 at once
            while counter < recordsSize:
                helpList.append(records[counter])
                if len(helpList) == 200 or counter == recordsSize -1:
                    try:
                        aux = service.update(helpList)
                        for index, val in enumerate(aux):
                            if val["success"]:
                                logFile.write(val["id"] + "\n")
                            else:
                                logFile2.write(helpList[index]["Id"] + "-->" + str(val["errors"]) + "\n")
                                failAppFiles.add(helpList[index]["Id"])
                    except:
                        traceback.print_exc()
                        
                    helpList = []
                counter+=1
            
            for myId in failAppFiles:
                attsToDelete.append(attsByParentIds[myId])

            logFile.close()
            logFile2.close()
            
            # Delete attachments of failed app files.
            counter = 0
            helpList = []
            recordsSize = len(attsToDelete)
            # we can't update more than 200 at once
            while counter < recordsSize:
                helpList.append(attsToDelete[counter])
                if len(helpList) == 200 or counter == recordsSize -1:
                    try:
                        service.delete(helpList)
                    except:
                        traceback.print_exc()
                    helpList = []
                counter+=1
                