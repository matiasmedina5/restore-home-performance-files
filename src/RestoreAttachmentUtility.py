import traceback
import boto3
__author__ = "Matias Medina"
__date__ = "$11/07/2016 04:02:59 PM$"

class RestoreAttachmentUtility:
   
    def executeQuery(self, aparentIds, wichQuery, service):
        queryStr = ""
        if aparentIds:
            if wichQuery == 1:
                queryStr = """
                    SELECT Id, File_Name__c, S3_File_Name__c, S3_Uploaded__c, 
                        (SELECT parentId from Attachments) 
                    FROM Application_File__c 
                    WHERE Id IN (\'""" + "\',\'".join(aparentIds) + "\')"
            else:
                queryStr = """
                    SELECT BodyLength, ParentId 
                    FROM Attachment 
                    WHERE ParentId IN (""" + ",".join(aparentIds) + ")"


            query_result = service.query(queryStr)

            partialAppFiles = query_result['records']
            total_records = query_result['size']
            query_locator = query_result['queryLocator']

            # Loop through, pulling the next 500 and appending it to your records dict.
            while not query_result['done'] and len(partialAppFiles) < total_records:
                # Get the updated queryLocator.
                query_result = service.queryMore(query_locator)
                # Append to records dictionary.
                query_locator = query_result['queryLocator']
                partialAppFiles = partialAppFiles + query_result['records']
            
            return partialAppFiles

    def getAppFiles(self, service):
        """
        Return a list of application files that need their attachments back from S3.
        @type  service: object
        @param service: The connection to Salesforce.
        @rtype: list[]
        @return: List of app files: [{}, {}].
        """
        # The app files that need to get the attachment back from S3.
        appFilesToUpdate = []
        # App file ids of the needed app files
        appFileIds = []
        
        # Query the sub apps that meet the requirements (post: rebate issued, canceled, rejected)
        query_result = service.query("SELECT Home_Performance_XML__c, Home_Performance_System_File__c FROM Sub_Application__c WHERE (Home_Performance_XML__c != null OR Home_Performance_System_File__c != null) AND (Application__r.Status__c = \'Canceled\' OR Application__r.Status__c = \'Rebate Issued\' OR Application__r.Status__c = \'Rejected\') AND (RecordType.Name = \'POST-APP\' OR RecordType.Name = \'EnergyPro Post-Installation\')")
        
        #query_result = service.query(queryStr)
        subApps = query_result['records']
        total_records = query_result['size']
        query_locator = query_result['queryLocator']

        # Loop through, pulling the next 500 and appending it to your records dict.
        while not query_result['done'] and len(subApps) < total_records:
            # Get the updated queryLocator.
            query_result = service.queryMore(query_locator)
            # Append to records dictionary.
            query_locator = query_result['queryLocator']
            subApps = subApps + query_result['records']
        
        # Loop through the sub apps to get the only the needed app file ids.
        for subApp in subApps:
            if subApp.Home_Performance_XML__c != None and subApp.Home_Performance_XML__c != "":
                appFileIds.append(subApp.Home_Performance_XML__c)
            if subApp.Home_Performance_System_File__c != None and subApp.Home_Performance_System_File__c != "":
                appFileIds.append(subApp.Home_Performance_System_File__c)
        
        if appFileIds and len(appFileIds) > 0:
            # Remove the comma at the end of the string.
            # Query for those app files that have an attachment in S3.
            appFiles = []

            while len(appFileIds) > 0:
                partialParentIds = []
                index = 0

                while index < 900 and len(appFileIds) > 0 :
                    partialParentIds.append(appFileIds.pop(0))
                    index += 1

                appFiles = appFiles + self.executeQuery(partialParentIds, 1, service)

            print "appFiles:" + str(len(appFiles))

            logFile = open("appFilesWithAttachments.txt", "w")
            for appFile in appFiles:
                # Make sure that the app file doesn't have attachment 'cause it is in S3.
                if len(appFile.Attachments) == 0:
                    appFilesToUpdate.append(appFile)
                    logFile.write(appFile.Id + "\n")
                #else:
                    #logFile.write(appFile.Id + "\n")
            logFile.close()
        return appFilesToUpdate

    def processAppFiles(self, appFiles, service, projectSettings):
        """
        Take attachments from S3 and insert them into Salesforce.
        @type  appFiles: list[]
        @param appFiles: list  of app files.
        @type  service: object
        @param service: connection to Salesforce.
        @type  projectSettings: object
        @param projectSettings: json file that contains credentials.
        """
        # Store attachments to be inserted in Salesforce.
        atts = []
        # ParentId to Attachment size
        sizesByParentId = dict()
        
        # Connect to S3.
        client = boto3.client(
            's3',
            aws_access_key_id=str(projectSettings["AWS_ACCESS_KEY_ID"]),
            aws_secret_access_key=str(projectSettings["AWS_SECRET_ACCESS_KEY"]),
        )
        
        # Activate aws s3 accelerator for the needed bucket. This uses the accelerator endpoint.
        client.put_bucket_accelerate_configuration(
            Bucket=str(projectSettings["S3_BUCKET_NAME"]),
            AccelerateConfiguration={
                'Status':'Enabled'
            }
        )
        
        for appFile in appFiles:
            try:
                # Get the object file from the bucket.
                obj = client.get_object(Bucket=str(projectSettings["S3_BUCKET_NAME"]), Key="Attachments/" + appFile["S3_File_Name__c"])
                if obj['ContentLength'] > 0:
                    xmlStr = obj["Body"].read().encode("base64")
                    attachment = {"type": "Attachment", "ParentId": appFile["Id"], "Name": appFile["File_Name__c"], "Body": xmlStr}
                    atts.append(attachment)
                    sizesByParentId[appFile["Id"]] = obj['ContentLength']
            except:
                traceback.print_exc()

        # Insert attachments in Salesforce
        if len(appFiles) > 0:
            successParentIds = self.dmlOperations(atts, service)

            if successParentIds and len(successParentIds) > 0:
                successAtts = []
                while len(successParentIds) > 0:
                    partialParentIds = []
                    index = 0

                    while index < 900 and len(successParentIds) > 0 :
                        partialParentIds.append(successParentIds.pop(0))
                        index += 1

                    successAtts = successAtts + self.executeQuery(partialParentIds, 2, service)

                logFile = open("attsCorrupt.txt", "w")
                for att in successAtts:
                    if att.BodyLength != sizesByParentId[att.ParentId]:
                        logFile.write(att.ParentId + "\n")
                logFile.close()


    def dmlOperations(self, records, service):
        logFile = open("atts.txt", "w")
        logFile2 = open("attErrors.txt", "w")
        recordsSize = len(records)
        successParentIds = []
        """
        Insert attachments in Salesforce.
        @type  records: list[]
        @param records: list of attachments to be inserted.
        @type  service: object
        @param service: connection to Salesforce.
        """
        
        if recordsSize > 0:
            # Commit changes to SF.
            counter = 0
            helpList = []
            # We can't update more than 200 at once.
            while counter < recordsSize:
                helpList.append(records[counter])
                if len(helpList) == 200 or counter == recordsSize -1:
                    try:
                        # Store output from Salesforce to know which atts where inserted.
                        aux = service.create(helpList)
                        for index, val in enumerate(aux):
                            if val["success"]:
                                """ 
                                According to SF SOAP API the output of dmls is in the same order of the parameter list
                                so get the relative record in the helpList 
                                """
                                successParentIds.append("\'" + helpList[index]["ParentId"] + "\'")
                                logFile.write(helpList[index]["ParentId"] + "," + val["id"] + "\n")
                            else:
                                logFile2.write(helpList[index]["ParentId"] + "-->" + str(val["errors"]) + "\n")
                    except:
                        traceback.print_exc()
                    helpList = []
                counter+=1
            logFile.close()
            logFile2.close()
            return successParentIds
            
        
        

    
        
        
        