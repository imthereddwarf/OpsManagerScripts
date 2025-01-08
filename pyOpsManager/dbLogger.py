'''
Created on Feb 7, 2022

@author: peter.williamson
'''
import pymongo
from pymongo import ReturnDocument
from bson.codec_options import CodecOptions
from datetime import datetime, timedelta, timezone
from time import sleep
import sys


class dblogError(Exception):
    pass

class statusError(dblogError):
    # Nothing wrong but this node can't be patched
    pass

class cmdError(dblogError):
    # Invalid command line arguments
    pass

class dbError(dblogError):
    # Error accessing the control database
    pass

class cntrlDB:
    
    NEW = "New"
    COMPLETED = "Complete"
    INPROGRESS = "Shutdown in progress"
    HALTED = "Patch Group Halted"
    FORCEMODE = "Force Mode active"
    RESTARTING = "Restart in progress" 
    
    # Save Types
    
    MAINTENANCE = "Before Maintenance"
    REQUEST = "Save Requested"
    FORCE = "Before Forced Update"

    def __init__(self,URI,projectId,projectName):
        self.controlColl = None
        self.logColl = None 
        self.configColl = None 
        self.projectId = None
        self.controlDoc = None
        self.configDoc = None
        self.projName = None 
        if isinstance(projectName,str):
            self.projName = projectName
    

        myclient = pymongo.MongoClient(URI) #insert credentials into the URI and open a connection
        myDB = myclient.get_default_database()
        self.controlColl = myDB["control"].with_options(codec_options=CodecOptions(tz_aware=True,tzinfo=timezone.utc))
        self.logColl = myDB["messageLog"].with_options(codec_options=CodecOptions(tz_aware=True,tzinfo=timezone.utc))
        self.configColl = myDB["projectConfig"].with_options(codec_options=CodecOptions(tz_aware=True,tzinfo=timezone.utc))
        self.configHistoryColl = myDB["projectConfigHist"].with_options(codec_options=CodecOptions(tz_aware=True,tzinfo=timezone.utc))
        if "projectConfigHist" not in myDB.list_collection_names():
            self.configHistoryColl.create_index([("projectId",1),("version",1)],unique=True)
        self.projectId = projectId

    def getStatus(self):
        if self.controlDoc != None:
            return self.controlDoc["status"]
        searchClause= {"_id": self.projectId}
        lockDoc = self.controlColl.find_one(searchClause)
        if lockDoc == None:  # Not found so check it exists
            return None
        self.controlDoc = lockDoc 
        return lockDoc["status"]
        
    def getLock(self,myName):
        now = datetime.now(timezone.utc)
        lockTimeout = now - timedelta(seconds =5)
        searchNotLocked = {"_id": self.projectId, "$or": [{"lockedBy": {"$exists": False}}, {"lockedAt": {"$lt": lockTimeout}}]}
        gotLock = {"lockedBy": myName, "lockedAt": datetime.now(timezone.utc)}
        lockDoc = self.controlColl.find_one_and_update(searchNotLocked,{"$set": gotLock},return_document=ReturnDocument.AFTER)
        if lockDoc == None:  # Not found so check it exists
            projectControl = {"_id": self.projectId}
            gotLock["status"] = self.NEW
            lockDoc = self.controlColl.find_one_and_update(projectControl, {"$setOnInsert": gotLock},  upsert=True, return_document=ReturnDocument.AFTER)
            del gotLock["status"]  
        #We either got the lock or someonelse has it so Loop until we have it    
          
        while not( (lockDoc != None) and ("lockedBy" in lockDoc) and (lockDoc["lockedBy"] == myName)):
            sleep(1)
            lockDoc = self.controlColl.find_one_and_update(searchNotLocked,{"$set": gotLock},return_document=ReturnDocument.AFTER)
        self.controlDoc = lockDoc 
        return lockDoc["status"]
    
    def doUnlock(self,newStatus=None,newValues=None):
        projectControl = {"_id": self.projectId}
        removeLock = {"$unset": {"lockedBy": 1, "lockedAt": 1}}
        if newValues != None:
            if newStatus != None:
                newValues["status"] = newStatus
            removeLock["$set"] = newValues
        else:
            if newStatus != None:
                removeLock["$set"] = {"status": newStatus}
        if newStatus == self.INPROGRESS:
            removeLock["$inc"] = {"patchWindowNumber": 1} 
        lockDoc = self.controlColl.find_one_and_update(projectControl,removeLock, return_document=ReturnDocument.AFTER)
        if lockDoc == None:
            raise dbError("doUnlock: Control Record was not updated")
        self.controlDoc = lockDoc 
        return 0
    

    
    def logEvent(self,hostname,severity,message,WindowId=None):
        now = datetime.now(timezone.utc)
        eventDoc = {"projectId": self.projectId, "projectName": self.projName, "hostName": hostname, "msgType": severity, "message": message,"ts": now }
        if WindowId != None:
            eventDoc["maintainanceWindowId"] =  WindowId
        status = self.logColl.insert_one(eventDoc)
        if not status.acknowledged:
            raise dbError('logEvent: Failure to insert event - "{}".'.format(message))
        return 
    
    
    def getMessages(self,WindowId=None,Limit=5):
        if WindowId == None:
            query = {"projectId": self.projectId}
        else:
            query = {"projectId": self.projectId, "maintainanceWindowId": WindowId}
        cur = self.logColl.find(query).sort("ts",-1).limit(Limit)
        messages = []
        for msg in cur:
            messages.append(msg)
        return messages

    
class myLogger:
    
    DEBUG = 0
    INFO = 1
    MESSAGE = 2
    WARNING = 3
    ERROR = 4
    FATAL = 5
    
    
    def __init__(self,myName,db=None,syslogd=None,file=None,severity=None):
        self.errorText = ["Debug", "Info", "Message", "Warning", "Error", "Fatal"]
        self.myName = myName # Any identifier
        self.db = db
        self.syslog = syslogd
        self.maintWindowId = None
        if file is None:
            self.file = sys.stderr
        elif file == "-":
            self.file = sys.stdout
        elif isinstance(file,str):
            try:
                f = open(file,"w")
            except Exception as e:
                print("Error opening {}, {}. Using stderr for output".format(file,e))
                self.file = sys.stderr
            else:
                self.file = f
        else:
            print("Logger: file parameter must be a string or None, using stderr.",sys.stderr)
            self.file = file
        if (severity is None) or (severity < 0) or (severity > 5):
            self.sevLevel = self.MESSAGE
        else:
            self.sevLevel = severity
        return
    
    def logDebug(self,message):
        if self.DEBUG >= self.sevLevel:
            print("DEBUG: {}".format(message),file=self.file)
        return
    
    def logInfo(self,message):
        if self.INFO >= self.sevLevel:
            print("INFO: {}".format(message),file=self.file)
        return    
    
    def logMessage(self,message,logDB=False):
        if self.MESSAGE >= self.sevLevel:
            print(message,file=self.file)
        if logDB:
            sevString = self.errorText[self.MESSAGE]
            self.db.logEvent(self.myName,sevString,message,self.maintWindowId)
        return
    
    def logWarning(self,message,logDB=False):
        if self.WARNING >= self.sevLevel:
            print("WARNING: {}".format(message),file=self.file)
        if logDB:
            sevString = self.errorText[self.WARNING]
            self.db.logEvent(self.myName,sevString,message,self.maintWindowId)
        return

    def logError(self,message,logDB=False):
        if self.ERROR >= self.sevLevel:
            print("ERROR: {}".format(message),file=self.file)
        if logDB:
            sevString = self.errorText[self.ERROR]
            self.db.logEvent(self.myName,sevString,message,self.maintWindowId)
        return
    
    def logFatal(self,message,logDB=False):
        if self.MESSAGE >= self.sevLevel:
            print("FATAL: {}".format(message),file=self.file)
        if logDB:
            sevString = self.errorText[self.FATAL]
            self.db.logEvent(self.myName,sevString,message,self.maintWindowId)
        return
    
    def logProgress(self):
        self.file.write(".")
        self.file.flush()
    
    def logComplete(self):
        print(" ",file=self.file)
        
    def setWindow(self,ID):
        self.maintWindowId = ID
        
