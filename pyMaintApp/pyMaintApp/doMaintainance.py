#!/usr/bin/env python3
'''
doMaintainance -- Call Ops Manager to shutdown a node for patching

Run for a node being patched this script will call MongoDb Ops Manager to safely place the database cluster in Maintainance Mode

@author:     Peter Williamson, Sr Consulting Engineer

@copyright:  2021 MongoDB Inc. All rights reserved.

@license:    license

@contact:    peter.williamson@mongodb.com
@deffield    updated: Updated
'''
# import pymongo
import logging, sys
#from bson import json_util
import requests
import os
import socket
import json
from requests.auth import HTTPDigestAuth
from argparse import ArgumentParser
from argparse import RawDescriptionHelpFormatter
from datetime import datetime, timedelta, timezone
import dateutil.parser
import time
import pymongo
import traceback
from pymongo import ReturnDocument
from bson.codec_options import CodecOptions
from time import sleep
from platform import node
import tzlocal
import tempfile
import re
import math 
import urllib3
from dateutil import tz
from dns.rdataclass import NONE
import argparse
from pickle import TRUE, FALSE
from cffi.api import basestring









__all__ = []
__version__ = 0.2
__date__ = '2021-02-26'
__updated__ = '2021-02-26'

# Global variables set from the environment

publicKey = None
privateKey = None
OMRoot = None
OMServer = None
OMInternal = None # Private name of OM Server
DBURI = "mongodb://localhost:27017"
tokenLocation = "/tmp/MONGODB_MAINTAINANCE_IN_PROGRESS"
failsafeLogLocation = "/tmp"
agentCommand = None
logger = None
validID = re.compile("^[a-fA-F0-9]{24}$")
alertTypeNames = ["HOST", "REPLICA_SET", "CLUSTER", "AGENT", "BACKUP"]


myRealName = socket.gethostname()
myName = myRealName # May be overidden 


targetProject = 'PYA_DEV'  #Atlas Project name
clusterName = "app_dev"   #Atlas Cluster name

class myError(Exception):
    pass

class statusError(myError):
    # Nothing wrong but this node can't be patched
    pass

class cmdError(myError):
    # Invalid command line arguments
    pass

class dbError(myError):
    # Error accessing the control database
    pass

class fatalError(myError):
    # We can't proceed due to an error
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
        
    def getLock(self):
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
    
    def startNode(self,hostname):
        if not hostname in self.controlDoc["patchData"]["validatedHosts"]:
            if hostname in self.controlDoc["patchData"]["activeHosts"]:
                self.doUnlock()
                logger.logWarning("{} has already been prepared for Maintainance".format(hostname))
                return(0)
            elif hostname in self.controlDoc["patchData"]["completedHosts"]:
                self.doUnlock()
                raise statusError(hostname+" has already finished Maintainance")
            elif hostname in self.controlDoc["patchData"]["skippedHosts"]:
                logger.logWarning("{} is being skipped.".format(hostname),logDB=True)
                self.doUnlock()
                return 1
            else:
                self.doUnlock()
                raise statusError(hostname+" has not been prepared for Maintainance")
        projectControl = {"_id": self.projectId}
        changes = {"$unset": {"lockedBy": 1, "lockedAt": 1},"$pull": {"patchData.validatedHosts": hostname},"$push": {"patchData.activeHosts": hostname},"$currentDate": {"patchData.lastUpdate": True}}
        status = self.controlColl.update_one(projectControl,changes)
        if status.matched_count != 1:
            raise dbError("startNode: Control Record was not updated")
        return 0
    
    def endNode(self,hostname):
        if hostname in self.controlDoc["patchData"]["activeHosts"]:
            projectControl = {"_id": self.projectId}
            changes = {"$unset": {"lockedBy": 1, "lockedAt": 1},"$pull": {"patchData.activeHosts": hostname}, "$push": {"patchData.completedHosts": hostname}, "$currentDate": {"patchData.lastUpdate": True} }
            status = self.controlColl.update_one(projectControl,changes)
            if status.matched_count != 1:
                raise dbError("endNode: Control Record was not updated")
            return True
        else:
            if myName in self.controlDoc["patchData"]["validatedHosts"]:
                logger.logError("Host {} has not yet been patched. Use --start before --finish.".format(hostname),logDB=True)
            elif myName in self.controlDoc["patchData"]["completedHosts"]:
                logger.logMessage("Host {} has already completed patching.".format(hostname))
            elif myName in self.controlDoc["patchData"]["skippedHosts"]:
                logger.logMessage("Patching skipped for host {}.".format(hostname))
            else:
                logger.logMessage("Host {} is not part of this patch group.".format(hostname))
            self.doUnlock()
            return False
                
    def startPatch(self,hosts,active,resetDoc,patchGroup,maintId, disabledAlerts, skippedHosts, originalConfig, failedNodes, goalVersion):
        now = datetime.now(timezone.utc)
        completed = []
        projectControl = {"_id": self.projectId}
        patchData = {"patchCount": len(hosts), "currentPatchGroup": patchGroup, "validatedHosts": hosts, "OMConfigVersion": goalVersion, \
                      "activeHosts": active, "completedHosts": completed, "skippedHosts": skippedHosts, "originalSettings": resetDoc, "lastUpdate":  now}
        if maintId != None:
            patchData["maintId"] = maintId
        if len(disabledAlerts) > 0:
            patchData["disabledAlerts"] = disabledAlerts
        changes = {"$unset": {"lockedBy": 1, "lockedAt": 1},\
                   "$set": {"patchData": patchData, "status": self.HALTED, "originalConfig": originalConfig},\
                   "$currentDate": { "startTime": True}}
        if len(failedNodes) > 0:
            changes["$set"]["deployFailed"] = failedNodes
        status = self.getLock()
        if status != self.INPROGRESS:
            raise statusError('Control doc in "{}" state, "{}" expected.',status,self.INPROGRESS)
        status = self.controlColl.update_one(projectControl,changes)
        if status.matched_count != 1:
            raise dbError("startPatch: Control Record was not updated")
        return 0
    
    def startForce(self, failedNodes, originalConfig):
        projectControl = {"_id": self.projectId}
        changes = {"$unset": {"lockedBy": 1, "lockedAt": 1},\
                   "$set": {"status": self.FORCEMODE, "originalConfig": originalConfig},\
                   "$currentDate": { "startTime": True}}
        if len(failedNodes) > 0:
            changes["$set"]["deployFailed"] = failedNodes
        else:
            changes["$unset"] = {"deployFailed": 1}
        status = self.getLock()
        if status != self.INPROGRESS:
            raise statusError('Control doc in "{}" state, "{}" expected.',status,self.INPROGRESS)
        status = self.controlColl.update_one(projectControl,changes)
        if status.matched_count != 1:
            raise dbError("startPatch: Control Record was not updated")
        return 0
    
    def endPatch(self):
        projectControl = {"_id": self.projectId}
        changes = {"$unset": {"lockedBy": 1, "lockedAt": 1, "patchData": 1, "deployFailed": 1},"$set": {"status": self.COMPLETED}}
        status = self.getLock()
        if status != self.RESTARTING:
            raise statusError('Control doc in "{}" state, "{}" expected.'.format(status,self.RESTARTING))
        status = self.controlColl.update_one(projectControl,changes)
        if status.matched_count != 1:
            raise dbError("endPatch: Control Record was not updated")
        return 0
    
    
    def doReset(self):
        if self.controlDoc["lockedBy"] != myName:
            raise statusError("Control Doc is not locked by us.")
        projectControl = {"_id": self.projectId}
        changes = {"$unset": {"lockedBy": 1, "lockedAt": 1, "patchData": 1},"$set": {"status": self.COMPLETED}}
        status = self.controlColl.update_one(projectControl,changes)
        if status.matched_count != 1:
            raise dbError("endPatch: Control Record was not updated")
        return 0
    
    def allDone(self,hostname):
        if (len(self.controlDoc["patchData"]["activeHosts"]) == 1) and (len(self.controlDoc["patchData"]["validatedHosts"]) == 0)\
                and (hostname == self.controlDoc["patchData"]["activeHosts"][0]):
            return True 
        return False
    
    def logEvent(self,hostname,severity,message,WindowId=None):
        now = datetime.now(timezone.utc)
        eventDoc = {"projectId": self.projectId, "projectName": self.projName, "hostName": hostname, "msgType": severity, "message": message,"ts": now }
        if WindowId != None:
            eventDoc["maintainanceWindowId"] =  WindowId
        status = self.logColl.insert_one(eventDoc)
        if not status.acknowledged:
            raise dbError('logEvent: Failure to insert event - "{}".'.format(message))
        return 
    
    def getProjConfig(self):
        if self.configDoc is None:
            projectConfig = {"_id": self.projectId}
            self.config_doc = self.configColl.find_one(projectConfig)
        return self.config_doc
    
    def updateProjConfig(self,newDoc):
        projectConfig = {"_id": self.projectId}
        newDoc.pop("_id",None)  #remove the _id if it exists
        self.configDoc = self.configColl.find_one_and_replace(projectConfig,newDoc,return_document=ReturnDocument.AFTER)
        status = self.configColl.replace_one(projectConfig,newDoc, upsert=True)
        if (status.matched_count != 1) and (status.upserted_id is None):
            raise dbError("updateProjConfig: Config Record was not updated for {}".format(self.projectId))
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
    
    def addSavedOMConfig(self,config,saveType,comment,OMversion):
        now = datetime.now(timezone.utc)
        configHistDoc = {"projectId": self.projectId, "ProjectName":  self.projName, "saveDT": now, "comment": comment, \
                         "configDoc": config, "version": OMversion, "saveType": saveType}
        try:
            status = self.configHistoryColl.insert_one(configHistDoc)
        except pymongo.errors.DuplicateKeyError:
            
            currentDoc = self.configHistoryColl.find_one({"projectId": self.projectId, "version": OMversion}, \
                                                         {"_id":1, "saveDT":1, "version":1})
            logger.logWarning("Configuration version {version} was already saved on {saveDT:%Y-%m-%d %H:%M} UTC.".\
                              format(**currentDoc))
            return currentDoc["_id"]
            
        if not status.acknowledged:
            raise dbError("addSavedOMConfig: Config History Record was not inserted for {}".format(self.projectId))
        return status.inserted_id
    
    def getSavedConfig(self,version):
        filterSpec = {"projectId": self.projectId, "version": version}
        historyDoc = self.configHistoryColl.find_one(filterSpec)
        if "configDoc" in historyDoc:
            return historyDoc["configDoc"]
        else:
            raise None
    def getConfigHistory(self,earliest=90):
        now = datetime.now(timezone.utc)
        startDT = now - timedelta(days =earliest) # Default go back 3 months
        filterSpec = {"projectId": self.projectId, "saveDT": {"$gt": startDT}}
        projection = {"_id":0, "ProjectName": 1, "saveDT": 1, "comment":1, "version":1, "saveType": 1 }
        history = []
        for configDoc in self.configHistoryColl.find(filterSpec,projection).sort("version",-1).limit(40):
            history.append(configDoc)
        return history
    
class myLogger:
    
    DEBUG = 0
    INFO = 1
    MESSAGE = 2
    WARNING = 3
    ERROR = 4
    FATAL = 5
    
    
    def __init__(self,host,db=None,syslogd=None,file=None,severity=None):
        self.errorText = ["Debug", "Info", "Message", "Warning", "Error", "Fatal"]
        self.myName = host
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
        
class OpsManager:
    def __init__(self,omUri,omInternal,timeout=None,cafile=True):
        self.OMRoot = omUri+"/api/public/v1.0"
        self.Server = omUri
        self.internalName = omInternal
        self.deployTimeout = timeout
        httpSession = requests.Session()
        httpSession.verify = cafile
        self.OMSession = httpSession

        
    def doRequest(self,method,notFoundOK=False):
        response = self.OMSession.get(self.OMRoot+method, auth=HTTPDigestAuth(publicKey, privateKey))
        if (response.status_code != 200) :
            if (response.status_code == 404) and notFoundOK:
                return
            resp = response.json()
            if ("error" in resp) and ("detail" in resp):
                raise fatalError(self.OMRoot+method+": "+str(resp["error"])+": ",resp["detail"])
            else:
                raise fatalError("Error OM request - "+str(response.status_code)+": "+OMRoot+method)

        logger.logDebug(self.OMRoot+method)
        return(response.json())

    def doPut(self,method,postData):
        if isinstance(postData,dict):
            response = self.OMSession.put(self.OMRoot+method, data = json.dumps(postData), auth=HTTPDigestAuth(publicKey, privateKey), headers= {"Content-Type": "application/json"})
        else:
            response = self.OMSession.put(self.OMRoot+method, data = postData, auth=HTTPDigestAuth(publicKey, privateKey), headers= {"Content-Type": "application/json"})
        if (response.status_code != 200) :
            resp = response.json()
            if ("error" in resp) and ("detail" in resp):
                raise fatalError(self.OMRoot+method+": "+str(resp["error"])+": ",resp["detail"])
            else:
                raise fatalError("Error OM request - "+str(response.status_code)+": "+OMRoot+method)
        logger.logDebug(self.OMRoot+method)
        return 0
    
    def doPost(self,method,postData):
        if isinstance(postData,dict):
            response = self.OMSession.post(self.OMRoot+method, data = json.dumps(postData), auth=HTTPDigestAuth(publicKey, privateKey), headers= {"Content-Type": "application/json"})
        else:
            response = self.OMSession.post(self.OMRoot+method, data = postData, auth=HTTPDigestAuth(publicKey, privateKey), headers= {"Content-Type": "application/json"})
        if (response.status_code >= 300) :
            resp = response.json()
            if ("error" in resp) and ("detail" in resp):
                raise fatalError(self.OMRoot+method+": "+str(resp["error"])+": ",resp["detail"])
            else:
                raise fatalError("Error OM request - "+str(response.status_code)+": "+self.OMRoot+method)
        logger.logDebug(self.OMRoot+method)
        return response.json()
    
    def doPatch(self,method,postData):
        response = self.OMSession.patch(self.OMRoot+method, data = json.dumps(postData), auth=HTTPDigestAuth(publicKey, privateKey), headers= {"Content-Type": "application/json"})
        if (response.status_code == 404):
            return None
        elif (response.status_code != 200) :
            resp = response.json()
            if ("error" in resp) and ("detail" in resp):
                raise fatalError(self.OMRoot+method+": "+str(resp["error"])+": ",resp["detail"])
            else:
                raise fatalError("Error OM request - "+str(response.status_code)+": "+OMRoot+method)
        logger.logDebug(self.OMRoot+method)
        return response.json()
    
    def followLink(self,url):
        if self.internalName is None:
            request_url = url
        else:
            request_url = url.replace(self.internalName,self.Server)
        response = self.OMSession.get(request_url, auth=HTTPDigestAuth(publicKey, privateKey))
        if (response.status_code != 200) :
            resp = response.json()
            if ("error" in resp) and ("detail" in resp):
                raise fatalError(request_url+": "+str(resp["error"])+": ",resp["detail"])
            else:
                raise fatalError("Error OM request - "+str(response.status_code)+": "+request_url)
        logger.logDebug(url.replace(request_url))
        return(response.json())
            
    def findHost(self,hostName,projectID=None):
        myProjects = []
        hostProjects = []
        hostInfo = []
        if projectID is None:
            for project in self.doRequest("/groups")["results"]:
                myProjects.append(project["id"])
        else:
            myProjects.append(projectID)
        for prjID in myProjects:
            for host in self.doRequest("/groups/"+prjID+"/hosts")["results"]:
                if host["hostname"] == hostName:
                    if prjID not in hostProjects:
                        hostProjects.append(prjID)
                        hostInfo.append(host)
        if len(hostProjects) == 1:
            return(hostInfo[0])
        else:
            return(None)
class hostStatus:
    
    def __init__(self,projectId,auto):
        self.projectId = projectId
        self.hostStatus = {}
        self.automation = auto
        response = auto.opsManager.doRequest("/groups/"+projectId+"/hosts")
        now = datetime.now(timezone.utc)
        delayedping = now - timedelta(minutes =2)
        for host in response["results"]:
            lastPing = dateutil.parser.parse(host["lastPing"])
            if host["hostname"] not in self.hostStatus:
                self.hostStatus[host["hostname"]] = True # Assume its up
            if lastPing < delayedping:
                self.hostStatus[host["hostname"]] = False # any delayed ping and assume down
 
    
    def nodeUp(self,node):
        host = self.automation.getHostname(node)
        if host in self.hostStatus:
            return self.hostStatus[host]
        return(False)

    def hostUp(self,host):

        if host in self.hostStatus:
            return self.hostStatus[host]
        return(False)
        
class automation:
    
    def __init__(self,OM,projectId,projectName,db,autoconfig=None,skipCheck=False):
        self.config = autoconfig
        self.projectId = projectId
        self.projName = projectName
        self.NodeHostMapping = {}
        self.HostNodeMapping = {}
        self.configIDX = {}
        self.nodeTags = {}
        self.opsManager = OM
        self.deployTimeout = OM.deployTimeout
        self.prjConfig = db.getProjConfig()
        self.db = db
        self.newVersion = None # Version of of our updated config
        
        # First check we are at goal state
        if not skipCheck:
            response = self.opsManager.doRequest("/groups/"+projectId+"/automationStatus")
            self.currentVersion = response["goalVersion"]
            goalState = True
            for node in response["processes"]:
                if node["lastGoalVersionAchieved"] != self.currentVersion:
                    goalState = False
                    break
            if not goalState:
                self.config = None 
                return 
        
        if autoconfig is None:
            self.config = self.opsManager.doRequest("/groups/"+projectId+"/automationConfig")
            
        rIdx = 0
        for replset in self.config["replicaSets"]:
            nIdx = 0
            for node in replset["members"]:
                hostLoc = {"replSet": rIdx, "node": nIdx}
                self.configIDX[replset["_id"]+":"+node["host"]] =  hostLoc
                if "tags" in node:
                    self.nodeTags[node["host"]] = node["tags"]
                else:
                    self.nodeTags[node["host"]] = {}
                nIdx += 1
            rIdx += 1
        
        pIdx = 0
        for process in self.config["processes"]:
            self.NodeHostMapping[process["name"]] = {"hostName": process["hostname"], "isStopped": process["disabled"], "index": pIdx,\
                                                      "type": process["processType"] }
            if process["processType"] != "mongos":
                container = process ["args2_6"]["replication"]["replSetName"]
                if process["hostname"] in self.HostNodeMapping:
                    self.HostNodeMapping[process["hostname"]]["replicaSet"] = container
                    self.HostNodeMapping[process["hostname"]]["name"] = process["name"]
                    self.HostNodeMapping[process["hostname"]]["isStopped"] = process["disabled"]
                    self.HostNodeMapping[process["hostname"]]["index"] = pIdx
                else:
                    self.HostNodeMapping[process["hostname"]] = {"replicaSet": container,"name": process["name"], "isStopped": process["disabled"], "index": pIdx}
            else:
                if process["hostname"] in self.HostNodeMapping:
                    self.HostNodeMapping[process["hostname"]]["mongosName"] = process["name"]
                    self.HostNodeMapping[process["hostname"]]["mongosIsStopped"] = process["disabled"]
                    self.HostNodeMapping[process["hostname"]]["mongosIndex"] = pIdx
                else:
                    self.HostNodeMapping[process["hostname"]] = {"mongosName": process["name"], "mongosIsStopped": process["disabled"], "mongosIndex": pIdx}
            pIdx += 1
        
        #
        # Get mongos tags
        #
        if (self.prjConfig != None) and ("mongos" in self.prjConfig):
            for nodeName in self.prjConfig["mongos"]:
                node = self.prjConfig["mongos"][nodeName]
                if "tags" in node:
                    self.nodeTags[nodeName] = node["tags"]
                else:
                    self.nodeTags[nodeName] = {}
            

        return
    
    def configCurrent(self):
        response = self.opsManager.doRequest("/groups/"+self.projectId+"/automationStatus")
        if self.currentVersion == response["goalVersion"]:  # Nothing has changed
            return True 
        else:
            return False 
        
    def getPatchGroup(self,hostName):
        node = self.getNodeName(hostName)
        if node == None:
            return None
        tags = self.nodeTags[node]
        if "patchGroup" in tags:
            return tags["patchGroup"]
        else:
            return None 
            
    def getHostIdx(self,hostName):
        replNode = self.HostNodeMapping[hostName]
        configKey = replNode["replicaSet"]+":"+replNode["name"]
        return self.configIDX[configKey]
    
    def getNodeIdx(self,replset,node):
        configKey = replset+":"+node
        return self.configIDX[configKey]
    
    def getHostProcIdx(self,hostname):
        return(self.HostNodeMapping[hostname]["index"])
    
    def getNodeProcIdx(self,hostname):
        return(self.NodeHostMapping[hostname]["index"])
    
    def getHostname(self,nodename):
        return self.NodeHostMapping[nodename]["hostName"]
    
    def isNodeStopped(self,nodename):
        return self.NodeHostMapping[nodename]["isStopped"]
    
    def getNodeName(self,hostname):
        if hostname in self.HostNodeMapping:
            if "name" in self.HostNodeMapping[hostname]:
                return self.HostNodeMapping[hostname]["name"]
            elif "mongosName" in self.HostNodeMapping[hostname]:
                return self.HostNodeMapping[hostname]["mongosName"]
        return None
    
    def getAllNodes(self,hostname):
        nodes = []
        if hostname in self.HostNodeMapping:
            if "name" in self.HostNodeMapping[hostname]:
                nodes.append(self.HostNodeMapping[hostname]["name"])
            if "mongosName" in self.HostNodeMapping[hostname]:
                nodes.append(self.HostNodeMapping[hostname]["mongosName"])
        return nodes
    
    def getProcesses(self):
        return self.config["processes"]
    
    def getReplicaSets(self):
        return self.config["replicaSets"]
    
    def gotMajority(self,totalVotes,numArbiters,activeVotes,numHidden):
        newVotes = activeVotes + numHidden
        dataBearingVotes = newVotes - numArbiters
        if dataBearingVotes > (totalVotes/2):
            return True 
        else:
            return False 
    
    def gotQuorum(self,totalVotes,numArbiters,votesLost,numHidden):
        newVotes = totalVotes + numHidden - votesLost
        if newVotes > (totalVotes/2):
            return True 
        else:
            return False 
    
    def allGoal(self):
        response = self.opsManager.doRequest("/groups/"+self.projectId+"/automationStatus")
        targetVersion  = response["goalVersion"]
        self.newVersion = targetVersion
        notAtGoal = 0
        for host in response["processes"]:
            if host["lastGoalVersionAchieved"] != targetVersion:
                logger.logDebug(host["hostname"]+" is not at Goal Version.")
                notAtGoal += 1
        if notAtGoal > 0:
            return False
        return True
    
    def notGoal(self):
        response = self.opsManager.doRequest("/groups/"+self.projectId+"/automationStatus")
        targetVersion  = response["goalVersion"]
        notAtGoal = []
        for host in response["processes"]:
            if host["lastGoalVersionAchieved"] != targetVersion:
                logger.logMessage(host["hostname"]+" is not at Goal Version.")
                if host["hostname"] not in notAtGoal:
                    notAtGoal.append(host["hostname"])
        return notAtGoal
    
    def deployChanges(self,newAutomation,checkAgents=False,override=False):
        if checkAgents:
            agents = self.opsManager.doRequest("/groups/"+self.projectId+"/agents/AUTOMATION")
            now = datetime.now(timezone.utc)
            delayedping = now - timedelta(minutes =2)
            missingAgents = 0
            for agent in agents["results"]:
                lastConf = dateutil.parser.parse(agent["lastConf"])
                if lastConf < delayedping:
                    logger.logInfo("Last response from "+agent["hostname"]+" at "+str(lastConf)+".")
                    missingAgents += 1
            if missingAgents > 0:
                raise fatalError("All agents must be responsive to sucessfully deploy! {} of {} agents not responding." \
                                 .format(missingAgents,len(agents["results"])))
        if (not override) and (not self.allGoal()): #override True -> Start new deploy
            raise fatalError("Another deploy already in progress!")
        status = self.opsManager.doPut("/groups/"+self.projectId+"/automationConfig",newAutomation)
        if status != 0:
            raise fatalError("Error status {} attempting to PUT new config!".format(status))
        logger.logMessage("Deploying changes to {}.".format(self.projName))
        deployTime = 0
        notDeployed = []
        while not self.allGoal():
            logger.logInfo("Waiting for Goal state.")
            logger.logProgress()
            time.sleep(5)
            deployTime += 5
            if (self.deployTimeout != None) and (deployTime > self.deployTimeout):
                logger.logComplete()
                notDeployed = self.notGoal()
                break
        logger.logComplete()
        if len(notDeployed) > 0:
            logger.logWarning("{} Nodes failed to deploy.".format(len(notDeployed)),logDB=True)
        return notDeployed
    
    def disableAlerts(self,globalAlerts,perHostAlerts):
        #
        # Start a Maintainance winodow
        #
        prjConfig = self.prjConfig
        maintId = None
        if len(self.prjConfig["maintainanceAlertGroups"]) > 0:
            now = datetime.now(timezone.utc)
            nowIso = now.isoformat()
            end = now+timedelta(hours=4)
            endIso = end.isoformat()
            maintData = {"startDate": nowIso, "endDate": endIso, "alertTypeNames": prjConfig["maintainanceAlertGroups"], "description": "Auto Generated"}
            maintResponse = self.opsManager.doPost("/groups/"+prjConfig["projectId"]+"/maintenanceWindows",maintData)
            maintId = maintResponse["id"]
        #
        # Disable Specific Alerts
        #
        disabledAlerts = []
        fullList = globalAlerts
        for alertId in perHostAlerts:
            if not alertId in fullList:
                fullList.append(alertId) 
                
        if len(fullList) > 0:
            postDisabled = {"enabled": False}
            for alertId in fullList:
                if self.opsManager.doPatch("/groups/"+prjConfig["projectId"]+"/alertConfigs/"+alertId,postDisabled) != None:
                    disabledAlerts.append(alertId)
        return maintId,disabledAlerts
    
    def enableAlerts(self,maintId,alerts):
        #
        # End a Maintainance winodow
        #
        prjConfig = self.prjConfig
        if maintId != None:
            now = datetime.now(timezone.utc)
            endIso = now.isoformat()
            maintData = {"endDate": endIso }
            maintId = self.opsManager.doPatch("/groups/"+prjConfig["projectId"]+"/maintenanceWindows/"+maintId,maintData)
        #
        # Disable Specific Alerts
        #
        if len(alerts) > 0:
            postEnabled = {"enabled": True}
            for alertId in alerts:
                if self.opsManager.doPatch("/groups/"+prjConfig["projectId"]+"/alertConfigs/"+alertId,postEnabled) == None:
                    logger.logWarning("Alert {} could not be found to enable.".format(alertId))
        return 
    
    def isCandidate(self,nodeInfo,shutdownSpec):
        if "dc" in shutdownSpec:
            if ("tags" in nodeInfo) and ("dc" in nodeInfo["tags"]) and (nodeInfo["tags"]["dc"] == shutdownSpec["dc"]):
                return True 
        if "patchGroup" in shutdownSpec:
            if ("tags" in nodeInfo) and ("patchGroup" in nodeInfo["tags"]) and (nodeInfo["tags"]["patchGroup"] == shutdownSpec["patchGroup"]):
                return True 
        if "nodeName" in shutdownSpec:
            node = shutdownSpec["nodeName"]
            if isinstance(node,list):
                if (nodeInfo["host"] in node):
                    return True
            else:
                if nodeInfo["host"] == node:
                    return True 
        return False 
    
    def startMaintainance(self,patchGroup,db,alrtConfig,comment,isForce=False):
        resetDoc = {}
        newAutomation = self.config
        hostStat = hostStatus(db.projectId,self)
        shutdownReason = db.MAINTENANCE
        if isForce:
            shutdownReason = db.FORCE 
        

        originalConfig = db.addSavedOMConfig(newAutomation,shutdownReason,comment,newAutomation["version"])
        
        stopped = []  # Node will be stopped
        activeNodes = [] # Stopping myself so I will move straight to active
        skippedHosts = [] # Nodes in the Patch Group that were skipped
        downHosts = []
        for replSet in self.config["replicaSets"]:
            members = active = voting = hidden = totalVotes = arbiter = pgVotes = 0
            inCurrentPg = []
            hiddenNodes = []
            activeVotes = 0 # Votes for nodes that will remain up
            for member in replSet["members"]:
                isDown = False
                if member["arbiterOnly"]:
                    arbiter += 1
                if member["hidden"]:
                    hidden += 1
                    hiddenNodes.append(member["host"])
                if member["votes"] > 0:
                    voting += 1
                    totalVotes += member["votes"]
                if not self.isNodeStopped(member["host"]) and hostStat.nodeUp(member["host"]):
                    active += 1
                    activeVotes += member["votes"]
                else:
                    isDown = True
                    if member["host"] not in downHosts:
                        downHosts.append(member["host"])
                if ("tags" in member) and self.isCandidate(member,patchGroup):
                    inCurrentPg.append(member["host"])
                    pgVotes += member["votes"]
                    if not isDown:
                        activeVotes -= member["votes"] # This node will be shutdown
                members += 1

            if len(inCurrentPg) == 0:
                logger.logWarning("No nodes in Patch Spec "+str(patchGroup)+" for replicaset "+replSet["_id"],logDB=True)
                continue
            elif (len(inCurrentPg) > 1) and (members >= 5):  # 5 or more nodes shutdown 2 is possible
                logger.logWarning("Multiple nodes in Patch Spec "+str(patchGroup)+" in replicaset "+replSet["_id"]+".",logDB=True)
            elif len(inCurrentPg) != 1:
                logger.logWarning("Only 1 node allowed per shard. {} configured in patch spec {} in replicaset {}." \
                                  .format(len(inCurrentPg),str(patchGroup),replSet["_id"]),logDB=True)
                for shutdownNode in inCurrentPg:
                    shutdownHost = self.getHostname(shutdownNode)
                    if shutdownHost not in skippedHosts:
                        skippedHosts.append(shutdownHost)
                continue
        

            #
            # 3 or 5 Node Cluster - all must be up and voting, shutdown one node
            #
            if members == 3 or members == 5:
                if self.gotMajority(totalVotes,arbiter,activeVotes,0):
                    for shutdownNode in inCurrentPg:
                        shutdownHost = self.getHostname(shutdownNode)
                        newAutomation["processes"][self.getNodeProcIdx(shutdownNode)]["disabled"] = True
                        resetDoc[shutdownNode] = {"host": shutdownHost, "disabled": False}
                        if shutdownHost == myName:
                            if not myName in activeNodes:
                                activeNodes.append(myName)
                        else:
                            if not shutdownHost in stopped:
                                stopped.append(shutdownHost)
                else:
                    logger.logWarning("No Majority for replica set "+replSet["_id"]+" skipping.",logDB=True)
                    for shutdownNode in inCurrentPg:
                        shutdownHost = self.getHostname(shutdownNode)
                        if shutdownHost not in skippedHosts:
                            skippedHosts.append(shutdownHost)
            #
            # 4 node cluster, should be one Hidden and 3 votes
            #
            elif members == 4:
                if len(inCurrentPg) != 1:
                    logger.logWarning("Must exactly 1  Patch Group "+str(patchGroup)+" in replicaset "+replSet["_id"]+".",logDB=True)
                shutdownNode = inCurrentPg[0]
                shutdownHost = self.getHostname(shutdownNode)
                if ((active == 4) and self.gotMajority(totalVotes,arbiter,activeVotes,hidden)) or \
                    ((active == 3) and (inCurrentPg[0] in  downHosts) ):
                    if not shutdownNode in hiddenNodes:  # if were not shutting down the hidden node we need to activate one
                        for node in hiddenNodes:
                            host = self.getHostname(node)
                            hostCfg = {"host": host}
                            autoLoc = self.getNodeIdx(replSet["_id"],node)
                            hostCfg["disabled"] = newAutomation["processes"][self.getNodeProcIdx(node)]["disabled"]
                            hostCfg["votes"] = newAutomation["replicaSets"][autoLoc["replSet"]]["members"][autoLoc["node"]]["votes"]
                            if hostCfg["votes"] == 0:
                                newAutomation["replicaSets"][autoLoc["replSet"]]["members"][autoLoc["node"]]["votes"] = 1
                            hostCfg["hidden"] = newAutomation["replicaSets"][autoLoc["replSet"]]["members"][autoLoc["node"]]["hidden"]
                            if hostCfg["hidden"] == True:
                                newAutomation["replicaSets"][autoLoc["replSet"]]["members"][autoLoc["node"]]["hidden"] = False
                            resetDoc[node] = hostCfg
                    hostCfg = {"host": shutdownHost, "disabled": False}
                    autoLoc = self.getNodeIdx(replSet["_id"],shutdownNode)
                    newAutomation["processes"][self.getNodeProcIdx(shutdownNode)]["disabled"] = True
                    hostCfg["votes"] = newAutomation["replicaSets"][autoLoc["replSet"]]["members"][autoLoc["node"]]["votes"]
                    if hostCfg["votes"] == 1:
                        newAutomation["replicaSets"][autoLoc["replSet"]]["members"][autoLoc["node"]]["votes"] = 0
                    hostCfg["priority"] = newAutomation["replicaSets"][autoLoc["replSet"]]["members"][autoLoc["node"]]["priority"]
                    if hostCfg["priority"] > 0:
                        newAutomation["replicaSets"][autoLoc["replSet"]]["members"][autoLoc["node"]]["priority"] = 0
                    resetDoc[shutdownNode] = hostCfg
                    if shutdownHost == myName:
                        if not myName in activeNodes:
                            activeNodes.append(myName)
                    else:
                        if not shutdownHost in stopped:
                            stopped.append(shutdownHost)
                else:
                    logger.logWarning("Inconsistent 4 node replica set "+replSet["_id"]+" skipping.",logDB=True)
                    for shutdownNode in inCurrentPg:
                        shutdownHost = self.getHostname(shutdownNode)
                        if shutdownHost not in skippedHosts:
                            skippedHosts.append(shutdownHost)
            else:
                logger.logWarning("Ignoring "+members+" member replicaset "+replSet["_id"],logDB=True)
        #
        # Check for Mongos
        #
        shardedClusters = {}
        for nodeName in self.prjConfig["mongos"]:
            node = self.prjConfig["mongos"][nodeName]
            cluster = node["cluster"]
            hostName = node["hostname"]
            if cluster in shardedClusters:
                clusterCounts = shardedClusters[cluster]
            else:
                clusterCounts = {"total": 0, "active": 0, "inPatchGroup": []}
                shardedClusters[cluster] = clusterCounts
            if ("tags" in node) and ('patchGroup' in node["tags"]):
                if node["tags"]["patchGroup"] == patchGroup:
                    clusterCounts["inPatchGroup"].append(nodeName)
            clusterCounts["total"] += 1
            pIdx = self.getNodeProcIdx(nodeName)
            if newAutomation["processes"][pIdx]["disabled"] == False and hostStat.hostUp(hostName):
                clusterCounts["active"] += 1
        for clusterName in shardedClusters:
            cluster = shardedClusters[clusterName]
            disabled = cluster["total"] - cluster["active"]
            maxDown = math.ceil(cluster["total"]/3) - disabled
            if len(cluster["inPatchGroup"]) <= maxDown:
                for node in cluster["inPatchGroup"]:
                    pIdx = self.NodeHostMapping[node]["index"]
                    shutdownHost = self.NodeHostMapping[node]["hostName"]
                    newAutomation["processes"][pIdx]["disabled"] = True
                    if shutdownHost == myName:
                        if not myName in activeNodes:
                            activeNodes.append(myName)
                    else:
                        if not shutdownHost in stopped:
                            stopped.append(shutdownHost)
                    resetDoc[node] = {"host": shutdownHost, "mongos": True, "disabled": False}
            else:
                logger.logWarning("Mongos for cluster {} can't be stopped. Of {} mongos, {} down, and {} in patch group"\
                                  .format(cluster,cluster["total"],disabled,len(cluster["inPatchGroup"])))
                for node in cluster["inPatchGroup"]:
                    shutdownHost = self.NodeHostMapping[node]["hostName"]
                    if shutdownHost not in skippedHosts:
                        skippedHosts.append(shutdownHost)
            
        dumpJSON(newAutomation,"after.json")
        #
        # Disable Alerts
        #
        alertsToDisable = []
        for host in activeNodes:
            hostAlerts = alrtConfig.getAlertsForHost(host)
            for aid in hostAlerts:  
                alertsToDisable.append(aid)
        for host in stopped:
            hostAlerts = alrtConfig.getAlertsForHost(host)
            for aid in hostAlerts:  
                alertsToDisable.append(aid)
            
        maintId, disabledAlerts = self.disableAlerts(alrtConfig.getGlobalAlerts(),alertsToDisable)
        
        origSettings = {"nodes": resetDoc}
        #
        # Check the Balancer
        # 
        if (len(self.config["balancer"]) == 0) and (len(self.config["sharding"]) > 0):
            logger.logWarning("Balancer is not managed by Ops Manager and can't be stopped")
        else:
            balancerRunning = []
            for shard in self.config["balancer"]:
                if self.config["balancer"][shard]["stopped"] != True:
                    balancerRunning.append(shard)
                    newAutomation["balancer"][shard]["stopped"] = True 
            if len(balancerRunning) > 0:
                logger.logMessage("Balancer stopped for clusters {}.".format(balancerRunning),logDB=True)
                origSettings["Balancer"] = balancerRunning
        #
        #And then deplot the changes
        #
        failedNodes =  self.deployChanges(newAutomation)
        for host in failedNodes:
            if host in activeNodes:
                activeNodes.remove(host)
                skippedHosts.append(host)
        if isForce:
            db.startForce(failedNodes,originalConfig)
            return True
        db.startPatch(stopped,activeNodes,origSettings,patchGroup,maintId, disabledAlerts, skippedHosts, originalConfig,failedNodes,self.newVersion)
        if myName in activeNodes:
            return True
        return False
    
    def endMaintainance(self,db):
        newAutomation = self.config
        resetDoc = db.controlDoc["patchData"]["originalSettings"]
        if "Balancer" in resetDoc:
            for shard in resetDoc["Balancer"]:
                newAutomation["balancer"][shard]["stopped"] = False 
        for node in resetDoc["nodes"]:
            resetSpec = resetDoc["nodes"][node]
            host = resetSpec["host"]
            if "mongos" in resetSpec:  # Only have a replSet config for servers
                autoLoc = None
            else:
                autoLoc = self.getHostIdx(host)
            autoProcIdx = self.getNodeProcIdx(node)
            if "votes" in resetSpec:
                newAutomation["replicaSets"][autoLoc["replSet"]]["members"][autoLoc["node"]]["votes"] = resetSpec["votes"]
            if "hidden" in resetSpec:
                newAutomation["replicaSets"][autoLoc["replSet"]]["members"][autoLoc["node"]]["hidden"] = resetSpec["hidden"]
            if "priority" in resetSpec:
                newAutomation["replicaSets"][autoLoc["replSet"]]["members"][autoLoc["node"]]["priority"] = resetSpec["priority"]
            if "disabled" in resetSpec:
                newAutomation["processes"][autoProcIdx]["disabled"] = resetSpec["disabled"]
        failedNodes = self.deployChanges(newAutomation)
        if len(failedNodes) > 0:
            logger.logError("Failed to restart {} nodes {}".format(len(failedNodes),failedNodes),logDB=True)
        if "maintId" in db.controlDoc["patchData"]:
            maintId = db.controlDoc["patchData"]["maintId"]
        else:
            maintId = None
        if "disabledAlerts" in db.controlDoc["patchData"]:
            disabledAlerts = db.controlDoc["patchData"]["disabledAlerts"]
        else:
            disabledAlerts = []
        self.enableAlerts(maintId, disabledAlerts)
        
        db.endPatch()
        
    def exposeHiddenNodes(self,db,comment):
        newAutomation = self.config
        hostStat = hostStatus(db.projectId,self)
        allExposed = []
        allStopped = []
        RSModified = []
        RSUnchanged = []
        originalConfig = db.addSavedOMConfig(newAutomation,db.FORCE,comment,newAutomation["version"])
        
        for replSet in self.config["replicaSets"]:
            if len(replSet["members"]) == 4: # only applies to replica sets with 4 nodes
                members = active = voting = hidden = totalVotes = arbiter = activeVotes = 0
                hiddenNodes = []
                downNodes = []
                for member in replSet["members"]:
                    if member["arbiterOnly"]:
                        arbiter += 1
                    if member["hidden"]:
                        hidden += 1
                        hiddenNodes.append(member["host"])
                    if member["votes"] > 0:
                        voting += 1
                        totalVotes += member["votes"]
                    if not self.isNodeStopped(member["host"]) and hostStat.nodeUp(member["host"]):
                        active += 1
                        activeVotes += member["votes"]
                    else:
                        if member["host"] not in downNodes:
                            downNodes.append(member["host"])
                        if member["host"] in hiddenNodes:
                            hiddenNodes.pop(member["host"])
                if (len(downNodes) > 0) and (len(hiddenNodes) > 0):
                    for i in range(len(downNodes)): # one for one
                        node = hiddenNodes[i]
                        host = self.getHostname(node)
                        allExposed.append(node)
                        hostCfg = {"host": host}
                        autoLoc = self.getNodeIdx(replSet["_id"],node)
                        hostCfg["disabled"] = newAutomation["processes"][self.getNodeProcIdx(node)]["disabled"]
                        hostCfg["votes"] = newAutomation["replicaSets"][autoLoc["replSet"]]["members"][autoLoc["node"]]["votes"]
                        if hostCfg["votes"] == 0:
                            newAutomation["replicaSets"][autoLoc["replSet"]]["members"][autoLoc["node"]]["votes"] = 1
                        hostCfg["hidden"] = newAutomation["replicaSets"][autoLoc["replSet"]]["members"][autoLoc["node"]]["hidden"]
                        if hostCfg["hidden"] == True:
                            newAutomation["replicaSets"][autoLoc["replSet"]]["members"][autoLoc["node"]]["hidden"] = False
                    for shutdownNode in downNodes:
                        shutdownHost = self.getHostname(shutdownNode)
                        allStopped.append(shutdownNode)
                        hostCfg = {"host": shutdownHost, "disabled": False}
                        autoLoc = self.getNodeIdx(replSet["_id"],shutdownNode)
                        newAutomation["processes"][self.getNodeProcIdx(shutdownNode)]["disabled"] = True
                        hostCfg["votes"] = newAutomation["replicaSets"][autoLoc["replSet"]]["members"][autoLoc["node"]]["votes"]
                        if hostCfg["votes"] == 1:
                            newAutomation["replicaSets"][autoLoc["replSet"]]["members"][autoLoc["node"]]["votes"] = 0
                        hostCfg["priority"] = newAutomation["replicaSets"][autoLoc["replSet"]]["members"][autoLoc["node"]]["priority"]
                        if hostCfg["priority"] > 0:
                            newAutomation["replicaSets"][autoLoc["replSet"]]["members"][autoLoc["node"]]["priority"] = 0
                    logger.logMessage("Replacing {} stopped nodes with {} hidden nodes in {}".format(len(downNodes), \
                                        len(hiddenNodes),replSet["_id"]), logDB=True)
                    RSModified.append(replSet["_id"])
                    
                else:
                    RSUnchanged.append(replSet["_id"])
                    logger.logMessage("Skipping {}: {} stopped nodes and {} hidden nodes.".format(replSet["_id"],len(downNodes), \
                                        len(hiddenNodes)), logDB=True)

            
        dumpJSON(newAutomation,"after.json")

  
        #
        failedNodes =  self.deployChanges(newAutomation)
        db.startForce(failedNodes,originalConfig)
        logger.logMessage("{} hidden nodes exposed, {} nodes not responding".format(len(allExposed),len(failedNodes)))
        logger.logInfo("Replicaset(s) modified:")
        for rs in RSModified:
            logger.logInfo("   {}".format(rs))
        logger.logInfo("Replicaset(s) unchanged:")
        for rs in RSUnchanged:
            logger.logInfo("   {}".format(rs))
            

        return 
        
        
class alertConfig:
    def __init__(self,OM,projectId,globalAlrtNames):
        self.projectId = projectId
        self.OM = OM
        self.alertsByHost = {}
        self.globalAlerts = []
        self.activeAlertTypes = []
        response = OM.doRequest("/groups/"+self.projectId+"/alertConfigs")
        for alert in response["results"]:
            if len(alert["matchers"]) > 0:
                for match in alert["matchers"]:
                    host = None
                    if match["fieldName"] == "HOSTNAME":
                        if match['operator'] == "EQUALS":
                            host = match["value"]
                            alertId = alert["id"]
                        else:
                            logger.logInfo("Unexpected match operator {} alert {}"\
                                              .format(match["operator"],alert["id"]))
                    elif match["fieldName"] == "HOSTNAME_AND_PORT":
                        if (match['operator'] == "EQUALS") or (match['operator'] == "REGEX"):  # Matches being used with a literal
                            components = match["value"].split(":")
                            host = components[0]
                            alertId = alert["id"]
                        else:
                            logger.logInfo("Unexpected match operator {} alert {}"\
                                              .format(match["operator"],alert["id"]))
                    else:
                        logger.logInfo("Unexpected match field Name {} alert {}"\
                                          .format(match["fieldName"],alert["id"]))
#                    print("Found Alert {} for host {}.".format(alertId,host))
                    if not host == None:
                        if host in self.alertsByHost:
                            self.alertsByHost[host].append(alertId)
                        else:
                            self.alertsByHost[host] = [alertId]
            if alert["eventTypeName"] in globalAlrtNames:
                self.globalAlerts.append(alert["id"])
            if alert["eventTypeName"] not in self.activeAlertTypes:
                self.activeAlertTypes.append(alert["eventTypeName"])
        return 
    
    def getAlertsForHost(self,host):
        if host in self.alertsByHost:
            return(self.alertsByHost[host])
        else:
            return []
        
    def getGlobalAlerts(self):
        return self.globalAlerts


def dumpJSON(dictionary,filename,always=False):
    if (logger.sevLevel > myLogger.DEBUG) and not always:
        return 
    try:
        f = open(filename,"w")
        f.write(json.dumps(dictionary,indent=4))
        logger.logDebug("Dictionary dumped to {}.".format(filename))
    except Exception as e:
        if always:
            logger.logDebug("Error attempting to extract JSON to {} ({}).".format(filename,format(e)))
        else:
            logger.logDebug("Error attempting to create debug dump to {} ({}).".format(filename,format(e)))
        pass
    finally:
        f.close
    return

def getJSON(dictionary):
    try:
        return json.dumps(dictionary,indent=4)
    except Exception as e:
        logger.logDebug("Error attempting to create config dump ({}).".format(format(e)))
    return "Error dumping config!"
    
def fmtDate(dateField):
    return dateField.astimezone(tz.tzlocal()).strftime("%x %X")
            
    
    
def getVariables():
    global publicKey, privateKey, OMServer, myName, OMInternal, OMRoot, DBURI,tokenLocation,agentCommand,failsafeLogLocation
    global cafile
    publicKey = os.getenv("OM_PUBLIC")
    privateKey = os.getenv("OM_PRIVATE")
    OMServer = os.getenv("OM_SERVER")
    
    temp = os.getenv("OM_HOSTNAME")
    if temp != None:
        myName = temp
    temp = os.getenv("OM_INT_URL")
    if temp != None:
        OMInternal = temp
    else:
        OMInternal = None 
    temp = os.getenv("OM_DB_URI")
    if temp != None:
        DBURI = temp
    temp = os.getenv("TOKENDIR")
    if temp != None:
        tokenLocation = temp+"/MONGODB_MAINTAINANCE_IN_PROGRESS"
    temp = os.getenv("RESTART_AGENT")
    if temp != None:
        agentCommand = temp
    temp = os.getenv("rootCA")
    if temp != None:
        cafile = temp
    temp = os.getenv("FAILSAFE_LOG")
    if temp != None:
        failsafeLogLocation = temp
        

 
# TODO - Create WF specific mapping                   
def dcMapping(hostname):
    host2dc = {"16": "cc","17": "cc","21": "cc","22": "ss","24": "sv","25": "sv","26": "sv","27": "ox","28": "ox","29": "ox","30": "az","31": "sl"}
    dcpart = hostname[10:12]
    if dcpart in host2dc:
        return(host2dc[dcpart])
    return("uk")

def getMongosInfo(projectInfo,auto,db):
    projectCfg = db.getProjConfig()
    #
    # Build a list of MONGOS processes
    #
    if projectCfg is None:
        projectCfg = {"projectName": projectInfo["name"], "orgId": projectInfo["orgId"], "projectId": projectInfo["id"],\
                      "alerts": [], "maintainanceAlertGroups": []}
        projectCfg["mongos"] = {}
    #
    # Build a list of existing mongos to check for any deleted one
    #
    existingNodes = []
    for node in projectCfg["mongos"]:
        existingNodes.append(node)
    # Loop through the automation config 
    clusters = {}
    for host in auto.config["processes"]:
        if host["processType"] == "mongos":
            if host["name"] in projectCfg["mongos"]:
                try:
                    existingNodes.remove(host["name"])
                except Exception:
                    pass
                projectCfg["mongos"][host["name"]]["hostname"] = host["hostname"]
                projectCfg["mongos"][host["name"]]["cluster"] = host["cluster"]
            else:
                projectCfg["mongos"][host["name"]] = {"hostname": host["hostname"], "cluster": host["cluster"], "tags": {}}
            if not (host["cluster"] in clusters):
                clusters[host["cluster"]] = []
            clusters[host["cluster"]].append({"name": host["name"], "tags": projectCfg["mongos"][host["name"]]["tags"]})
    
    if len(existingNodes) > 0:
        for node in existingNodes:
            projectCfg["mongos"][node]["deleted"] = True 
            clusters[projectCfg["mongos"][node]["cluster"]].append({"name": node, "deleted": True})
            
    return clusters, projectCfg
    
def genTags(projectInfo,auto,db):
    projectDoc = {"projectName": projectInfo["name"], "orgId": projectInfo["orgId"], "projectId": projectInfo["id"]}
    clusters, projectCfg = getMongosInfo(projectInfo,auto,db)
    #
    # Copy any Alerts or Maintainance Settings
    #
    if "alerts" in projectCfg:
        projectDoc["alerts"] = projectCfg["alerts"]
    else:
        projectDoc["alerts"] = []
        
    if "maintainanceAlertGroups" in projectCfg:
        projectDoc["maintainanceAlertGroups"] = projectCfg["maintainanceAlertGroups"]
    else:
        projectDoc["maintainanceAlertGroups"] = []
    #
    # Now generate the tag file  for the mongos
    #  
    serverNodes = []
    clusterProcs = []
    for cluster in clusters:
        # Look for existing patchGroup Settings
        setTags = {"cluster": cluster }
        hosts = clusters[cluster]
        nodes = []
        groupsCounts = [0,0,0,0,0]
        for node in hosts:
            if "tags" in node:
                if "patchGroup" in node["tags"]:
                    pg = node["tags"]["patchGroup"]
                    if type(pg) == int:
                        groupsCounts[pg] += 1
        # Now set missing ones
        for node in hosts:  
            myHostName = projectCfg["mongos"][node["name"]]["hostname"]
            if  ("deleted" in node) and (node["deleted"]):
                nodeTags = {"nodeName": node["name"], "hostName": myHostName, "deleted": True}
            else: 
                if ("tags" not in node) or (len(node["tags"]) == 0):
                    tags = {}
                    tags["dc"] = dcMapping(myHostName)
                    minVal = 9999
                    for i in range(3):
                        if groupsCounts[i] < minVal:
                            minVal = groupsCounts[i]
                            nextPG = i
                    tags["patchGroup"] = str(nextPG)
                    groupsCounts[nextPG] += 1
                else:
                    tags = node["tags"]
                    if "dc" not in tags:
                        tags["dc"] = dcMapping(myHostName)
                    if "patchGroup" not in tags:
                        minVal = 9999
                        for i in range(3):
                            if groupsCounts[i] < minVal:
                                minVal = groupsCounts[i]
                                nextPG = i
                        tags["patchGroup"] = str(nextPG)
                        groupsCounts[nextPG] += 1
                nodeTags = {"nodeName": node["name"], "hostName": myHostName, "tags": tags} 
            nodes.append(nodeTags)
        setTags["mongos"] = nodes
        clusterProcs.append(setTags)
    projectDoc["shardedClusters"] = clusterProcs
    #
    # Now tags for the servers

    for replset in auto.config["replicaSets"]:
        # Look for existing patchGroup Settings
        setTags = {"replicaSet": replset["_id"] }
        nodes = []
        groupsInSet = [False,False,False,False,False,False,False,False]
        for node in replset["members"]:
            if "tags" in node:
                if "patchGroup" in node["tags"]:
                    pg = node["tags"]["patchGroup"]
                    if type(pg) == int:
                        groupsInSet[pg] = True
        # Now set missing ones
        for node in replset["members"]:  
            myHostName = auto.getHostname(node["host"])
           
            if "tags" not in node:
                tags = {}
                tags["dc"] = dcMapping(myHostName)
                for i in range(8):
                    if not groupsInSet[i]:
                        groupsInSet[i] = True
                        tags["patchGroup"] = str(i)
                        break
            else:
                tags = node["tags"]
                if "dc" not in tags:
                    tags["dc"] = dcMapping(myHostName)
                if "patchGroup" not in tags:
                    for i in range(8):
                        if not groupsInSet[i]:
                            groupsInSet[i] = True
                            tags["patchGroup"] = str(i)
                            break
            nodeTags = {"nodeName": node["host"], "hostName": myHostName, "tags": tags}  
            nodes.append(nodeTags)
        setTags["nodes"] = nodes
        serverNodes.append(setTags)
    projectDoc["replicaSets"] = serverNodes
    db.updateProjConfig(projectCfg)
    return(projectDoc)
                  
def setTags(auto,tagDoc,projectInfo,db,om,alertInfo):
    # First get the Mongos Definition from the project config document
    _, projectCfg = getMongosInfo(projectInfo, auto, db)
    status = 0   # No changes yet
    #
    # First Check for updated Alert or Maintainance config
    #
    validAlerts = []
    if "alerts" in tagDoc:
        for alert in tagDoc["alerts"]:
            if isinstance(alertInfo.activeAlertTypes,list) and isinstance(alert, str) and (alert not in alertInfo.activeAlertTypes):
                logger.logWarning('No Alerts of type "{}" used in the project.'.format(alert))
            validAlerts.append(alert)

        if ((not "alerts" in projectCfg) and (len(validAlerts) > 0)) or (validAlerts != projectCfg['alerts']):
            projectCfg['alerts'] =validAlerts
            status |= 4
                
    validMaint = [] 
    allValid = True           
    if "maintainanceAlertGroups" in tagDoc:
        for maint in tagDoc["maintainanceAlertGroups"]:
            if isinstance(maint,str) and (maint in alertTypeNames):
                validMaint.append(maint)
            else:
                allValid = False
                logger.logMessage('Alert type "{}" is not valid'.format(maint))
        if allValid:
            if ((not "maintainanceAlertGroups" in projectCfg) and (len(validMaint) > 0))\
                or (validMaint != projectCfg['maintainanceAlertGroups']):
                projectCfg["maintainanceAlertGroups"] = validMaint
                status |=8
    #
    # Build a list of host PatchGroup Mapping for validation
    #
    hostPGmapping = {}
    configValid = True
            
    #
    # Apply any updates to the mongos's
    #
    if "shardedClusters" in tagDoc:
        for cluster in tagDoc["shardedClusters"]:
            if "mongos" in cluster:
                for node in cluster["mongos"]:
                    if ("deleted" in node) and node["deleted"]:
                        key = node["nodeName"]
                        if key in projectCfg["mongos"]:
                            mongos = projectCfg["mongos"]
                            del mongos[key]
                            logger.logInfo("Node {} removed from the project config.".format(node["hostName"]))
                            status |=  2
                    else:
                        try:
                            currentTags =  projectCfg["mongos"][node["nodeName"]]["tags"]
                        except Exception:
                            currentTags = {}
                            pass
                        if node["tags"] != currentTags:
                            status |=  2   # Config changed
                            projectCfg["mongos"][node["nodeName"]]["tags"] = node["tags"].copy()
                        # Check for the samne host in multiple patch groups
                        if node["hostName"] in hostPGmapping:
                            if hostPGmapping[node["hostName"]] != node["tags"]["patchGroup"]:
                                logger.logMessage("Mismatched Patch Groups for {}.".format(node["hostName"]))
                                configValid = False
                        else:
                            hostPGmapping[node["hostName"]] = node["tags"]["patchGroup"]
                            

    dumpJSON(auto.config,"orig_config.json")
    automation = auto.config
    # Now loop through the replica sets
    added = changed = deleted = 0
    for replSet in tagDoc["replicaSets"]:
        for node in replSet["nodes"]:
            autoLoc = auto.getNodeIdx(replSet["replicaSet"],node["nodeName"])
            currentNode = auto.config["replicaSets"][autoLoc["replSet"]]["members"][autoLoc["node"]]
            if currentNode["host"] == node["nodeName"]:
                if "tags" in currentNode:
                    if "tags" in node:   #new & old so compare
                        if node["tags"] != currentNode["tags"]:
                            logger.logInfo("Changing tags for "+node["nodeName"]+".")
                            automation["replicaSets"][autoLoc["replSet"]]["members"][autoLoc["node"]]["tags"] = node["tags"]
                            changed += 1
                    else: # No new value so remove
                        logger.Info("Removing tags from "+node["nodeName"]+".")
                        del automation["replicaSets"][autoLoc["replSet"]]["members"][autoLoc["node"]]["tags"]
                        deleted += 1
                else: #No tags so set
                    if "tags" in node:      
                        logger.logInfo("Creating tags for "+node["nodeName"]+".")
                        automation["replicaSets"][autoLoc["replSet"]]["members"][autoLoc["node"]]["tags"] = node["tags"]
                        added += 1
                    else:
                        logger.Info("No tags to set for "+node["nodeName"]+".")
                # Check for the same host in multiple patch groups
                if node["hostName"] in hostPGmapping:
                    if hostPGmapping[node["hostName"]] != node["tags"]["patchGroup"]:
                        logger.logMessage("Mismatched Patch Groups for {}.".format(node["hostName"]))
                        configValid = False
                else:
                    hostPGmapping[node["hostName"]] = node["tags"]["patchGroup"]
    if configValid == False:
        logger.logError("One or more hosts in conflicting patch groups. Config load aborting.")
        return None, 0
    logging.info("About to add new tags to "+str(added)+" update "+str(changed)+" and delete tags from "+str(deleted)+" nodes.")
    if (added + changed + deleted) > 0:
        status |= 1
    dumpJSON(automation,"new_config.json")   
    if status > 1:
        db.updateProjConfig(projectCfg)             
    return automation, status

def query_yes_no(question, default="yes"):
    """Ask a yes/no question via raw_input() and return their answer.

    "question" is a string that is presented to the user.
    "default" is the presumed answer if the user just hits <Enter>.
            It must be "yes" (the default), "no" or None (meaning
            an answer is required of the user).

    The "answer" return value is True for "yes" or False for "no".
    """
    valid = {"yes": True, "y": True, "ye": True, "no": False, "n": False}
    if default is None:
        prompt = " [y/n] "
    elif default == "yes":
        prompt = " [Y/n] "
    elif default == "no":
        prompt = " [y/N] "
    else:
        raise ValueError("invalid default answer: '%s'" % default)

    while True:
        sys.stdout.write(question + prompt)
        choice = input().lower()
        if default is not None and choice == "":
            return valid[default]
        elif choice in valid:
            return valid[choice]
        else:
            sys.stdout.write("Please respond with 'yes' or 'no' " "(or 'y' or 'n').\n")

def createToken():
    try:
        f = open(tokenLocation, "w")
        f.close()
    except Exception as e:
        logger.logWarning("Failed to create token as {} ({})".format(tokenLocation,e),logDB=True)
        
    
    
def deleteToken():
    try:
        os.remove(tokenLocation)
    except Exception as e:
        logger.logWarning("Failed to remove token {} ({})".format(tokenLocation,e),logDB=True)
    
def main(argv=None):
    
    global myName,logger
    if argv is None:
        argv = sys.argv
    else:
        sys.argv.extend(argv)

    program_name = os.path.basename(sys.argv[0])
    program_version = "v%s" % __version__
    program_build_id = str('$Id: FIRST$')
    program_long_version_message = '{} {} ({})'.format(program_name, program_version, program_build_id[4:-1])
    program_short_version_message = '{} {}'.format(program_name, program_version)
    program_shortdesc = __import__('__main__').__doc__.split("\n")[1]
    program_license = '''%s

  Created by Pete Williamson on %s.
  Copyright 2020 MongoDB Inc . All rights reserved.

  Licensed under the Apache License 2.0
  http://www.apache.org/licenses/LICENSE-2.0

  Distributed on an "AS IS" basis without warranties
  or conditions of any kind, either express or implied.

USAGE
''' % (program_shortdesc, str(__date__))
    try:
        # Setup argument parser
        parser = ArgumentParser(description=program_license, formatter_class=RawDescriptionHelpFormatter)
        parser.add_argument("-v", "--verbose", dest="verbose", action="count", help="set verbosity level to INFO")
        parser.add_argument("-d", "--debug", dest="debug", action="count", help="set verbosity level to DEBUG")
        parser.add_argument('-V', '--version', action='version', version=program_long_version_message)
        parser.add_argument('--project', dest="project", metavar='project', help="MongoDb Ops Manager Project Name")
        parser.add_argument('--output', '-o', dest="outFile", metavar='tagFile', help="MongoDb Ops Manager Project Name")
        parser.add_argument('--input', '-i', dest="inFile", metavar='inFile', help="Input Tag Definition")
        parser.add_argument('--logfile', '-l', dest="logFile", metavar='logFile', help="Redirect Messages to a file")
        parser.add_argument('--hostname', dest="host", metavar='host', help="Override the hostname")
        parser.add_argument('--messages', dest="msgCount", metavar='msgCount', help="Number of messages to display", type=int)
        parser.add_argument('--CA', dest="cacert", metavar='cacert', help="Specify the path to the CA public cert")
        parser.add_argument('--deployTimeout', dest="deployTO", metavar='deployTimeoutSecs', type=int,  help="Time to wait (seconds) for Deploy to complete")
        parser.add_argument('--comment', dest="comment", metavar='comment text', type=ascii,  help="Comment")
        
        feature_parser = parser.add_mutually_exclusive_group(required=True)
        feature_parser.add_argument('--generateTagFile', dest='generate', action='store_true')
        feature_parser.add_argument('--loadTagFile', dest='load', action='store_true')
        feature_parser.add_argument('--start', dest='start', action='store_true')
        feature_parser.add_argument('--finish', dest='end', action='store_true')
        feature_parser.add_argument('--failsafe', dest='failsafe', action='store_true')
        feature_parser.add_argument('--resetProject', dest="resetPrj", action='store_true')
        feature_parser.add_argument('--status', dest="status", action="store_true")
        feature_parser.add_argument('--revert', dest="revert", metavar="version", nargs=1, type=int)
        feature_parser.add_argument('--extractConfig', dest="extract", metavar=("version","filename"), nargs=2)
        feature_parser.add_argument('--saveConfig', dest="saveConfig", action='store_true')
        feature_parser.add_argument('--configHistory', dest="cfgHist", metavar="days", type=int)
        feature_parser.add_argument('--force', dest="force", metavar= ("mode", "name"), action='store', nargs="+")
        


        # Process arguments
        args = parser.parse_args()

        getVariables()
        
        if not args.host is None:
            myName = args.host
            
        
        if args.debug:
            logLevel = myLogger.DEBUG
        elif args.verbose:
            logLevel = myLogger.INFO
        else:
            logLevel = myLogger.MESSAGE
        
        if args.logFile is None:
            logger = myLogger(myName,severity=logLevel)
        else:
            logger = myLogger(myName,file=args.logFile,severity=logLevel)
        
        logger.logInfo(program_short_version_message)
        
        deployTO = None
        if args.deployTO is not None:
            try:
                deployTO = int(args.deployTO)
            except ValueError:
                logger.logError('Timeout value "{}" should be an integer'.format(args.deployTO) )
                return 1
            

        cafile = True 
        if args.cacert is not None:
            if args.cacert.lower() == "ignore":
                cafile = False
                logger.logWarning("SSL Warnings are being ignored.")
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            elif os.path.exists(args.cacert):
                cafile = args.cacert
            else:
                logger.logError('CA Bundle "{}" does not exist.'.format(args.cacert))
        endpoint = OpsManager(OMServer,OMInternal,deployTO,cafile)
    
        # load or save tags from anywhere
        if (not (args.start or args.end)) and args.project is not None:
            projectInfo = endpoint.doRequest("/groups/byName/"+args.project)
            projectId = projectInfo["id"]
        else:
            hostInfo = endpoint.findHost(myName)
            if hostInfo is None:
                raise cmdError("Host {} not found in OM using API public key {}".format(myName, publicKey))
            projectId = hostInfo["groupId"]
            #clusterId = hostInfo["clusterId"]
            projectInfo = endpoint.doRequest("/groups/"+projectId)
          
        if ("name" in projectInfo):
            projName = projectInfo["name"]
        else:
            projName = None  
            
        # Open database connection
        db = cntrlDB(DBURI,projectId,projName)
        logger.db = db  
        
        prjConfig = db.getProjConfig()
        
        if args.status:

            prjStatus = db.getStatus()
            logger.setWindow(db.controlDoc["patchWindowNumber"])
            if prjStatus == None:
                logger.logMessage("No status for project {}.".format(projName))
                return 1
            if (prjStatus == db.NEW) or (prjStatus == db.COMPLETED):
                logger.logMessage("No maintainance active for project {}, last run was {}.".format(projName,fmtDate(db.controlDoc["startTime"])))
                if (logger.INFO >= logger.sevLevel) or (args.msgCount is not None):
                    if args.msgCount is not None:
                        messages = db.getMessages(logger.maintWindowId,Limit=args.msgCount)
                    else:
                        messages = db.getMessages(logger.maintWindowId)
                    numMessages = len(messages)
                    if numMessages > 0:
                        logger.logMessage("Last {} Messages for Project:".format(numMessages))
                        for msg in messages:
                            logger.logMessage("   {}: {}".format(fmtDate(msg["ts"]),msg["message"]))
            else:
                patchData = db.controlDoc["patchData"]
                logger.logMessage("Project {}, current Status: {}, {} Nodes waiting, {} Nodes Active, {} Node Completed, {} Nodes Skipped." \
                                  .format(projName,prjStatus,len(patchData["validatedHosts"]),len(patchData["activeHosts"]), \
                                  len(patchData["completedHosts"]),len(patchData["skippedHosts"])))
                if (logger.INFO >= logger.sevLevel) and (len(patchData["validatedHosts"] )> 0):
                    logger.logMessage("Hosts waiting:")
                    for host in patchData["validatedHosts"]:
                        logger.logMessage("   {}".format(host))
                if (logger.INFO >= logger.sevLevel) and (len(patchData["activeHosts"]) > 0):
                    logger.logMessage("Hosts active:")
                    for host in patchData["activeHosts"]:
                        logger.logMessage("   {}".format(host))
                if (logger.INFO >= logger.sevLevel) and (len(patchData["completedHosts"]) > 0):
                    logger.logMessage("Hosts finished:")
                    for host in patchData["completedHosts"]:
                        logger.logMessage("   {}".format(host))
                if (logger.INFO >= logger.sevLevel) and (len(patchData["skippedHosts"]) > 0):
                    logger.logMessage("Hosts skipped:")
                    for host in patchData["skippedHosts"]:
                        logger.logMessage("   {}".format(host))
                if (logger.INFO >= logger.sevLevel) or (args.msgCount is not None):
                    if args.msgCount is not None:
                        messages = db.getMessages(logger.maintWindowId,Limit=args.msgCount)
                    else:
                        messages = db.getMessages(logger.maintWindowId)
                    numMessages = len(messages)
                    if numMessages > 0:
                        logger.logMessage("Last {} Messages for window {}:".format(numMessages,logger.maintWindowId))
                        for msg in messages:
                            logger.logMessage("   {}: {}".format(fmtDate(msg["ts"]),msg["message"]))
            return 0
                        
        if args.resetPrj:
            prjStatus = db.getLock()
            if not query_yes_no("Current maintainance status: "+prjStatus+" OK to reset?"):
                logger.logMessage("Reset aborted!")
                return 1   #abort
            db.doReset()
            return 0
            
        alrtCfg = None
        auto = None
        
       
        if prjConfig is not None: 
            alrtCfg = alertConfig(endpoint,projectId,prjConfig["alerts"])
        #
        #use the Project ID to get the automation config
        
        prjStatus = db.getStatus()
        skpCheck = (("deployFailed" in db.controlDoc) and \
                ((prjStatus == db.HALTED) or (prjStatus == db.FORCEMODE)))  # Skip fetching config if shutdown failed to deploy 
        auto = automation(endpoint,projectId,projName,db,skipCheck=skpCheck)
        counter = 0
        timeout = 300 # Five Minutes: we can't start if an OM deploy is in progress
        if isinstance(endpoint.deployTimeout,int):
            timeout = endpoint.deployTimeout    
            
        while auto.config is None:  
            if (counter % 6) == 0:
                logger.logMessage("Ops Manager changes being deployed by another process, waiting for it to finish.")
            sleep(10)
            counter += 1
            auto = automation(endpoint,projectId,projName,db)
            if counter*10 > timeout:
                raise fatalError("Timeout trying to fetch OM config")

        

            


        if args.generate:
            if args.outFile is None:
                raise cmdError("Output file is required for tag template generation")
            try:
                f = open(args.outFile, "w")
            except Exception as e:
                raise cmdError("Error opening "+args.outFile+" "+format(e))
            f.write(json.dumps(genTags(projectInfo,auto,db),indent=4))
            f.close()
            return 0
        elif args.load:
            if args.inFile is None:
                raise cmdError("Input file is required for tag load")
            try:
                f = open(args.inFile, "r")
                tags = f.read()
                tagDoc = json.loads(tags)
                if tagDoc["projectId"] != projectInfo["id"]:
                    raise cmdError("Tag file {} and selected project {} have different Id's.".format(args.inFile,projName))
            except Exception as e:
                raise cmdError("Error reading "+args.inFile+" "+format(e))
                return(2)
            newAutomation, tagStatus = setTags(auto,tagDoc,projectInfo,db,endpoint,alrtCfg)
            if (tagStatus % 2) == 1: #lsb indicates automation change
                status = auto.deployChanges(newAutomation)
                if status == 0:
                    if tagStatus > 1:
                        logger.logMessage("Tags loaded and Project Config modified for {}.".format(projName),logDB=True)
                    else:
                        logger.logMessage("Tags loaded for project {}.".format(projName),logDB=True)
                    return(0)
                else:
                    raise fatalError("Error deploying new tags")
            elif tagStatus > 1:  # projectConfig changed
                logger.logMessage("Project {}, config updated.".format(projName),logDB=True)
            else:
                logger.logMessage("No changes found")
            return 0

        #
        # Tag file options handled we must be starting or stopping
        #
        patchGroup = None 
        if args.start or args.end:
            patchGroup = auto.getPatchGroup(myName)
            if patchGroup is None:
                logger.logWarning("{} is not a member of any patch group",logDB=True)
                return(1)
        

        prjStatus = db.getLock()
        maintWindowId = None
        if "patchWindowNumber" in db.controlDoc:
            maintWindowId = db.controlDoc["patchWindowNumber"]
            logger.setWindow(maintWindowId)
        if args.start:
            while prjStatus == db.INPROGRESS:  # Shutdown underway, we have to wait
                sleep(1)
                prjStatus = db.getLock()
            if (prjStatus == db.NEW) or (prjStatus == db.COMPLETED):
                db.doUnlock(db.INPROGRESS)  # We are first, shutdown the cluster
                maintWindowId = db.controlDoc["patchWindowNumber"]
                logger.setWindow(maintWindowId)
                patchspec = {"patchGroup": patchGroup}
                comment = "Start of Maintenance window {}".format(maintWindowId)
                if auto.startMaintainance(patchspec,db,alrtCfg,comment):
                    createToken()
                    logger.logMessage("Patch group {} halted. Ok to start patching {}.".format(patchGroup,myName),logDB=True)
                else:
                    logger.logMessage("Patch group {} halted. Skipping this node {}.".format(patchGroup,myName),logDB=True)
                    return 1  # Abort patching
            elif prjStatus == db.HALTED: 
                if db.startNode(myName) == 0:
                    createToken()
                    logger.logMessage("Patching OK for {}".format(myName),logDB=True)
                else:
                    return(1)
                return(0)
            elif prjStatus == db.RESTARTING: #too late
                db.doUnlock()
                logger.logError("Too Late! Restart of project {} already in progress".format(projName),logDB=True)
                return(1)
            elif prjStatus == db.FORCEMODE: #too late
                db.doUnlock()
                logger.logError("Force Mode active for project {}.".format(projName),logDB=True)
                return(1)
            else:
                db.doUnlock()
                logger.logError("Unexpected status {} during start maintainance for {}".format(prjStatus,projName),logDB=True)
                return(1)
        elif args.end:
            if (prjStatus == db.NEW) or (prjStatus == db.COMPLETED):
                db.doUnlock() 
                logger.logWarning("No Maintainance active for project {}.".format(projName))
            elif prjStatus == db.HALTED:
                stream = os.popen('ps --no-headers -C "mongodb-mms-automation-agent"')
                output = stream.read()
                if len(output) == 0:  # Nothing found
                    logger.logWarning("Automation agent is not running")
                    if agentCommand != None:
                        os.system(agentCommand)
                deleteToken()
                if db.allDone(myName):
                    db.doUnlock(db.RESTARTING)
                    if auto == None:
                        auto = automation(endpoint,projectId,projName,db,skipCheck=True)
                    auto.endMaintainance(db) 
                else:
                    if not db.endNode(myName):
                        return(1)
                #
                # The first node to finish patching runs the failsafe daemon which waits till the end of the patch window and then
                # restarts the cluster if nodes haven't been patched
                #
                if len(db.controlDoc["patchData"]["completedHosts"]) == 0:  # We are first to finish
                    if os.path.isdir(failsafeLogLocation):
                        tfName = failsafeLogLocation+"/"+projName+"."+datetime.now().strftime("%Y%m%d-%H%M%S")+"failsafe.log"
                    else:
                        tfName = failsafeLogLocation
                    pid = os.spawnl(os.P_NOWAIT, sys.argv[0], sys.argv[0],'--failsafe','--logfile',tfName,'--hostname', myName)
                    logger.logMessage("Patching complete. Failsafe started on {} as PID {} with logs in {}."\
                                      .format(myRealName,str(pid),tfName),logDB=True) 
                else: 
                    logger.logMessage("Patching complete on {}".format(myName),logDB=True) 
                return(0)
            elif prjStatus == db.RESTARTING: #too late
                db.doUnlock()
                logger.logWarning("Restart already in progress for project {}, when Maintainance end run on {}" \
                                  .format(projName,myName),logDB=True)
                return(1)
            elif prjStatus == db.FORCEMODE: 
                db.doUnlock()
                logger.logError("Force Mode active for project {}.".format(projName),logDB=True)
                return(1)
            else:
                logger.logWarning("Unexpected status {} for project {} while finishing maintainance on {}." \
                                  .format(prjStatus,projName,myName),logDB=True)
                return(1)
        elif args.revert is not None:
            config = db.getSavedConfig(args.revert[0])
            if (config != None):
                db.doUnlock(db.RESTARTING)
                nodesFailed = auto.deployChanges(config)
                if len(nodesFailed)== 0:
                    logger.logMessage("Project Configuration reverted",logDB=True)
                    db.endPatch()
                    return 0
                logger.logError("Revert Configuration failed on hosts {}!".format(nodesFailed), logDB=True)
            else:
                db.doUnlock()
                logger.logMessage("Config version {} not found!".format(args.revert))
            return 1
        elif args.extract is not None:
            cfgVer = None
            try:
                cfgVer = int(args.extract[0])
            except Exception as e:
                logger.logError('Argument "{}" could not be converted to Integer.'.format(args.extract[0]))
                return 1
            config = db.getSavedConfig(cfgVer)
            if (config != None):
                db.doUnlock()
                dumpJSON(config, args.extract[1], always=True)
                logger.logMessage("Config version {} extracted to {}.".format(cfgVer,args.extract[1]))
                return 0
            else:
                db.doUnlock()
                logger.logMessage("Config version {} not found!".format(args.extract))
                return 1
        elif args.force is not None:
            if not isinstance(args.comment,basestring):
                logger.logError("A comment is required for --force")
                db.doUnlock()
                return 1    
            if deployTO is None:
                if not query_yes_no("Deploy Timeout not set OK to use 2 minutes?"):
                    logger.logMessage("Use --deployTimeout to specify a timeout")
                    db.doUnlock
                    return 1
            if not((prjStatus == db.NEW) or (prjStatus == db.COMPLETED) or (prjStatus == db.FORCEMODE)):
                logger.logError("--force not permitted while patching is in progress")
                db.doUnlock()
                return 1
            mode = args.force[0]
            objectName = None
            if len(args.force) > 1:
                objectName = args.force[1]
            patchspec = {}
            if mode == "host":
                nodes = auto.getAllNodes(objectName)
                if len(nodes) == 0:
                    logger.logError("No Mongo processes found for {}.".format(objectName))
                    return(1)
                patchspec = {"nodeName": nodes}
            elif mode == "dc":
                patchspec = {"dc": objectName}
            elif mode == "hidden":
                patchspec = {"hidden": "reveal"}
            else:
                logger.logError("Invalid force option {}.".format(mode))
                db.doUnlock()
                return 1
            db.doUnlock(db.INPROGRESS)
            if "hidden" in patchspec:
                auto.exposeHiddenNodes(db,args.comment.strip("'"))
            else:
                if auto.startMaintainance(patchspec,db,alrtCfg,args.comment.strip("'"),isForce=True):
                    logger.logMessage("Sucessfully shutdown {}".format(patchspec))         
                else:
                    logger.logError("Failed to apply patchspec {}".format(patchspec))
        elif args.cfgHist is not None:
            db.doUnlock()
            history = db.getConfigHistory(args.cfgHist)
            logger.logMessage("Saved configurations for project {}:".format(projName))
            logger.logMessage(" Version Save Date        Reason               Comment")
            logger.logMessage(" ------- ---------------- -------------------- ----------------------------------------")
            for entry in history: # ProjectName": 1, "saveDT": 1, "comment":1, "version":1, "saveType"
                logger.logMessage("   {version: ^5} {saveDT:%Y-%m-%d %H:%M} {saveType:20} {comment:40}".format(**entry))
        elif args.saveConfig:
            db.doUnlock()
            if args.comment is None:
                logger.logError("A comment is required when saving the project Configuration")
                return 1
            db.addSavedOMConfig(auto.config, db.REQUEST, args.comment.strip("'"), auto.config["version"])
            return 0
        else:  # failsafe daemon
            db.doUnlock()
            if not (prjStatus == db.HALTED): # Not in Maintainance Mode so nothing to do
                logger.logMessage("Status is "+prjStatus+" so nothing to do")
                return(0)
            timeSoFar = datetime.now(timezone.utc) - db.controlDoc["startTime"]
            secsToSleep = timedelta(hours=4).seconds - timeSoFar.seconds  
            #secsToSleep = timedelta(minutes=2).seconds - timeSoFar.seconds 
            wakeTimeUTC = datetime.now(timezone.utc) + timedelta(seconds=secsToSleep)
            wakeTime = wakeTimeUTC.astimezone(tzlocal.get_localzone())
            if secsToSleep > 0:
                logger.logMessage("Sleeping until {}...".format(wakeTime))
                logger.file.flush()
                sleep(secsToSleep)
            prjStatus = db.getLock()
            if prjStatus == db.HALTED:
                auto.endMaintainance(db)
                logger.logMessage("Failsafe Triggered",logDB=True)
            else:
                logger.logMessage("Nothing to do")
            return(0)    
            

            
             
    except myError as progEx:
        _, exceptionObject, tb  = sys.exc_info()
        stackSummary = traceback.extract_tb(tb,1)
        frameSummary = stackSummary[0]
        logline = '{}: {}. Aborting at line {}.'\
                 .format(progEx.__class__.__name__,progEx,frameSummary.lineno,frameSummary.line)       
        print(logline,file=sys.stderr)
        return(1)
    except pymongo.errors.PyMongoError as of:
        _, exceptionObject, tb  = sys.exc_info()
        if hasattr(of, 'details'):
            errorDetails = of.details
            msg = errorDetails["errmsg"]
            code = errorDetails["code"]
            eType = exceptionObject.__class__.__name__
            stackSummary = traceback.extract_tb(tb,1)
            frameSummary = stackSummary[0]
            logline = '{}: {} ({}) at line {} -  "{}"'.format(eType,msg,code,frameSummary.lineno)     
        elif isinstance(of,pymongo.errors.InvalidURI):
            msg = of._message     
            logline = 'InvalidURI: {} -  "{}"'.format(msg,DBURI)      
        else:
            print(type)
            msg = of._message     
            eType = exceptionObject.__class__.__name__
            stackSummary = traceback.extract_tb(tb,1)
            frameSummary = stackSummary[0]
            logline = '{}: {} at line {} -  "{}"'.format(eType,msg,frameSummary.lineno,frameSummary.line)      
        print(logline,file=sys.stderr)
        return(1)
    except Exception as e:
        raise(e)
        return(2)
    
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)


    
    #clusterInfo = endpoint.doRequest("/groups/"+projectId+"/clusters/"+clusterId)
    #print("Processing cluster "+clusterInfo["clusterName"]+" in project "+projectInfo["name"]+".")
    #parentInfo = None
    #parentId = None
    #for link in clusterInfo["links"]:
    #    if link["rel"] == "http://mms.mongodb.com/parentCluster":
    #        parentInfo = endpoint.followLink(link["href"])
    #        print(parentInfo)
    #        break
    #if parentInfo is not None: 
    #    parentId = parentInfo["id"]
    #    clusterInfo = endpoint.doRequest("/groups/"+projectId+"/clusters?parentClusterId="+parentId)["results"]
    #else:
    #    parentId = clusterId
    #allHosts = endpoint.doRequest("/groups/"+projectId+"/hosts?clusterId="+parentId)["results"]

    #print(allHosts[0])
    
     


if __name__ == "__main__":
    sys.exit(main())    

