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
logger = None


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
    RESTARTING = "Restart in progress" 

    def __init__(self,URI,projectId,projectName):
        self.controlColl = None
        self.logColl = None 
        self.configColl = None 
        self.projectId = None
        self.controlDoc = None
        self.projName = None 
        if isinstance(projectName,str):
            self.projName = projectName
    

        myclient = pymongo.MongoClient(URI) #insert credentials into the URI and open a connection
        myDB = myclient.get_default_database()
        self.controlColl = myDB["control"].with_options(codec_options=CodecOptions(tz_aware=True,tzinfo=timezone.utc))
        self.logColl = myDB["messageLog"].with_options(codec_options=CodecOptions(tz_aware=True,tzinfo=timezone.utc))
        self.configColl = myDB["projectConfig"].with_options(codec_options=CodecOptions(tz_aware=True,tzinfo=timezone.utc))
        self.projectId = projectId

        
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
          
        while not( ("lockedBy" in lockDoc) and (lockDoc["lockedBy"] == myName)):
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
        status = self.controlColl.update_one(projectControl,removeLock)
        if status.matched_count != 1:
            raise dbError("doUnlock: Control Record was not updated")
        return 0
    
    def startNode(self,hostname):
        if not hostname in self.controlDoc["patchData"]["validatedHosts"]:
            if hostname in self.controlDoc["patchData"]["activeHosts"]:
                logger.logWarning("{} has already been prepared for Maintainance".format(hostname))
                return(0)
            elif hostname in self.controlDoc["patchData"]["completedHosts"]:
                raise statusError(hostname+" has already finished Maintainance")
            else:
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
            else:
                logger.logMessage("Host {} is not part of this patch group.".format(hostname))
            self.doUnlock()
            return False
                
    def startPatch(self,hosts,active,resetDoc,patchGroup):
        now = datetime.now(timezone.utc)
        completed = []
        projectControl = {"_id": self.projectId}
        patchData = {"patchCount": len(hosts), "currentPatchGroup": patchGroup, "validatedHosts": hosts, "activeHosts": active, "completedHosts": completed, "originalSettings": resetDoc, "lastUpdate":  now}
        changes = {"$set": {"patchData": patchData, "status": self.HALTED},"$currentDate": { "startTime": True}}
        status = self.getLock()
        if status != self.INPROGRESS:
            raise statusError('Control doc in "{}" state, "{}" expected.',status,self.INPROGRESS)
        status = self.controlColl.update_one(projectControl,changes)
        if status.matched_count != 1:
            raise dbError("startPatch: Control Record was not updated")
        return 0
    
    def endPatch(self):
        projectControl = {"_id": self.projectId}
        changes = {"$unset": {"lockedBy": 1, "lockedAt": 1, "patchData": 1},"$set": {"status": self.COMPLETED}}
        status = self.getLock()
        if status != self.HALTED:
            raise statusError('Control doc in "{}" state, "{}" expected.',status,self.HALTED)
        status = self.controlColl.update_one(projectControl,changes)
        if status.matched_count != 1:
            raise dbError("endPatch: Control Record was not updated")
        return 0
    
    def allDone(self,hostname):
        if (len(self.controlDoc["patchData"]["activeHosts"]) == 1) and (len(self.controlDoc["patchData"]["validatedHosts"]) == 0)\
                and (hostname == self.controlDoc["patchData"]["activeHosts"][0]):
            return True 
        return False
    
    def logEvent(self,hostname,severity,message):
        now = datetime.now(timezone.utc)
        eventDoc = {"projectId": self.projectId, "projectName": self.projName, "hostName": hostname, "msgType": severity, "message": message,"ts": now }
        status = self.logColl.insert_one(eventDoc)
        if not status.acknowledged:
            raise dbError('logEvent: Failure to insert event - "{}".'.format(message))
        return 
    
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
            self.db.logEvent(self.myName,sevString,message)
        return
    
    def logWarning(self,message,logDB=False):
        if self.WARNING >= self.sevLevel:
            print("WARNING: {}".format(message),file=self.file)
        if logDB:
            sevString = self.errorText[self.WARNING]
            self.db.logEvent(self.myName,sevString,message)
        return

    def logError(self,message,logDB=False):
        if self.ERROR >= self.sevLevel:
            print("ERROR: {}".format(message),file=self.file)
        if logDB:
            sevString = self.errorText[self.ERROR]
            self.db.logEvent(self.myName,sevString,message)
        return
    
    def logFatal(self,message,logDB=False):
        if self.MESSAGE >= self.sevLevel:
            print("FATAL: {}".format(message),file=self.file)
        if logDB:
            sevString = self.errorText[self.FATAL]
            self.db.logEvent(self.myName,sevString,message)
        return
    
    def logProgress(self):
        self.file.write(".")
        self.file.flush()
    
    def logComplete(self):
        print(" ",file=self.file)
        
class OpsManager:
    def __init__(self,omUri,omInternal):
        self.OMRoot = omUri+"/api/public/v1.0"
        self.Server = omUri
        self.internalName = omInternal
        
    def doRequest(self,method):
        response = requests.get(self.OMRoot+method, auth=HTTPDigestAuth(publicKey, privateKey))
        if (response.status_code != 200) :
            resp = response.json()
            if ("error" in resp) and ("detail" in resp):
                raise fatalError(self.OMRoot+method+": "+str(resp["error"])+": ",resp["detail"])
            else:
                raise fatalError("Error OM request - "+str(response.status_code)+": "+OMRoot+method)

        logger.logDebug(self.OMRoot+method)
        return(response.json())

    def doPut(self,method,postData):
        response = requests.put(self.OMRoot+method, data = json.dumps(postData), auth=HTTPDigestAuth(publicKey, privateKey), headers= {"Content-Type": "application/json"})
        if (response.status_code != 200) :
            resp = response.json()
            if ("error" in resp) and ("detail" in resp):
                raise fatalError(self.OMRoot+method+": "+str(resp["error"])+": ",resp["detail"])
            else:
                raise fatalError("Error OM request - "+str(response.status_code)+": "+OMRoot+method)
        logger.logDebug(self.OMRoot+method)
        return 0
    
    def followLink(self,url):
        if self.internalName is None:
            request_url = url
        else:
            request_url = url.replace(self.internalName,self.Server)
        response = requests.get(request_url, auth=HTTPDigestAuth(publicKey, privateKey))
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
        
class automation:
    
    def __init__(self,OM,projectId,projectName,autoconfig=None):
        self.config = autoconfig
        self.projectId = projectId
        self.projName = projectName
        self.NodeHostMapping = {}
        self.HostNodeMapping = {}
        self.configIDX = {}
        self.nodeTags = {}
        self.opsManager = OM
        
        # First check we are at goal state
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
            self.NodeHostMapping[process["name"]] = {"hostName": process["hostname"], "isStopped": process["disabled"], "index": pIdx}
            if process["processType"] == "mongos":
                container = process["cluster"]+":mongos"
            else:
                container = process ["args2_6"]["replication"]["replSetName"]
            self.HostNodeMapping[process["hostname"]] = {"replicaSet": container,"name": process["name"], "isStopped": process["disabled"], "index": pIdx}
            pIdx += 1
            

        return
    
    def configCurrent(self):
        response = self.opsManager.doRequest("/groups/"+self.projectId+"/automationStatus")
        if self.currentVersion == response["goalVersion"]:  # Nothing has changed
            return True 
        else:
            return False 
        
    def getPatchGroup(self,hostName):
        node = self.getNodeName(hostName)
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
    
    def getHostname(self,nodename):
        return self.NodeHostMapping[nodename]["hostName"]
    
    def isNodeStopped(self,nodename):
        return self.NodeHostMapping[nodename]["isStopped"]
    
    def getNodeName(self,hostname):
        return self.HostNodeMapping[hostname]["name"]
    
    def getProcesses(self):
        return self.config["processes"]
    
    def getReplicaSets(self):
        return self.config["replicaSets"]
    
    def gotMajority(self,totalVotes,numArbiters,votesLost,numHidden):
        newVotes = totalVotes + numHidden - votesLost
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
        notAtGoal = 0
        for host in response["processes"]:
            if host["lastGoalVersionAchieved"] != targetVersion:
                logger.logDebug(host["hostname"]+" is not at Goal Version.")
                notAtGoal += 1
        if notAtGoal > 0:
            return False
        return True
    
    def deployChanges(self,newAutomation):
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
        if not self.allGoal():
            raise fatalError("Another deploy already in progress!")
        status = self.opsManager.doPut("/groups/"+self.projectId+"/automationConfig",newAutomation)
        if status != 0:
            return status
        logger.logMessage("Deploying changes to {}.".format(self.projName))
        while not self.allGoal():
            logger.logInfo("Waiting for Goal state.")
            logger.logProgress()
            time.sleep(5)
        logger.logComplete()
        return 0
    
    def startMaintainance(self,patchGroup,db):
        resetDoc = {}
        newAutomation = self.config
        dumpJSON(newAutomation,"before.json")
        stopped = []
        activeNodes = []
        for replSet in self.config["replicaSets"]:
            members = active = voting = hidden = totalVotes = arbiter = pgVotes = 0
            inCurrentPg = []
            hiddenNodes = []
            for member in replSet["members"]:
                if member["arbiterOnly"]:
                    arbiter += 1
                if member["hidden"]:
                    hidden += 1
                    hiddenNodes.append(self.getHostname(member["host"]))
                if member["votes"] > 0:
                    voting += 1
                    totalVotes += member["votes"]
                if not self.isNodeStopped(member["host"]):
                    active += 1
                if ("tags" in member) and ("patchGroup" in member["tags"]) and (member["tags"]["patchGroup"] == patchGroup):
                    inCurrentPg.append(self.getHostname(member["host"]))
                    pgVotes += member["votes"]
                members += 1

            if len(inCurrentPg) == 0:
                logger.logWarning("No nodes in Patch Group "+patchGroup+" for replicaset "+replSet["_id"],logDB=True)
            elif len(inCurrentPg) > 1:
                logger.logWarning("Multiple nodes in Patch Group "+patchGroup+" in replicaset "+replSet["_id"]+" skipping",logDB=True)
            else:
                shutdownHost = inCurrentPg[0]
            #
            # 3 or 5 Node Cluster - all must be up and voting, shutdown one node
            #
                if members == 3 or members == 5:
                    if (active == members) and self.gotMajority(totalVotes,arbiter,pgVotes,0):
                        newAutomation["processes"][self.getHostProcIdx(shutdownHost)]["disabled"] = True
                        resetDoc[shutdownHost] = {"disabled": False}
                        if shutdownHost == myName:
                            activeNodes.append(myName)
                        else:
                            stopped.append(shutdownHost)
                    else:
                        logger.logWarning("No Quorum for replica set "+replSet["_id"]+" skipping.",logDB=True)
            #
            # 4 node cluster, should be one Hidden and 3 votes
            #
                elif members == 4:
                    if (active == 4) and self.gotMajority(totalVotes,arbiter,pgVotes,hidden):
                        if not shutdownHost in hiddenNodes:  # if were not shutting down the hidden node we need to activate one
                            for host in hiddenNodes:
                                hostCfg = {}
                                autoLoc = self.getHostIdx(host)
                                hostCfg["votes"] = newAutomation["replicaSets"][autoLoc["replSet"]]["members"][autoLoc["node"]]["votes"]
                                if hostCfg["votes"] == 0:
                                    newAutomation["replicaSets"][autoLoc["replSet"]]["members"][autoLoc["node"]]["votes"] = 1
                                hostCfg["hidden"] = newAutomation["replicaSets"][autoLoc["replSet"]]["members"][autoLoc["node"]]["hidden"]
                                if hostCfg["hidden"] == True:
                                    newAutomation["replicaSets"][autoLoc["replSet"]]["members"][autoLoc["node"]]["hidden"] = False
                                resetDoc[host] = hostCfg
                        hostCfg = {"disabled": False}
                        autoLoc = self.getHostIdx(shutdownHost)
                        newAutomation["processes"][self.getHostProcIdx(shutdownHost)]["disabled"] = True
                        hostCfg["votes"] = newAutomation["replicaSets"][autoLoc["replSet"]]["members"][autoLoc["node"]]["votes"]
                        if hostCfg["votes"] == 1:
                            newAutomation["replicaSets"][autoLoc["replSet"]]["members"][autoLoc["node"]]["votes"] = 0
                        hostCfg["priority"] = newAutomation["replicaSets"][autoLoc["replSet"]]["members"][autoLoc["node"]]["priority"]
                        if hostCfg["priority"] > 0:
                            newAutomation["replicaSets"][autoLoc["replSet"]]["members"][autoLoc["node"]]["priority"] = 0
                        resetDoc[shutdownHost] = hostCfg
                        if shutdownHost == myName:
                            activeNodes.append(myName)
                        else:
                            stopped.append(shutdownHost)
                    else:
                        logger.logWarning("Inconsistent 4 node replica set "+replSet["_id"]+" skipping.",logDB=True)
                else:
                    logger.logWarning("Ignoring "+members+" member replicaset "+replSet["_id"],logDB=True)
        dumpJSON(newAutomation,"after.json")
        if self.deployChanges(newAutomation) > 0:
            return(1)   #Deploy Failed
        db.startPatch(stopped,activeNodes,resetDoc,patchGroup)
    
    def endMaintainance(self,db):
        newAutomation = self.config
        resetDoc = db.controlDoc["patchData"]["originalSettings"]
        for host in resetDoc:
            resetSpec = resetDoc[host]
            autoLoc = self.getHostIdx(host)
            if "votes" in resetSpec:
                newAutomation["replicaSets"][autoLoc["replSet"]]["members"][autoLoc["node"]]["votes"] = resetSpec["votes"]
            if "hidden" in resetSpec:
                newAutomation["replicaSets"][autoLoc["replSet"]]["members"][autoLoc["node"]]["hidden"] = resetSpec["hidden"]
            if "priority" in resetSpec:
                newAutomation["replicaSets"][autoLoc["replSet"]]["members"][autoLoc["node"]]["priority"] = resetSpec["priority"]
            if "disabled" in resetSpec:
                newAutomation["processes"][self.getHostProcIdx(host)]["disabled"] = resetSpec["disabled"]
        if self.deployChanges(newAutomation) > 0:
            raise fatalError("Deploy to end Maintainance failed")   #Deploy Failed
        db.endPatch()
                
def dumpJSON(dictionary,filename):
    if logger.sevLevel > myLogger.DEBUG:
        return
    try:
        f = open(filename,"w")
        f.write(json.dumps(dictionary,indent=4))
        logger.logDebug("Dictionary dumped to {}.".format(filename))
    except Exception as e:
        logger.logDebug("Error attempting to create debug dump to {} ({}).".format(filename,format(e)))
        pass
    finally:
        f.close
    return
    
            
    
    
def getVariables():
    global publicKey, privateKey, OMServer, myName, OMInternal, OMRoot, DBURI
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
        

 
 # TODO - Create WF specific mapping                   
def dcMapping(hostname):
    host2dc = {"16": "cc","17": "cc","21": "cc","22": "ss","24": "sv","25": "sv","26": "sv","27": "ox","28": "ox","29": "ox","30": "az","31": "sl"}
    dcpart = hostname[10:12]
    if dcpart in host2dc:
        return(host2dc[dcpart])
    return("uk")

def genTags(projectInfo,auto):
    projectDoc = {"projectName": projectInfo["name"], "orgId": projectInfo["orgId"], "projectId": projectInfo["id"]}
    allNodes = []
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
                        tags["patchGroup"] = i
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
        allNodes.append(setTags)
    projectDoc["replicaSets"] = allNodes
    return(projectDoc)
                  
def setTags(auto,tagDoc):
    # First build a lookup table to the automation dictionary
    dumpJSON(auto.config,"orig_config.json")
    automation = auto.config
    # Now loop through the input file 
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
                        logger.Info("Creating tags for "+node["nodeName"]+".")
                        automation["replicaSets"][autoLoc["replSet"]]["members"][autoLoc["node"]]["tags"] = node["tags"]
                        added += 1
                    else:
                        logger.Info("No tags to set for "+node["nodeName"]+".")
    logging.info("About to add new tags to "+str(added)+" update "+str(changed)+" and delete tags from "+str(deleted)+" nodes.")
    dumpJSON(automation,"new_config.json")                
    return automation


                  
    
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
        
        feature_parser = parser.add_mutually_exclusive_group(required=True)
        feature_parser.add_argument('--generateTagFile', dest='generate', action='store_true')
        feature_parser.add_argument('--loadTagFile', dest='load', action='store_true')
        feature_parser.add_argument('--start', dest='start', action='store_true')
        feature_parser.add_argument('--finish', dest='end', action='store_true')
        feature_parser.add_argument('--failsafe', dest='failsafe', action='store_true')
        


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
        
        endpoint = OpsManager(OMServer,OMInternal)
        # load or save tags from anywhere
        if (args.generate or args.load) and args.project is not None:
            projectInfo = endpoint.doRequest("groups/byName/"+args.project)
            projectId = projectInfo["id"]
        else:
            hostInfo = endpoint.findHost(myName)
            if hostInfo is None:
                raise cmdError("Host {} not found in OM using API public key {}".format(myName, publicKey))
            projectId = hostInfo["groupId"]
            clusterId = hostInfo["clusterId"]
            projectInfo = endpoint.doRequest("/groups/"+projectId)
          
        if ("name" in projectInfo):
            projName = projectInfo["name"]
        else:
            projName = None    
        #
        #use the Project ID to get the automation config
        auto = automation(endpoint,projectId,projName)
        if auto is None:  
            raise fatalError("Unable to fetch OM config")
        

            
        db = cntrlDB(DBURI,projectId,projName)
        logger.db = db 

        if args.generate:
            if args.outFile is None:
                raise cmdError("Output file is required for tag template generation")
            try:
                f = open(args.outFile, "w")
            except Exception as e:
                raise cmdError("Error opening "+args.outFile+" "+format(e))
            f.write(json.dumps(genTags(projectInfo,auto),indent=4))
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
            newAutomation = setTags(auto,tagDoc)
            status = auto.deployChanges(newAutomation)
            if status == 0:
                logger.logMessage("Tags loaded",logDB=True)
                return(0)
            else:
                raise fatalError("Error deploying new tags")

        #
        # Tag file options handled we must be starting or stopping
        #
        patchGroup = auto.getPatchGroup(myName)
        if patchGroup is None:
            logger.logWarning("{} is not a member of any patch group",logDB=True)
            return(1)
        

        prjStatus = db.getLock()
        if args.start:
            if (prjStatus == db.NEW) or (prjStatus == db.COMPLETED):
                db.doUnlock(db.INPROGRESS)  # We are first, shutdown the cluster
                auto.startMaintainance(patchGroup,db)
                logger.logMessage("Patch group {} halted. Ok to start patching {}.".format(patchGroup,myName),logDB=True)
            elif prjStatus == db.HALTED: 
                if db.startNode(myName) == 0:
                    logger.logMessage("Patching OK for {}".format(myName),logDB=True)
                else:
                    logger.logError("Patching aborted")
                    return(1)
                return(0)
            elif prjStatus == db.RESTARTING: #too late
                db.doUnlock()
                logger.logError("Too Late! Restart of project {} already in progress".format(projName),logDB=True)
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
                if db.allDone(myName):
                    auto.endMaintainance(db) 
                else:
                    if not db.endNode(myName):
                        return(1)
                #
                # The first node to finish patching runs the failsafe daemon which waits till the end of the patch window and then
                # restarts the cluster if nodes haven't been patched
                #
                if len(db.controlDoc["patchData"]["completedHosts"]) == 0:  # We are first to finish
                    tf = tempfile.NamedTemporaryFile("w",delete=False,suffix=".failsafe.log")
                    tfName = tf.name
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
            else:
                logger.logWarning("Unexpected status {} for project {} while finishing maintainance on {}." \
                                  .format(prjStatus,projName,myName),logDB=True)
                return(1)
        else:  # failsafe daemon
            db.doUnlock()
            if not (prjStatus == db.HALTED): # Not in Maintainance Mode so nothing to do
                logger.logMessage("Status is "+prjStatus+" so nothing to do")
                return(0)
            timeSoFar = datetime.now(timezone.utc) - db.controlDoc["startTime"]
            secsToSleep = timedelta(hours=4).seconds - timeSoFar.seconds  
            wakeTimeUTC = datetime.now(timezone.utc) + timedelta(seconds=secsToSleep)
            wakeTime = wakeTimeUTC.astimezone(tzlocal.get_localzone())
            if secsToSleep > 0:
                logger.logMessage("Sleeping until {}...".format(wakeTime))
                logger.file.flush()
                sleep(secsToSleep)
            prjStatus = db.getLock()
            if prjStatus == db.HALTED:
                auto.endMaintainance()
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
            logline = '{}: {} ({}) at line {} -  "{}"'.format(eType,msg,code,frameSummary.linenoå)     
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
