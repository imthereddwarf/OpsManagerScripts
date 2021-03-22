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
from pymongo import ReturnDocument
from dns.rdataclass import NONE
from time import sleep
from pickle import TRUE
from platform import node



__all__ = []
__version__ = 0.1
__date__ = '2021-02-26'
__updated__ = '2021-02-26'

# Global variables set from the environment

publicKey = None
privateKey = None
OMRoot = None
OMServer = None
OMInternal = None # Private name of OM Server
DBURI = "mongodb://localhost:27017"


myName = socket.gethostname()


targetProject = 'PYA_DEV'  #Atlas Project name
clusterName = "app_dev"   #Atlas Cluster name

class cntrlDB:
    
    NEW = "New"
    COMPLETED = "Complete"
    INPROGRESS = "Shutdown in progress"
    HALTED = "Patch Group Halted"
    RESTARTING = "Restart in progress" 

    def __init__(self,URI,projectId):
        self.controlColl = None 
        self.projectId = None
        self.controlDoc = None
    
        try:
            myclient = pymongo.MongoClient(URI) #insert credentials into the URI and open a connection
            myDB = myclient.get_default_database()
            self.controlColl = myDB["control"]
            self.projectId = projectId
        except Exception as e:
            print(URI)
            print(type(e))
            print(e)
            print("Error connecting to control database.")
            return(1) 
        
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
            print("Control Record not updated")
            return(1)
        return 0
    
    def startNode(self,hostname):
        if not hostname in self.controlDoc["patchData"]["validatedHosts"]:
            if hostname in self.controlDoc["patchData"]["activeHosts"]:
                print(hostname+" has already been prepared for Maintainance")
                return(0)
            elif hostname in self.controlDoc["patchData"]["completedHosts"]:
                print(hostname+" has already finished Maintainance")
                return(1)
            else:
                print(hostname+" has not been prepared for Maintainance")
                return(1)
        projectControl = {"_id": self.projectId}
        changes = {"$unset": {"lockedBy": 1, "lockedAt": 1},"$pull": {"patchData.validatedHosts": hostname},"$push": {"patchData.activeHosts": hostname},"$currentDate": {"patchData.lastUpdate": True}}
        status = self.controlColl.update_one(projectControl,changes)
        if status.matched_count != 1:
            print("Control Record not updated")
            return(1)
        return 0
    
    def endNode(self,hostname):
        projectControl = {"_id": self.projectId}
        changes = {"$unset": {"lockedBy": 1, "lockedAt": 1},"$pull": {"patchData.activeHosts": hostname}, "$push": {"patchData.completedHosts": hostname}, "$currentDate": {"patchData.lastUpdate": True} }
        status = self.controlColl.update_one(projectControl,changes)
        if status.matched_count != 1:
            print("Control Record not updated")
            return(1)
        return 0    
    
    def startPatch(self,hosts,active,resetDoc,patchGroup):
        now = datetime.now(timezone.utc)
        completed = []
        projectControl = {"_id": self.projectId}
        patchData = {"patchCount": len(hosts), "currentPatchGroup": patchGroup, "validatedHosts": hosts, "activeHosts": active, "completedHosts": completed, "originalSettings": resetDoc, "lastUpdate":  now}
        changes = {"$set": {"patchData": patchData, "status": self.HALTED},"$currentDate": { "startTime": True}}
        status = self.getLock()
        if status != self.INPROGRESS:
            print("Error: control doc not in correct state")
            return(1)
        status = self.controlColl.update_one(projectControl,changes)
        if status.matched_count != 1:
            print("Control Record not updated")
            return(1)
        return 0
    
    def endPatch(self):
        projectControl = {"_id": self.projectId}
        changes = {"$unset": {"lockedBy": 1, "lockedAt": 1, "patchData": 1},"$set": {"status": self.COMPLETED}}
        status = self.getLock()
        if status != self.HALTED:
            print("Error: control doc not in correct state")
            return(1)
        status = self.controlColl.update_one(projectControl,changes)
        if status.matched_count != 1:
            print("Control Record not updated")
            return(1)
        return 0
    
    def allDone(self,hostname):
        if (len(self.controlDoc["patchData"]["activeHosts"]) == 1) and (hostname == self.controlDoc["patchData"]["activeHosts"][0]):
            return True 
        return False
            
    
class OpsManager:
    def __init__(self,omUri,omInternal):
        self.OMRoot = omUri+"/api/public/v1.0"
        self.Server = omUri
        self.internalName = omInternal
        
    def doRequest(self,method):
        response = requests.get(self.OMRoot+method, auth=HTTPDigestAuth(publicKey, privateKey))
        if (response.status_code != 200) :
            resp = response.json()
            print(resp["error"],": ",resp["detail"])
            logging.error(str(response.status_code)+": "+OMRoot+method)
            exit(1)
        logging.debug(self.OMRoot+method)
        return(response.json())

    def doPut(self,method,postData):
        response = requests.put(self.OMRoot+method, data = json.dumps(postData), auth=HTTPDigestAuth(publicKey, privateKey), headers= {"Content-Type": "application/json"})
        if (response.status_code != 200) :
            resp = response.json()
            if ("error" in resp) and ("detail" in resp):
                print(str(resp["error"])+": "+resp["detail"])
                logging.error(self.OMRoot+method+": "+str(resp["error"])+": ",resp["detail"])
            return 1
        logging.debug(self.OMRoot+method)
        return 0
    
    def followLink(self,url):
        if self.internalName is None:
            request_url = url
        else:
            request_url = url.replace(self.internalName,self.Server)
        response = requests.get(request_url, auth=HTTPDigestAuth(publicKey, privateKey))
        if (response.status_code != 200) :
            resp = response.json()
            print(resp["error"],": ",resp["detail"])
            logging.error(str(response.status_code)+": "+url.replace(OMInternal,OMServer))
            exit(1)
        logging.debug(url.replace(OMInternal,OMServer))
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
    
    def __init__(self,OM,projectId,autoconfig=None):
        self.config = autoconfig
        self.projectId = projectId
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
                logging.info(host["hostname"]+" is not at Goal Version.")
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
                print("Last response from "+agent["hostname"]+" at "+str(lastConf)+".")
                missingAgents += 1
        if missingAgents > 0:
            logging.error("All agents must be responsive to sucessfully deploy!")
            return 1
        if not self.allGoal():
            logging.error("Deploy in progress!")
            return 1
        status = self.opsManager.doPut("/groups/"+self.projectId+"/automationConfig",newAutomation)
        if status != 0:
            return status
        while not self.allGoal():
            print("Waiting for Goal state.")
            time.sleep(5)
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
                print("No nodes in Patch Group "+patchGroup+" in replicaset "+replSet["_id"])
            elif len(inCurrentPg) > 1:
                print("Multiple nodes in Patch Group "+patchGroup+" in replicaset "+replSet["_id"]+" skipping")
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
                            active.append(myName)
                        else:
                            stopped.append(shutdownHost)
                    else:
                        print("No Quorum "+replSet["_id"])
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
                                    print(newAutomation["replicaSets"][autoLoc["replSet"]]["members"][autoLoc["node"]]["votes"] )
                                    newAutomation["replicaSets"][autoLoc["replSet"]]["members"][autoLoc["node"]]["votes"] = 1
                                    print(newAutomation["replicaSets"][autoLoc["replSet"]]["members"][autoLoc["node"]]["votes"] )
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
                        print("Inconsistent 4 node replica set "+replSet["_id"])
                else:
                    print("Ignoring "+members+" member replicaset "+replSet["_id"])
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
            return(1)   #Deploy Failed
        db.endPatch()
                
def dumpJSON(dictionary,filename):
    try:
        f = open(filename,"w")
        f.write(json.dumps(dictionary,indent=4))
        f.close
    except Exception as e:
        print(type(e))
        print(e)
        print("Exception dumping dictionary to "+filename)
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
    print(projectInfo)
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
    f = open("auto_in.json","w")
    f.write(json.dumps(auto.config,indent=4))
    f.close
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
                            print(node["tags"])
                            print(currentNode["tags"])
                            logging.info("Changing tags for "+node["nodeName"]+".")
                            automation["replicaSets"][autoLoc["replSet"]]["members"][autoLoc["node"]]["tags"] = node["tags"]
                            changed += 1
                    else: # No new value so remove
                        logging.info("Removing tags from "+node["nodeName"]+".")
                        del automation["replicaSets"][autoLoc["replSet"]]["members"][autoLoc["node"]]["tags"]
                        deleted += 1
                else: #No tags so set
                    if "tags" in node:      
                        logging.info("Creating tags for "+node["nodeName"]+".")
                        automation["replicaSets"][autoLoc["replSet"]]["members"][autoLoc["node"]]["tags"] = node["tags"]
                        added += 1
                    else:
                        logging.info("No tags to set for "+node["nodeName"]+".")
    logging.info("About to add new tags to "+str(added)+" update "+str(changed)+" and delete tags from "+str(deleted)+" nodes.")
                    
    f = open("auto_out.json","w")
    f.write(json.dumps(automation,indent=4))
    f.close
    return automation


                  
    
def main(argv=None):
    
    global myName
    if argv is None:
        argv = sys.argv
    else:
        sys.argv.extend(argv)

    program_name = os.path.basename(sys.argv[0])
    program_version = "v%s" % __version__
    program_build_date = str(__updated__)
    program_version_message = '%%(prog)s %s (%s)' % (program_version, program_build_date)
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
        parser.add_argument("-v", "--verbose", dest="verbose", action="count", help="set verbosity level [default: %(default)s]")
        parser.add_argument('-V', '--version', action='version', version=program_version_message)
        parser.add_argument('--project', dest="project", metavar='project', help="MongoDb Ops Manager Project Name")
        parser.add_argument('--output', '-o', dest="outFile", metavar='tagFile', help="MongoDb Ops Manager Project Name")
        parser.add_argument('--input', '-i', dest="inFile", metavar='tagFile', help="MongoDb Ops Manager Project Name")
        parser.add_argument('--hostname', dest="host", metavar='host', help="Override the hostname")
        
        feature_parser = parser.add_mutually_exclusive_group(required=True)
        feature_parser.add_argument('--generateTagFile', dest='generate', action='store_true')
        feature_parser.add_argument('--loadTagFile', dest='load', action='store_true')
        feature_parser.add_argument('--start', dest='start', action='store_true')
        feature_parser.add_argument('--finish', dest='end', action='store_true')
        feature_parser.add_argument('--failsafe', dest='failsafe', action='store_true')
        


        # Process arguments
        args = parser.parse_args()
        
        if args.verbose:
            logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
        else:
            logging.basicConfig(stream=sys.stdout, level=logging.INFO)

        logging.info(program_version_message)
        getVariables()
        
        if not args.host is None:
            myName = args.host
        endpoint = OpsManager(OMServer,OMInternal)
        # load or save tags from anywhere
        if (args.generate or args.load) and args.project is not None:
            projectInfo = endpoint.doRequest("groups/byName/"+args.project)
            projectId = projectInfo["id"]
        else:
            hostInfo = endpoint.findHost(myName)
            if hostInfo is None:
                print("Host %s not found.",myName)
                exit(1)
            projectId = hostInfo["groupId"]
            clusterId = hostInfo["clusterId"]
            projectInfo = endpoint.doRequest("/groups/"+projectId)
            
        #
        #use the Project ID to get the automation config
        auto = automation(endpoint,projectId)
        if auto is None:  
            print("Unable to fetch OM config")
            return 1
    

        if args.generate:
            if args.outFile is None:
                print("Output file is required.")
                return(2)
            try:
                f = open(args.outFile, "w")
            except Exception as e:
                print("Error opening "+args.outFile+" "+format(e))
                return(2)
            f.write(json.dumps(genTags(projectInfo,auto),indent=4))
            f.close()
            return 0
        elif args.load:
            if args.inFile is None:
                print("Input file is required")
                return(2)
            try:
                f = open(args.inFile, "r")
                tags = f.read()
                tagDoc = json.loads(tags)
                if tagDoc["projectId"] != projectInfo["id"]:
                    print("Tag file and selected project don't match.")
                    return(2)
            except Exception as e:
                print("Error opening "+args.inFile+" "+format(e))
                return(2)
            newAutomation = setTags(auto,tagDoc)
            status = auto.deployChanges(newAutomation)
            if status == 0:
                print("Tags loaded")
                return(0)
            else:
                print("Error loading tags")
                return(status)
        #
        # Tag file options handled we must be starting or stopping
        #
        patchGroup = auto.getPatchGroup(myName)
        if patchGroup is None:
            print("This host is not a member of a patch group")
            return(1)
        
        db = cntrlDB(DBURI,projectId)
        prjStatus = db.getLock()
        if args.start:
            if (prjStatus == db.NEW) or (prjStatus == db.COMPLETED):
                db.doUnlock(db.INPROGRESS)  # We are first, shutdown the cluster
                auto.startMaintainance(patchGroup,db)
            #
            # Now fire off the failsafe process
            #
                pid = os.spawnl(os.P_NOWAIT, sys.argv[0], sys.argv[0],'--failsafe')
                print("Failsafe started as PID "+str(pid))
            elif prjStatus == db.HALTED: 
                if db.startNode(myName) == 0:
                    print("Patching OK")
                else:
                    print("Patching aborted")
                    return(1)
                return(0)
            elif prjStatus == db.RESTARTING: #too late
                db.doUnlock()
                print("Too Late! Restart in progress")
                return(1)
            else:
                db.doUnlock()
                print("Unexpected status "+prjStatus)
                return(1)
        elif args.end:
            if (prjStatus == db.NEW) or (prjStatus == db.COMPLETED):
                db.doUnlock() 
                print("No Maintainance active")
            elif prjStatus == db.HALTED:
                if db.allDone(myName):
                    auto.endMaintainance(db) 
                else:
                    db.endNode(myName)
                print("Patching complete")
                return(0)
            elif prjStatus == db.RESTARTING: #too late
                db.doUnlock()
                print("OOps Restart in progress")
                return(1)
            else:
                print("Unexpected status "+prjStatus)
                return(1)
        else:  # failsafe daemon
            db.doUnlock()
            if not (prjStatus == db.HALTED): # Not in Maintainance Mode so nothing to do
                print("Status is "+prjStatus+" so nothing to do")
                return(0)
            timeSoFar = datetime.now(timezone.utc) - db.controlDoc["startTime"]
            secsToSleep = datetime.timedelta(hours=4).seconds - timeSoFar.seconds  
            if secsToSleep > 0:
                sleep(secsToSleep)
            prjStatus = db.getLock()
            if prjStatus == db.HALTED:
                auto.endMaintainance()
                print ("Failsafe Triggered")
            else:
                print("Nothing to do")
            return(0)    
            

            
                
                 
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
