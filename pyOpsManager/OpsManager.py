'''
Created on Feb 1, 2022

@author: peter.williamson
'''

import requests
import json
import logging
import time
from datetime import datetime, timedelta, timezone
import dateutil.parser

from requests.auth import HTTPDigestAuth

class OMError(Exception):
    pass

class fatalError(OMError):
    # We can't proceed due to an error
    pass



class OpsManager:
    '''
    classdocs
    '''
    alertTypeNames = ["HOST", "REPLICA_SET", "CLUSTER", "AGENT", "BACKUP"]
    alertEventNames = {
"AUTOMATION_AGENT_DOWN": "Automation is down","AUTOMATION_AGENT_UP": "Automation is up",
"BACKUP_AGENT_CONF_CALL_FAILURE": "Backup has too many conf call failures","BACKUP_AGENT_DOWN": "Backup is down","BACKUP_AGENT_UP": "Backup is up","BACKUP_AGENT_VERSION_BEHIND": "Backup does not have the latest version","BACKUP_AGENT_VERSION_CURRENT": "Backup has the latest version",
"MONITORING_AGENT_DOWN": "Monitoring is down","MONITORING_AGENT_UP": "Monitoring is up","MONITORING_AGENT_VERSION_BEHIND": "Monitoring does not have the latest version","MONITORING_AGENT_VERSION_CURRENT": "Monitoring has the latest version",
"NEW_AGENT": "New agent","AUTOMATION_CONFIG_PUBLISHED_AUDIT": "Deployment configuration published","BAD_CLUSTERSHOTS": "Backup has possibly inconsistent cluster snapshots","CLUSTER_BLACKLIST_UPDATED_AUDIT": "Excluded namespaces were modified for cluster",
"CLUSTER_CHECKKPOINT_UPDATED_AUDIT": "Checkpoint interval updated for cluster","CLUSTER_CREDENTIAL_UPDATED_AUDIT": "Backup authentication credentials updated for cluster","CLUSTER_SNAPSHOT_SCHEDULE_UPDATED_AUDIT": "Snapshot schedule updated for cluster",
"CLUSTER_STATE_CHANGED_AUDIT": "Cluster backup state is now","CLUSTER_STORAGE_ENGINE_UPDATED_AUDIT": "Cluster storage engine has been updated","CLUSTERSHOT_DELETED_AUDIT": "Cluster snapshot has been deleted","CLUSTERSHOT_EXPIRY_UPDATED_AUDIT": "Clustershot expiry has been updated.",
"CONSISTENT_BACKUP_CONFIGURATION": "Backup configuration is consistent","GOOD_CLUSTERSHOT": "Backup has a good clustershot","INCONSISTENT_BACKUP_CONFIGURATION": "Inconsistent backup configuration has been detected","INITIAL_SYNC_FINISHED_AUDIT": "Backup initial sync finished",
"INITIAL_SYNC_STARTED_AUDIT": "Backup initial sync started","OPLOG_BEHIND": "Backup oplog is behind","OPLOG_CURRENT": "Backup oplog is current","RESTORE_REQUESTED_AUDIT": "A restore has been requested","RESYNC_PERFORMED": "Backup has been resynced","RESYNC_REQUIRED": "Backup requires a resync",
"RS_BLACKLIST_UPDATED_AUDIT": "Excluded namespaces were modified for replica set","RS_CREDENTIAL_UPDATED_AUDIT": "Backup authentication credentials updated for replica set","RS_ROTATE_MASTER_KEY_AUDIT": "A master key rotation has been requested for a replica set.",
"RS_SNAPSHOT_SCHEDULE_UPDATED_AUDIT": "Snapshot schedule updated for replica set","RS_STATE_CHANGED_AUDIT": "Replica set backup state is now","RS_STORAGE_ENGINE_UPDATED_AUDIT": "Replica set storage engine has been updated","SNAPSHOT_DELETED_AUDIT": "Snapshot has been deleted",
"SNAPSHOT_EXPIRY_UPDATED_AUDIT": "Snapshot expiry has been updated.","SYNC_PENDING_AUDIT": "Backup sync is pending","SYNC_REQUIRED_AUDIT": "Backup sync has been initiated","BI_CONNECTOR_DOWN": "BI Connector is down","BI_CONNECTOR_UP": "BI Connector is up",
"CLUSTER_MONGOS_IS_MISSING": "Cluster is missing an active mongos","CLUSTER_MONGOS_IS_PRESENT": "Cluster has an active mongos","SHARD_ADDED": "Shard added","SHARD_REMOVED": "Shard removed","DATA_EXPLORER": "User performed a Data Explorer read-only operation",
"DATA_EXPLORER_CRUD": "User performed a Data Explorer CRUD operation, which modifies data","ADD_HOST_AUDIT": "Host added","ADD_HOST_TO_REPLICA_SET_AUDIT": "Host added to replica set","ATTEMPT_KILLOP_AUDIT": "Attempted to kill operation",
"ATTEMPT_KILLSESSION_AUDIT": "Attempted to kill session","DB_PROFILER_DISABLE_AUDIT": "Database profiling disabled","DB_PROFILER_ENABLE_AUDIT": "Database profiling enabled","DELETE_HOST_AUDIT": "Host removed","DISABLE_HOST_AUDIT": "Host disabled",
"HIDE_AND_DISABLE_HOST_AUDIT": "Host disabled and hidden","HIDE_HOST_AUDIT": "Host hidden","HOST_DOWN": "Host is down","HOST_DOWNGRADED": "Host has been downgraded","HOST_IP_CHANGED_AUDIT": "Host IP address changed","HOST_NOW_PRIMARY": "Host is now primary",
"HOST_NOW_SECONDARY": "Host is now secondary","HOST_NOW_STANDALONE": "Host is now a standalone","HOST_RECOVERED": "Host has recovered","HOST_RECOVERING": "Host is recovering","HOST_RESTARTED": "Host has restarted","HOST_ROLLBACK": "Host experienced a rollback",
"HOST_SSL_CERTIFICATE_CURRENT": "Host’s SSL certificate is current","HOST_SSL_CERTIFICATE_STALE": "Host’s SSL certificate will expire within 30 days","HOST_UP": "Host is up","HOST_UPGRADED": "Host has been upgraded","INSIDE_METRIC_THRESHOLD": "Inside metric threshold","NEW_HOST": "Host is new",
"OUTSIDE_METRIC_THRESHOLD": "Outside metric threshold","PAUSE_HOST_AUDIT": "Host paused","REMOVE_HOST_FROM_REPLICA_SET_AUDIT": "Host removed from replica set","RESUME_HOST_AUDIT": "Host resumed","UNDELETE_HOST_AUDIT": "Host undeleted","VERSION_BEHIND": "Host does not have the latest version",
"VERSION_CHANGED": "Host version changed","VERSION_CURRENT": "Host has the latest version","ALL_ORG_USERS_HAVE_MFA": "Organization users have two-factor authentication enabled","ORG_API_KEY_ADDED": "API key has been added","ORG_API_KEY_DELETED": "API key has been deleted",
"ORG_EMPLOYEE_ACCESS_RESTRICTED": "MongoDB Production Support Employees restricted from accessing Atlas backend infrastructure for any Atlas cluster in this organization (You may grant a 24 hour bypass to the access restriction at the Atlas cluster level)",
"ORG_EMPLOYEE_ACCESS_UNRESTRICTED": "MongoDB Production Support Employees unrestricted from accessing Atlas backend infrastructure for any Atlas cluster in this organization","ORG_PUBLIC_API_WHITELIST_NOT_REQUIRED": "IP Whitelist for Public API Not Required",
"ORG_PUBLIC_API_WHITELIST_REQUIRED": "Require IP Whitelist for Public API Enabled","ORG_RENAMED": "Organization has been renamed","ORG_TWO_FACTOR_AUTH_OPTIONAL": "Two-factor Authentication Optional","ORG_TWO_FACTOR_AUTH_REQUIRED": "Two-factor Authentication Required",
"ORG_USERS_WITHOUT_MFA": "Organization users do not have two-factor authentication enabled","ALL_USERS_HAVE_MULTIFACTOR_AUTH": "Users have two-factor authentication enabled","USERS_WITHOUT_MULTIFACTOR_AUTH": "Users do not have two-factor authentication enabled",
"CONFIGURATION_CHANGED": "Replica set has an updated configuration","ENOUGH_HEALTHY_MEMBERS": "Replica set has enough healthy members","MEMBER_ADDED": "Replica set member added","MEMBER_REMOVED": "Replica set member removed","MULTIPLE_PRIMARIES": "Replica set elected multiple primaries",
"NO_PRIMARY": "Replica set has no primary","ONE_PRIMARY": "Replica set elected one primary","PRIMARY_ELECTED": "Replica set elected a new primary","TOO_FEW_HEALTHY_MEMBERS": "Replica set has too few healthy members","TOO_MANY_ELECTIONS": "Replica set has too many election events",
"TOO_MANY_UNHEALTHY_MEMBERS": "Replica set has too many unhealthy members","TEAM_ADDED_TO_GROUP": "Team added to project","TEAM_CREATED": "Team created","TEAM_DELETED": "Team deleted","TEAM_NAME_CHANGED": "Team name changed","TEAM_REMOVED_FROM_GROUP": "Team removed from project",
"TEAM_ROLES_MODIFIED": "Team roles modified in project","TEAM_UPDATED": "Team updated","USER_ADDED_TO_TEAM": "User added to team","INVITED_TO_GROUP": "User was invited to project","INVITED_TO_ORG": "User was invited to organization",
"JOIN_GROUP_REQUEST_APPROVED_AUDIT": "Request to join project was approved","JOIN_GROUP_REQUEST_DENIED_AUDIT": "Request to join project was denied","JOINED_GROUP": "User joined the project","JOINED_ORG": "User joined the organization","JOINED_TEAM": "User joined the team",
"REMOVED_FROM_GROUP": "User left the project","REMOVED_FROM_ORG": "User left the organization","REMOVED_FROM_TEAM": "User left the team","REQUESTED_TO_JOIN_GROUP": "User requested to join project","USER_ROLES_CHANGED_AUDIT": "User had their role changed"}


    def __init__(self,omUri,publicKey,privateKey,omInternalURI=None,timeout=None,cafile=True):
        self.OMRoot = omUri+"/api/public/v1.0"
        self.Server = omUri
        self.internalName = omInternalURI
        self.deployTimeout = timeout
        self.digest = HTTPDigestAuth(publicKey, privateKey)
        httpSession = requests.Session()
        httpSession.verify = cafile
        self.OMSession = httpSession

        
    def doRequest(self,method,notFoundOK=False):
        response = self.OMSession.get(self.OMRoot+method, auth=self.digest)
        if (response.status_code != 200) :
            if (response.status_code == 404) and notFoundOK:
                return
            resp = response.json()
            if ("error" in resp) and ("detail" in resp):
                raise fatalError(self.OMRoot+method+": "+str(resp["error"])+": ",resp["detail"])
            else:
                raise fatalError("Error OM request - "+str(response.status_code)+": "+self.OMRoot+method)

        logging.debug(self.OMRoot+method)
        return(response.json())

    def doPut(self,method,postData):
        if isinstance(postData,dict):
            response = self.OMSession.put(self.OMRoot+method, data = json.dumps(postData), auth=self.digest, headers= {"Content-Type": "application/json"})
        else:
            response = self.OMSession.put(self.OMRoot+method, data = postData, auth=self.digest, headers= {"Content-Type": "application/json"})
        if (response.status_code != 200) :
            resp = response.json()
            if ("error" in resp) and ("detail" in resp):
                raise fatalError(self.OMRoot+method+": "+str(resp["error"])+": ",resp["detail"])
            else:
                raise fatalError("Error OM request - "+str(response.status_code)+": "+self.OMRoot+method)
        logging.debug(self.OMRoot+method)
        return 0
    
    def doPost(self,method,postData):
        if isinstance(postData,dict):
            response = self.OMSession.post(self.OMRoot+method, data = json.dumps(postData), auth=self.digest, headers= {"Content-Type": "application/json"})
        else:
            response = self.OMSession.post(self.OMRoot+method, data = postData, auth=self.digest, headers= {"Content-Type": "application/json"})
        if (response.status_code >= 300) :
            resp = response.json()
            if ("error" in resp) and ("detail" in resp):
                raise fatalError(self.OMRoot+method+": "+str(resp["error"])+": ",resp["detail"])
            else:
                raise fatalError("Error OM request - "+str(response.status_code)+": "+self.OMRoot+method)
        logging.debug(self.OMRoot+method)
        return response.json()
    
    def doPatch(self,method,postData):
        response = self.OMSession.patch(self.OMRoot+method, data = json.dumps(postData), auth=self.digest, headers= {"Content-Type": "application/json"})
        if (response.status_code == 404):
            return None
        elif (response.status_code != 200) :
            resp = response.json()
            if ("error" in resp) and ("detail" in resp):
                raise fatalError(self.OMRoot+method+": "+str(resp["error"])+": ",resp["detail"])
            else:
                raise fatalError("Error OM request - "+str(response.status_code)+": "+self.OMRoot+method)
        logging.debug(self.OMRoot+method)
        return response.json()
    
    def doDelete(self,method):
        response = self.OMSession.delete(self.OMRoot+method, auth=self.digest, headers= {"Content-Type": "application/json"})
        if (response.status_code == 404):
            return None
        elif (response.status_code != 204) :
            resp = response.json()
            if ("error" in resp) and ("detail" in resp):
                raise fatalError(self.OMRoot+method+": "+str(resp["error"])+": ",resp["detail"])
            else:
                raise fatalError("Error OM request - "+str(response.status_code)+": "+self.OMRoot+method)
        logging.debug(self.OMRoot+method)
        return 0
    
    def followLink(self,url):
        if self.internalName is None:
            request_url = url
        else:
            request_url = url.replace(self.internalName,self.Server)
        response = self.OMSession.get(request_url, auth=self.digest)
        if (response.status_code != 200) :
            resp = response.json()
            if ("error" in resp) and ("detail" in resp):
                raise fatalError(request_url+": "+str(resp["error"])+": ",resp["detail"])
            else:
                raise fatalError("Error OM request - "+str(response.status_code)+": "+request_url)
        logging.debug(request_url)
        return(response.json())
    
    def getNextPage(self,previous):
        if ("links" in previous):
            for link in previous["links"]:
                if ("rel" in link) and (link["rel"] == "next"):
                    return self.followLink(link["href"])
        return None
            
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
        
    def deployAutomation(self,projectId,newAutomation,checkAgents=False,override=False):
        if checkAgents:
            agents = self.doRequest("/groups/"+projectId+"/agents/AUTOMATION")
            now = datetime.now(timezone.utc)
            delayedping = now - timedelta(minutes =2)
            missingAgents = 0
            for agent in agents["results"]:
                lastConf = dateutil.parser.parse(agent["lastConf"])
                if lastConf < delayedping:
                    logging.info("Last response from "+agent["hostname"]+" at "+str(lastConf)+".")
                    missingAgents += 1
            if missingAgents > 0:
                raise fatalError("All agents must be responsive to sucessfully deploy! {} of {} agents not responding." \
                                 .format(missingAgents,len(agents["results"])))
        if (not override) and (not self.allGoal()): #override True -> Start new deploy
            raise fatalError("Another deploy already in progress!")
        status = self.opsManager.doPut("/groups/"+self.projectId+"/automationConfig",newAutomation)
        if status != 0:
            raise fatalError("Error status {} attempting to PUT new config!".format(status))
        logging.info("Deploying changes to {}.".format(self.projName))
        deployTime = 0
        notDeployed = []
        while not self.allGoal():
            logging.info("Waiting for Goal state.")
            time.sleep(5)
            deployTime += 5
            if (self.deployTimeout != None) and (deployTime > self.deployTimeout):
                notDeployed = self.notGoal()
                break
        logging.info("Reached Goal state.")
        if len(notDeployed) > 0:
            logging.warning("{} Nodes failed to deploy.".format(len(notDeployed)),logDB=True)
        return notDeployed
    
class alertConfig:
    def __init__(self,OM,projectId):
        self.projectId = projectId
        self.OM = OM
        self.projectAlerts = []
        response = OM.doRequest("/groups/"+self.projectId+"/alertConfigs")
        while response != None:
            for alert in response["results"]:
                self.projectAlerts.append(alert)
            response = OM.getNextPage(response)
        return 
    
    def createAllAlerts(self):
        for alert in self.ProjectAlerts:
            self.createOneAlert(alert)
        return
    
    def createGlobalAlerts(self):
        for alert in self.ProjectAlerts:
            if len(alert["matchers"]) == 0:
                self.createOneAlert(alert)
        return
    
    def createOneAlert(self,alert,newProjectId):
        self.OM.doPost("/groups/"+newProjectId+"/alertcConfigs",alert)
        return
        
    def getGlobalAlerts(self):
        return self.globalAlerts

