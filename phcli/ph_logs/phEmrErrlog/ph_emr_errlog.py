import subprocess
import os
import sys
import json
import re

class phErrLogs(object):
    
    def __init__(self,**kwargs):
        self.clusterId = kwargs.get('cluster_id',None)
        self.stepId = kwargs.get('step_id',None)
      
     
    def extractlog(self):
        self.getLogByClusterAndstep()
        self.getLogByAppLicationId()
        
    # j-2VZ406OMKJUZU   s-1WZE1QAMF1VR2     
    def getLogByClusterAndstep(self):
        JS_comm = "aws emr describe-step --cluster-id " + self.clusterId + " --step-id " + self.stepId
        JS_logs = os.popen(JS_comm)
        logs = JS_logs.read()
        log = json.loads(logs)
        applicationid = re.findall('application_\d*_\d*',logs)
        state = log["Step"]["Status"]["State"]
        if state in "COMPLETED":
            print("集群ID或者步骤ID输入有误")
        else:
            log_path = "s3://ph-platform/2020-11-11/emr/yarnLogs/hadoop/logs-tfile/" + applicationid[0] + "/"
            comm_ls = "aws s3 cp --recursive " + log_path + " ./JS_log"
            cat_comm = "cat ./JS_log/*.compute.internal_8041 > otherLog"
            os.popen(comm_ls + " && " + cat_comm + " && " + "rm -rf ./JS_log")

                           
    def getLogByAppLicationId(self):
        stderr_path = "s3://ph-platform/2020-11-11/emr/logs/" + self.clusterId + "/" + "steps/" + self.stepId + "/"
        A_comm = "aws s3 cp --recursive " + stderr_path + " ./Application_log"
        gun_comm = "gunzip ./Application_log/stderr.gz"
        cat_comm = "cat ./Application_log/stderr > driverLog"
        os.popen(A_comm +" && "+ gun_comm + " && " + cat_comm + " && " + "rm -rf ./Application_log")