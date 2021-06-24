import subprocess
import os
import sys

class phErrLogs(object):
    
    def _init_(self,**args):
        self.clusterId = args.get('cluster-id',None)
        self.stepId = args.get('step-id',None)
     
    def start(self):
        getLogByClusterAndstep()
        getLogByAppLicationId()
    
    def getLogByClusterAndstep(self):
        JS_comm = "aws emr describe-step --cluster-id " + self.clusterId + " --step-id " + self.stepId
        JS_logs = os.popen(JS_comm).readlines()
        for JS_log in JS_logs:
            if 'Application' in JS_log:
                appication = JS_log
                appid = appication.split(" ")
                for aid in appid:
                    if 'application_' in aid:
                        JS_id = aid
        log_path = "s3://ph-platform/2020-11-11/emr/yarnLogs/hadoop/logs-tfile/" + JS_id + "/"
        comm_ls = "aws s3 sync " + log_path + " ./JS_log"
        os.popen(comm_ls)
        os.popen("cat JS_log/*.compute.internal_8041 > JS_result")
    
    def getLogByAppLicationId(self): 
        stderr_path = "s3://ph-platform/2020-11-11/emr/logs/" + self.clusterId + "/" + "steps/" + self.stepId + "/"
        A_comm = "aws s3 sync " + stderr_path + " ./Application_log"
        os.popen(A_comm)
        gun_comm = "gunzip Application_log/stderr.gz"
        os.popen(gun_comm)
        A_logs = os.popen("cat Application_log/stderr").readlines()
        for A_log in A_logs:
            if 'Exception' in A_log:
                appication = A_log
                appid = appication.split(" ")
                for aid in appid:
                    if 'application_' in aid:
                        A_id = aid
        log_path = "s3://ph-platform/2020-11-11/emr/yarnLogs/hadoop/logs-tfile/" + A_id + "/"                
        ls_command = "aws s3 sync " + log_path + " ./Application_log"
        os.popen(ls_command)
        os.popen("cat Application_log/*.compute.internal_8041 > A_result")