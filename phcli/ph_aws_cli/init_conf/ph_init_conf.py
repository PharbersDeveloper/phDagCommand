import subprocess

class PhInitConf(object):

    def init_conf(cluster_id):
        cmd1 = "aws s3 cp s3://ph-platform/2020-11-11/emr/remoteConfig/"+ cluster_id +"/etc/hadoop/conf/ hadoop/conf/ --recursive"
        cmd2 = "sudo cp hadoop/conf/* /etc/hadoop/conf/"
        cmd3 = "aws s3 cp s3://ph-platform/2020-11-11/emr/remoteConfig/"+ cluster_id +"/etc/spark/conf/ spark/conf/ --recursive"
        cmd4 = "sudo cp spark/conf/* /etc/spark/conf/"
        cmd = cmd1 + " && " + cmd2 + " && " + cmd3 + " && " + cmd4
        subprocess.call(cmd, shell=True)
