import subprocess
import os
import json

class PhInitConf(object):

    def init_conf():

        # 从ssm获取cluster_id
        cluster_id_cmd = "aws ssm get-parameter --name cluster_id"
        cluster_id_info = os.popen(cluster_id_cmd).readlines()
        cluster_id_str = ''.join(cluster_id_info)
        cluster_id_dict = json.loads(cluster_id_str)
        cluster_id = cluster_id_dict['Parameter']['Value']

        hadoop_cmd1 = "aws s3 cp s3://ph-platform/2020-11-11/emr/remoteConfig/" + cluster_id + "/etc/hadoop/conf/ hadoop/conf/ --recursive"
        hadoop_cmd2 = "sudo cp hadoop/conf/* /etc/hadoop/conf/"
        spark_cmd1 = "aws s3 cp s3://ph-platform/2020-11-11/emr/remoteConfig/" + cluster_id + "/etc/spark/conf/ spark/conf/ --recursive"
        spark_cmd2 = "sudo cp spark/conf/* /etc/spark/conf/"
        hive_cmd1 = "aws s3 cp s3://ph-platform/2020-11-11/emr/remoteConfig/" + cluster_id + "/etc/hive/conf/ hive/conf/ --recursive"
        hive_cmd2 = "sudo cp hive/conf/* /etc/hive/conf/"
        hive_hcatalog_cmd1 = "aws s3 cp s3://ph-platform/2020-11-11/emr/remoteConfig/" + cluster_id + "/etc/hive-hcatalog/conf/ hive-hcatalog/conf/ --recursive"
        hive_hcatalog_cmd2 = "sudo cp hive-hcatalog/conf/* /etc/hive-hcatalog/conf/"
        tez_cmd1 = "aws s3 cp s3://ph-platform/2020-11-11/emr/remoteConfig/" + cluster_id + "/etc/tez/conf/ tez/conf/ --recursive"
        tez_cmd2 = "sudo cp tez/conf/* /etc/tez/conf/"
        cmd = hadoop_cmd1 + " && " + hadoop_cmd2 + \
              " && " + spark_cmd1 + " && " + spark_cmd2 + \
              " && " + hive_cmd1 + " && " + hive_cmd2 + \
              " && " + hive_hcatalog_cmd1 + " && " + hive_hcatalog_cmd2 + \
              " && " + tez_cmd1 + " && " + tez_cmd2
        subprocess.call(cmd, shell=True)