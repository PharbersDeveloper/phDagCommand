import os
import time

class PhEmrOperation(object):
    def __init__(self, **kwargs):
        self.emr_name = kwargs.get('name', None)
        self.emr_operation = kwargs.get('operation', None)


    def choice_operation(self):
        if self.emr_operation == "create":
            self.create_emr()
        if self.emr_operation == "delete":
            self.delete_emr()


    def create_emr(self):
        print(self.emr_name)
        create_cluster_cmd1 = "sudo curl https://ph-platform.s3.cn-northwest-1.amazonaws.com.cn/2020-11-11/emr/common/client/emr.yaml -o ./emr.yaml"
        create_cluster_cmd2 = "aws cloudformation create-stack --stack-name "+ self.emr_name +" --template-body file://emr.yaml"
        os.system(create_cluster_cmd1)
        time.sleep(15)
        os.system(create_cluster_cmd2)
        print("create_emr success")
        create_cluster_cmd3 = "sudo rm -f emr.yaml"
        os.system(create_cluster_cmd3)

    def delete_emr(self):
        print(self.emr_operation)
        delete_cluster_cmd = "aws cloudformation delete-stack --stack-name " + self.emr_name
        os.system(delete_cluster_cmd)