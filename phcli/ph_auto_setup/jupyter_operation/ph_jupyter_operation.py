import os
import json


class PhJupyterOperation(object):
    def __init__(self, **kwargs):
        self.jupyter_name = kwargs.get('name', None)
        self.jupyter_operation = kwargs.get('operation', None)


    def choice_operation(self):
        if self.jupyter_operation == "create":
            self.create_jupyter()
        if self.jupyter_operation == "delete":
            self.delete_jupyter()


    def create_jupyter(self):
        # 4.需要创建jupyter的用户
        users = self.jupyter_name.split(",")

        # 从ssm获取cluster_id
        ls_cmd = "aws ssm get-parameter --name cluster_id"
        cluster_id_info = os.popen(ls_cmd).readlines()
        cluster_id_str = ''.join(cluster_id_info)
        cluster_id_dict = json.loads(cluster_id_str)
        cluster_id = cluster_id_dict['Parameter']['Value']

        # 5.创建ec2实例
        create_jupyter_cmd1 = "curl https://ph-platform.s3.cn-northwest-1.amazonaws.com.cn/2020-11-11/emr/common/client/jupyterec2cfn.yaml -o ./jupyterec2cfn.yaml"
        os.system(create_jupyter_cmd1)
        for user in users:
            create_jupyter_cmd2 = "aws cloudformation create-stack --stack-name " + user + "-jupyter " \
                                                                                           "--template-body file://jupyterec2cfn.yaml " \
                                                                                           "--parameters ParameterKey=EMRClusterId,ParameterValue=" + cluster_id + \
                                  " ParameterKey=EC2User,ParameterValue=" + user
            os.system(create_jupyter_cmd2)
        create_jupyter_cmd3 = "rm -f jupyterec2cfn.yaml"
        os.system(create_jupyter_cmd3)


    def delete_jupyter(self):
        # 需要删除jupyter的用户
        users = self.jupyter_name.split(",")

        # 删除所有的jupyter实例
        for user in users:
            delete_jupyter_cmd = "aws cloudformation delete-stack --stack-name " + user + "-jupyter"
            os.system(delete_jupyter_cmd)