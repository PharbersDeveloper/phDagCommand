import os

class PhJupyterOperation(object):
    def __init__(self, **kwargs):
        self.jupyter_name = kwargs.get('name', None)
        self.jupyter_operation = kwargs.get('operation', None)


    def choice_operation(self):
        if self.jupyter_operation == "create":
            self.create_jupyter()
        if self.jupyter_operation == "delete":
            self.delete_jupyter()


    def get_active_clusterId(self):
        # 获取clusterId
        ls_cmd = "aws emr list-clusters --active"
        cluster = os.popen(ls_cmd).readlines()
        for cluster_line in cluster:
            if 'Name": "phdev' in cluster_line:
                clusterId_index = cluster.index(cluster_line) - 1
        self.cluster_id = cluster[clusterId_index].lstrip('            "Id": "').rstrip('",\n')
        return self.cluster_id


    def put_clusterId_to_ssm(self, cluster_id):
        # 3.将获取的clusterId 写入到ssm中
        put_parameter_cmd = 'aws ssm put-parameter --name "cluster_id" --type String --overwrite --value ' + cluster_id
        os.system(put_parameter_cmd)


    def create_jupyter(self):
        cluster_id = self.get_active_clusterId()
        self.put_clusterId_to_ssm(cluster_id)

        # 4.需要创建jupyter的用户
        users = self.jupyter_name.split(",")
        # 5.创建ec2实例
        create_jupyter_cmd1 = "sudo curl https://ph-platform.s3.cn-northwest-1.amazonaws.com.cn/2020-11-11/emr/common/client/jupyterec2cfn.yaml -o ./jupyterec2cfn.yaml"
        os.system(create_jupyter_cmd1)
        for user in users:
            create_jupyter_cmd2 = "aws cloudformation create-stack --stack-name " + user + "-jupyter " \
                                                                                           "--template-body file://jupyterec2cfn.yaml " \
                                                                                           "--parameters ParameterKey=EMRClusterId,ParameterValue=" + cluster_id + \
                                  " ParameterKey=EC2User,ParameterValue=" + user
            os.system(create_jupyter_cmd2)
        create_jupyter_cmd3 = "sudo rm -f emr.yaml"
        os.system(create_jupyter_cmd3)


    def delete_jupyter(self):
        # 需要删除jupyter的用户
        users = self.jupyter_name.split(",")

        # 删除所有的jupyter实例
        for user in users:
            delete_jupyter_cmd = "aws cloudformation delete-stack --stack-name " + user + "-jupyter"
            os.system(delete_jupyter_cmd)