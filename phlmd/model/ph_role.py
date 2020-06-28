import boto3
from phlmd.model.aws_operator import AWSOperator
from phlmd.model.aws_util import AWSUtil


class PhRole(AWSOperator):
    """
    lambda 的 代理角色
    """

    aws_util = AWSUtil()
    iam_client = boto3.client('iam')

    def __get_arpd_object(self, data):
        """
        获取 assume role policy document 文档内容
        :param data:
            :arg name: 代理角色名字
            :arg arpd_path: assume role policy document 文档位置，
                            可以是本地（./file/trust-policy.json）或
                            s3 上的文件（s3://ph-api-lambda/template/role/trust-policy-template.json）
        :return: [str] arph_object
        """
        bucket_name, object_name = self.aws_util.sync_local_s3_file(
            path=data["arpd_path"],
            bucket_name=data.get("bucket", self._DEFAULT_BUCKET),
            dir_name=self._DEFAULT_ROLE_DIR.replace("#name#", data["name"]),
            version=data.get("version", ""),
        )

        obj = boto3.client('s3').get_object(Bucket=bucket_name, Key=object_name)
        return obj["Body"].read().decode('utf-8')

    def package(self, data):
        """
        role 不可打包
        """
        print(self.package.__doc__)

    def create(self, data):
        """
        创建 lambda 的代理角色
        :param data:
            :arg name: 代理角色名字
            :arg arpd_path: assume role policy document 文档位置，
                            可以是本地（./file/trust-policy.json）或
                            s3 上的文件（s3://ph-api-lambda/template/role/trust-policy-template.json）
            :arg policys_arn: 对角色附加的默认策略 arn
        """

        arpd_object = self.__get_arpd_object(data)
        response = self.iam_client.create_role(
            RoleName=data["name"],
            AssumeRolePolicyDocument=arpd_object,
        )

        for policy_arn in data.get("policys_arn", []):
            self.iam_client.attach_role_policy(
                RoleName=data["name"],
                PolicyArn=policy_arn,
            )

        return data.get("policys_arn", [])

    def lists(self, data):
        """
        获取所有 role 实例 【暂无】
        """
        print(self.lists.__doc__)

    def get(self, data):
        """
        获取指定 role 实例
        :param data:
            :arg name: 代理角色名字
        """
        try:
            response = self.iam_client.get_role(
                RoleName=data["name"],
            )
        except:
            response = {}

        return response

    def update(self, data):
        """
        更新指定 role 实例
        :param data:
            :arg name: 代理角色名字
            :arg policys_arn: 对角色附加的策略 arn
        """

        for policy_arn in data.get("policys_arn", []):
            self.iam_client.attach_role_policy(
                RoleName=data["name"],
                PolicyArn=policy_arn,
            )
        return data.get("policys_arn", [])

    def apply(self, data):
        """
        创建或更新 lambda 的代理角色
        :param data:
            :arg name: 代理角色名字
            :arg arpd_path: assume role policy document 文档位置，
                            可以是本地（file/trust-policy.json, 会先上传到 S3）或
                            s3 上的文件（s3://ph-api-lambda/template/role/trust-policy-template.json）
            :arg policys_arn: 对角色附加的策略 arn
        """
        if self.get(data) == {}:
            return self.create(data)
        else:
            return self.update(data)

    def stop(self, data):
        """
        role 不可停止
        """
        print(self.stop.__doc__)

    def start(self, data):
        """
        role 不可启动
        """
        print(self.start.__doc__)

    def delete(self, data):
        """
        删除 lambda 的代理角色
        :param data:
            :arg name: 代理角色名字
        """
        response = self.iam_client.delete_role(
            RoleName=data["name"],
        )

        return response
