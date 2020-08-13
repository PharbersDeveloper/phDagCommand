# -*- coding: utf-8 -*-

import boto3
from phlmd.runtime import python_rt
from phlmd.runtime import nodejs_rt
from phlmd.runtime import go_rt
from pherrs.ph_err import PhError


class AWSUtil(object):
    """
    AWS 的常用操作
    """

    @staticmethod
    def assume_role(role_arn, external_id):
        sts_client = boto3.client('sts')
        assumed_role_object = sts_client.assume_role(
            RoleArn=role_arn,
            RoleSessionName=external_id,
            ExternalId=external_id,
        )
        credentials = assumed_role_object['Credentials']

        return {
            'aws_access_key_id': credentials['AccessKeyId'],
            'aws_secret_access_key': credentials['SecretAccessKey'],
            'aws_session_token': credentials['SessionToken'],
        }

    @staticmethod
    def get_short_rt(runtime):
        if "python" in runtime:
            return "python"
        elif "nodejs" in runtime:
            return "nodejs"
        elif "go" in runtime:
            return "go"
        raise PhError("Invalid runtime")

    def get_rt_inst(self, runtime):
        rt_table = {
            'python': python_rt.PythonRT,
            'nodejs': nodejs_rt.NodejsRT,
            'go': go_rt.GoRT,
        }

        rt = rt_table[self.get_short_rt(runtime)]
        return rt()

    @staticmethod
    def put_s3_object(file, bucket_name, object_name, credentials=None):
        """
        上传本地文件到 S3
        :param file: 本地文件路径
        :param bucket_name: S3 桶名字
        :param object_name: S3 文件路径
        :param credentials: assumerole 证书，没有则使用执行者证书
        :return:
        """
        if credentials:
            s3_client = boto3.client('s3', **credentials)
        else:
            s3_client = boto3.client('s3')

        file_buf = open(file, 'rb')
        s3_client.put_object(
            Bucket=bucket_name,
            Key=object_name,
            Body=file_buf,
        )

    @staticmethod
    def __url_get_bucket_file(path):
        """
        根据 S3 URL 分析出 S3 Bucket 和文件的具体路径
        :param path: S3 URL
        :return: [bucket_name, file_path]
        """
        if not isinstance(path, str):
            raise PhError('Expected an str')

        if path.startswith("https://") or path.startswith("http://"):
            url = path.split("://")[1]
            bucket_name = url.split(".")[0]
            file_path = url.split(".amazonaws.com.cn/")[1]
            return [bucket_name, file_path]
        elif path.startswith("s3://"):
            url = path.split("://")[1]
            bucket_name = url.split("/")[0]
            file_path = "/".join(url.split("/")[1:])
            return [bucket_name, file_path]
        else:
            raise PhError("The url is wrong")

    def sync_local_s3_file(self, path, bucket_name, dir_name, version='', credentials=None):
        """
        如果上传的是本地文件，则自动同步到 S3, 然后返回桶名和文件路径
        如果上传的是 S3 的文件，则直接返回桶名和文件路径
        :param path: 文件路径
        :param bucket_name: 同步的桶名称
        :param dir_name: 放置到 S3 的目录位置
        :param version: 文件版本
        :param credentials: assumerole 证书，没有则使用执行者证书
        :return: [bucket_name, file_path]
        """

        if path.startswith("https://") or path.startswith("http://") or path.startswith("s3://"):
            return self.__url_get_bucket_file(path)
        else:
            object_name = path.split("/")[-1]
            if version != "":
                object_name = ".".join(object_name.split(".")[:-1]) + "-" + version + "." + object_name.split(".")[-1]

            if dir_name.endswith("/"):
                object_name = dir_name + object_name
            else:
                object_name = dir_name + "/" + object_name

            self.put_s3_object(path, bucket_name, object_name, credentials)
            return [bucket_name, object_name]
