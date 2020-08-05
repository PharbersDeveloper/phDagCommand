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

    def get_rt_inst(self, runtime):
        runtimes = [rt.lower() for rt in runtime.split(",")]
        for runtime in runtimes:
            if "python" in runtime:
                return python_rt.PythonRT()
            elif "nodejs" in runtime:
                return nodejs_rt.NodejsRT()
            elif "go" in runtime:
                return go_rt.GoRT()
        else:
            raise PhError("Invalid runtime")

    def put_s3_object(self, file, bucket_name, object_name):
        """
        上传本地文件到 S3
        :param file: 本地文件路径
        :param bucket_name: S3 桶名字
        :param object_name: S3 文件路径
        :return:
        """
        s3_client = boto3.client('s3')
        file_buf = open(file, 'rb')
        s3_client.put_object(
            Bucket=bucket_name,
            Key=object_name,
            Body=file_buf,
        )

    def url_get_bucket_file(self, path):
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

    def sync_local_s3_file(self, path, bucket_name, dir_name, version):
        """
        如果上传的是本地文件，则自动同步到 S3, 然后返回桶名和文件路径
        如果上传的是 S3 的文件，则直接返回桶名和文件路径
        :param path: 文件路径
        :param bucket_name: 同步的桶名称
        :param dir_name: 放置到 S3 的目录位置
        :param version: 文件版本
        :return: [bucket_name, file_path]
        """
        if path.startswith("https://") or path.startswith("http://") or path.startswith("s3://"):
            return self.url_get_bucket_file(path)
        else:
            object_name = path.split("/")[-1]
            if version != "":
                object_name = ".".join(object_name.split(".")[:-1]) + "-" + version + "." + object_name.split(".")[-1]
            if dir_name.endswith("/"):
                object_name = dir_name + object_name
            else:
                object_name = dir_name + "/" + object_name

            self.put_s3_object(path, bucket_name, object_name)
            return [bucket_name, object_name]
