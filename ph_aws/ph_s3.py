# -*- coding: utf-8 -*-

import sys
import string
import boto3

from ph_errs.ph_err import PhException
from ph_aws.aws_root import PhAWS


class PhS3(PhAWS):
    def __init__(self, *args, **kwargs):
        self.phsts = kwargs.get('phsts', None)

        if self.phsts:
            self.s3_client = boto3.client('s3', **self.phsts.get_cred())
        else:
            self.s3_client = boto3.client('s3')

    def list_buckets(self):
        bks = self.s3_client.list_buckets()["Buckets"]
        bks_names = []
        for it in enumerate(bks):
            bks_names.append(it[1]["Name"])
        return bks_names

    def upload(self, file, bucket_name, object_name):
        """
        上传本地文件到 S3
        :param file: 本地文件路径
        :param bucket_name: S3 桶名字
        :param object_name: S3 文件路径
        :return:
        """
        self.s3_client.upload_file(
            Bucket=bucket_name,
            Key=object_name,
            Filename=file
        )

    def open_object(self, bucket_name, object_name):
        """
        使用字符串打开 S3 文件
        :param bucket_name: S3 桶名字
        :param object_name: S3 文件路径
        :return: str
        """
        response = self.s3_client.get_object(
            Bucket=bucket_name,
            Key=object_name
        )
        if sys.version_info > (3, 0):
            return response["Body"].read().decode()
        else:
            return response["Body"].read()

    def open_object_by_lines(self, bucket_name, object_name):
        """
        按行打开 S3 文件
        :param bucket_name: S3 桶名字
        :param object_name: S3 文件路径
        :return: List[str]
        """
        object_str = self.open_object(bucket_name, object_name)
        if sys.version_info > (3, 0):
            return str.split(object_str, "\n")
        else:
            return filter(lambda x: x != "", string.split(object_str, "\n"))

    def download(self, bucket_name, object_name, file):
        """
        下载 S3 文件
        :param bucket_name: S3 桶名字
        :param object_name: S3 文件路径
        :param file: 本地文件路径
        :return:
        """
        f = open(file, "w")
        for line in self.open_object_by_lines(bucket_name, object_name):
            f.write(line + "\n")
        f.close()

    def __url_get_bucket_info(self, path):
        """
        根据 S3 URL 分析出 S3 Bucket 和文件的具体路径
        :param path: S3 URL
        :return: [bucket_name, file_path]
        """
        if not isinstance(path, str):
            raise PhException('Expected an str')

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
            raise PhException("The url is wrong")

    def sync_file_local_to_s3(self, path, bucket_name, dir_name, version=''):
        """
        如果参数的是本地文件，则自动同步到 S3, 然后返回桶名和文件路径
        如果参数的是 S3 的文件，则直接返回桶名和文件路径
        :param path: 文件路径
        :param bucket_name: 同步的桶名称
        :param dir_name: 放置到 S3 的目录位置
        :param version: 文件版本
        :param credentials: assumerole 证书，没有则使用执行者证书
        :return: [bucket_name, file_path]
        """

        if path.startswith("https://") or path.startswith("http://") or path.startswith("s3://"):
            return self.__url_get_bucket_info(path)
        else:
            object_name = path.split("/")[-1]
            if version != "":
                object_name = ".".join(object_name.split(".")[:-1]) + "-" + version + "." + object_name.split(".")[-1]

            if dir_name.endswith("/"):
                object_name = dir_name + object_name
            else:
                object_name = dir_name + "/" + object_name

            self.upload(path, bucket_name, object_name)
            return [bucket_name, object_name]
