# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This module document the YAML Job Args for Pharbers jobs
"""
import boto3
import pandas as pd
import string
import sys


class PhS3(object):
    def __init__(self):
        self.client = boto3.client('s3')

    def list_s3_buckets(self):
        bks = self.client.list_buckets()["Buckets"]
        bks_names = []
        for it in enumerate(bks):
            bks_names.append(it[1]["Name"])
        return bks_names

    def get_object_lines(self, bk_name, s3_path):
        response = self.client.get_object(
            Bucket=bk_name,
            Key=s3_path
        )
        if sys.version_info > (3, 0):
            return str.split(response["Body"].read().decode(), "\n")
        else:
            res = response["Body"].read()
            return string.split(res, "\n")

    def copy_object_2_file(self, bk_name, s3_path, local_path):
        f = open(local_path, "w")
        for line in self.get_object_lines(bk_name, s3_path):
            f.write(line + "\n")
        f.close()

    def put_object(self, bk_name, s3_path, local_path):
        self.client.upload_file(
            Bucket=bk_name,
            Key=s3_path,
            Filename=local_path
        )

    def get_excel_from_s3(self, s3_path):
        return pd.read_excel(s3_path)


s3 = PhS3()
