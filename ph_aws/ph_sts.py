# -*- coding: utf-8 -*-

import boto3
import botocore.exceptions

from ph_aws.aws_root import PhAWS
from ph_logs.ph_logs import phlogger


class PhSts(PhAWS):
    def __init__(self):
        self.credentials = None

    def get_cred(self):
        if not self.credentials:
           return {}

        return {
            'aws_access_key_id': self.credentials['AccessKeyId'],
            'aws_secret_access_key': self.credentials['SecretAccessKey'],
            'aws_session_token': self.credentials['SessionToken'],
        }

    def assume_role(self, role_arn, external_id):
        sts_client = boto3.client('sts')

        try:
            assumed_role_object = sts_client.assume_role(
                RoleArn=role_arn,
                RoleSessionName=external_id,
                ExternalId=external_id,
            )
        except botocore.exceptions.ClientError as err:
            phlogger.warn(err)
            self.credentials = {}
        else:
            self.credentials = assumed_role_object['Credentials']

        return self
