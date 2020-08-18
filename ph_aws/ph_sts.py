# -*- coding: utf-8 -*-

import boto3
from ph_aws.aws_root import PhAWS


class PhSts(PhAWS):
    def __init__(self):
        self.credentials = None

    def get_cred(self):
        return self.credentials

    def assume_role(self, role_arn, external_id):
        sts_client = boto3.client('sts')
        assumed_role_object = sts_client.assume_role(
            RoleArn=role_arn,
            RoleSessionName=external_id,
            ExternalId=external_id,
        )

        credentials = assumed_role_object['Credentials']
        self.credentials = {
            'aws_access_key_id': credentials['AccessKeyId'],
            'aws_secret_access_key': credentials['SecretAccessKey'],
            'aws_session_token': credentials['SessionToken'],
        }

        return self
