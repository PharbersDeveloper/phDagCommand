import boto3

def get_args_from_ssm(kwargs):
    ssm_client = boto3.client('ssm')
    response = ssm_client.get_parameter(
        Name=kwargs.get('ssm_args_name')
    )
    return response['Parameter']['Value']

def delete_args_from_ssm(kwargs):
    ssm_client = boto3.client('ssm')
    ssm_client.delete_parameter(
        Name=kwargs.get('ssm_args_name')
    )