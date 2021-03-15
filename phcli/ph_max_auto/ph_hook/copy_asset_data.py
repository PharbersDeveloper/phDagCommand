import boto3
from phcli.ph_max_auto import define_value as dv
from phcli.ph_max_auto.ph_hook.get_abs_path import get_asset_path


def copy_aset_data(kwargs):
    source_bucket_name = kwargs['result_path_prefix'].split("/")[2]
    source_path_prefix = '/'.join(kwargs['result_path_prefix'].split('/')[3:])
    asset_path = get_asset_path(kwargs)

    # 在s3进行copy
    s3_resource = boto3.resource('s3')
    s3_client = boto3.client("s3")
    res = s3_client.list_objects(Bucket=source_bucket_name, Prefix=source_path_prefix)
    for item in res['Contents']:
        copy_source = {
            'Bucket': source_bucket_name,
            'Key': item['Key']
        }
        source_filename = item['Key'].split('/')[-1]
        s3_resource.meta.client.copy(copy_source, dv.DEFAULT_ASSET_PATH_BUCKET, asset_path + source_filename)


if __name__ == '__main__':
    kwargs = {
        'path_prefix': 's3://ph-max-auto/2020-08-11/data_matching/refactor/runs',
        'result_path_prefix': 's3://ph-max-auto/2020-08-11/data_matching/refactor/runs/manual__2021-01-18T04:49:20.117595+00:00/cleaning_data_normalization/cleaning_result/',
        'dag_name': 'test_dag',
        'job_name': 'test_job',
        'run_id': 'test_run_id'
    }

    source_bucket_name = kwargs['result_path_prefix'].split("/")[2]
    source_path_prefix = '/'.join(kwargs['result_path_prefix'].split('/')[3:])
    asset_path = get_asset_path(kwargs)

    # 在s3进行copy
    s3_resource = boto3.resource('s3')
    s3_client = boto3.client("s3")
    res = s3_client.list_objects(Bucket=source_bucket_name, Prefix=source_path_prefix)
    for item in res['Contents']:
        copy_source = {
            'Bucket': source_bucket_name,
            'Key': item['Key']
        }
        source_filename = item['Key'].split('/')[-1]
        s3_resource.meta.client.copy(copy_source, dv.DEFAULT_ASSET_PATH_BUCKET, asset_path + source_filename)