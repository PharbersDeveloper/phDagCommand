import boto3
import base64
from phcli.ph_max_auto import define_value as dv
from phcli.ph_max_auto.ph_hook.get_abs_path import get_asset_path_prefix, get_run_time
from phcli.ph_aws.ph_s3 import PhS3
from phcli.ph_aws.ph_sts import PhSts


def copy_asset_data(kwargs):
    phsts = PhSts().assume_role(
        base64.b64decode(dv.ASSUME_ROLE_ARN).decode(),
        dv.ASSUME_ROLE_EXTERNAL_ID,
    )
    # boto3.setup_default_session(region_name="cn-northwest-1")
    phs3 = PhS3()

    source_bucket_name = kwargs['result_path_prefix'].split('/')[2]
    source_path_prefix = '/'.join(kwargs['result_path_prefix'].split('/')[3:])
    asset_path_prefix = get_asset_path_prefix(kwargs)
    asset_bucket_name = asset_path_prefix.split('/')[2]
    asset_file_path = '/'.join(asset_path_prefix.split('/')[3:])
    run_time = get_run_time()

    files_path = phs3.get_bucket_files_path(bucket_name=source_bucket_name, path_prefix=source_path_prefix)
    for source_file_path in files_path:
        if 'asset' in source_file_path:
            asset_file_path_suffix = source_file_path.replace(source_path_prefix, '')
            target_file_path = asset_file_path \
                               + asset_file_path_suffix.split('/')[1] + '/' \
                               + run_time + '/' \
                               + asset_file_path_suffix.split('/')[2]
            phs3.copy_file(source_bucket=source_bucket_name,
                           source_file_path=source_file_path,
                           target_bucket=asset_bucket_name,
                           target_file_path=target_file_path
                           )

if __name__ == '__main__':
    kwargs = {
        'path_prefix': 's3://ph-max-auto/2020-08-11/data_matching/refactor/runs',
        'result_path_prefix': 's3://ph-max-auto/2020-08-11/data_matching/refactor/runs/manual__2021-01-18T04:49:20.117595+00:00/cleaning_data_normalization',
        'dag_name': 'test_dag',
        'name': 'test_job',
        'run_id': 'test_run_id'
    }
    copy_asset_data(kwargs)
