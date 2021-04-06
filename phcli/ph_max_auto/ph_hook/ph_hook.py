from phcli.ph_max_auto.ph_hook.get_spark_session import get_spark_session_func
from phcli.ph_max_auto.ph_hook.get_abs_path import get_result_path
from phcli.ph_max_auto.ph_hook.get_abs_path import get_depends_path
from phcli.ph_max_auto.ph_hook.lineage import lineage
from phcli.ph_max_auto.ph_hook.copy_asset_data import copy_asset_data


def exec_before(*args, **kwargs):
    name = kwargs.get('name', None)
    job_id = kwargs.get('job_id', name)

    spark_func = get_spark_session_func(job_id)
    result_path_prefix = get_result_path(kwargs)
    depends_path = get_depends_path(kwargs)

    return {
        'spark': spark_func,
        'result_path_prefix': 's3://ph-max-auto/2020-08-11/data_matching/refactor/runs/manual__2021-01-18T04:49:20.117595+00:00/cleaning_data_normalization',
        'depends_path': depends_path,
    }


def exec_after(*args, **kwargs):
    owner = kwargs.get('owner', None)
    run_id = kwargs.get('run_id', None)
    job_id = kwargs.get('job_id', None)

    lineage(job_id, kwargs)
    copy_asset_data(kwargs)

    return kwargs



