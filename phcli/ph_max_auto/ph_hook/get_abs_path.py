import time
from phcli.ph_max_auto import define_value as dv


def get_run_time():
    return time.strftime("%Y-%m-%d_%H:%M:%S", time.localtime())


def get_dag_name(kwargs):
    dag_name = kwargs["dag_name"]
    return dag_name


def get_run_id(kwargs):
    return kwargs.get("run_id", "runid_" + "alfred_runner_test")


def get_job_full_name(kwargs):
    job_name = kwargs["job_name"]
    return job_name


def get_job_name(kwargs):
    job_name = kwargs["name"]
    return job_name


def get_result_path(kwargs):
    run_id = get_run_id(kwargs)
    job_name = get_job_name(kwargs)
    if 'path_prefix' in kwargs:
        path_prefix = kwargs['path_prefix']
    else:
        path_suffix = kwargs.get('path_suffix', dv.DEFAULT_RESULT_PATH_SUFFIX)
        path_prefix = dv.DEFAULT_RESULT_PATH_FORMAT_STR.format(
            bucket_name=dv.DEFAULT_RESULT_PATH_BUCKET,
            version=dv.DEFAULT_RESULT_PATH_VERSION,
            dag_name=get_dag_name(kwargs),
        ) + path_suffix

    return path_prefix + run_id + "/" + job_name + "/"


def get_depends_file_path(kwargs, job_name, job_key):
    return get_result_path(kwargs) + job_name + "/" + job_key


def get_depends_path(kwargs):
    depends_lst = eval(kwargs.get("depend_job_names_keys", []))
    result = {}
    for item in depends_lst:
        tmp_lst = item.split("#")
        depends_job = tmp_lst[0]
        depends_key = tmp_lst[1]
        depends_name = tmp_lst[2]
        result[depends_name] = get_depends_file_path(kwargs, depends_job, depends_key)
    return result


def get_target_path(kwargs):
    run_time = get_run_time()
    job_name = get_job_name(kwargs)
    dag_name = get_dag_name(kwargs)
    target_path = dv.DEFAULT_TARGET_PATH_FORMAT_STR.format(
        bucket_name=dv.DEFAULT_RESULT_PATH_BUCKET,
        version=dv.DEFAULT_TARGET_PATH_PREFIX
    )
    return target_path +'/'+ run_time+'/'+dag_name+'/'+job_name+'/'


if __name__ == '__main__':
    path_prefix_suffix_result = get_result_path({
        "name": "test_job",
        "dag_name": "test_dag"
    })
    print("path_prefix_suffix_result = " + path_prefix_suffix_result)
    assert(path_prefix_suffix_result == "s3://ph-max-auto/2020-08-11/test_dag/refactor/runs/runid_alfred_runner_test/test_job/")

    path_prefixexist_suffixexist_result = get_result_path({
        "name": "test_job",
        "dag_name": "test_dag",
        "path_prefix": "s3://exist/",
        "path_suffix": "exist"
    })
    print("path_prefixexist_suffixexist_result = " + path_prefixexist_suffixexist_result)
    assert(path_prefixexist_suffixexist_result == "s3://exist/runid_alfred_runner_test/test_job/")

    path_prefixexist_suffixexist_depends_file_path = get_depends_file_path({
        "name": "test_job",
        "dag_name": "test_dag",
        "path_prefix": "s3://exist/",
        "path_suffix": "exist"
    }, "test1", "c")
    print("path_prefixexist_suffixexist_depends_file_path = " + path_prefixexist_suffixexist_depends_file_path)
    assert(path_prefixexist_suffixexist_depends_file_path == "s3://exist/runid_alfred_runner_test/test_job/test1/c")

    prefixexist_suffixexist_depends_path = get_depends_path({
        "name": "test_job",
        "dag_name": "test_dag",
        "path_prefix": "s3://exist/",
        "path_suffix": "exist",
        "depend_job_names_keys": '["effectiveness_adjust_mnf#mnf_adjust_result#mnf_adjust", "effectiveness_adjust_spec#spec_adjust_result#spec_adjust"]'
    })
    print("prefixexist_suffixexist_depends_path = " + str(prefixexist_suffixexist_depends_path))
    assert len(prefixexist_suffixexist_depends_path) == 2
    assert "mnf_adjust" in prefixexist_suffixexist_depends_path
    assert prefixexist_suffixexist_depends_path['mnf_adjust'] == 's3://exist/runid_alfred_runner_test/test_job/effectiveness_adjust_mnf/mnf_adjust_result'
    assert "spec_adjust" in prefixexist_suffixexist_depends_path
    assert prefixexist_suffixexist_depends_path['spec_adjust'] == 's3://exist/runid_alfred_runner_test/test_job/effectiveness_adjust_spec/spec_adjust_result'

    path_target_path = get_target_path({
        'name': 'test_job',
        'dag_name': 'test_dag'
    })
    print("path_target_path = " + path_target_path)
    assert(path_target_path == "s3://ph-max-auto/result_data/2021-03-14_10:07:02/test_dag/test_job/")