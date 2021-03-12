
import time
from phcli.ph_max_auto import define_value as dv

def get_run_id(kwargs):
    run_id = kwargs["run_id"]
    if not run_id:
        run_id = "runid_" + "alfred_runner_test"
    return run_id


def get_run_time():
    run_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    return run_time


def get_job_name(kwargs):
    job_name = kwargs["job_name"]
    return job_name
    
def get_dag_name(kwargs):
    dag_name = kwargs["dag_name"]
    return dag_name


def get_result_path_prefix(kwargs):
    run_time = get_run_time()
    job_name = get_job_name(kwargs)
    return path_prefix + "/" + run_id + "/" + run_time + "/"

def get_target_path(kwargs):
    run_time = get_run_time()
    job_name = get_job_name(kwargs)
    dag_name = get_dag_name(kwargs)
    target_path = dv.TARGET_PATH_PREFIX
    return target_path + "/" + run_time+'/'+dag_name+'/'+job_name+'/'