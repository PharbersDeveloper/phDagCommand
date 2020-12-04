import os
import sys
import subprocess
from ph_logs.ph_logs import phlogger
from ph_max_auto import define_value as dv
from ph_max_auto.ph_config.phconfig.phconfig import PhYAMLConfig

DIR_NAME = 'preset_write_asset'


def job_conf_args2map(path):
    config = PhYAMLConfig(path)
    config.load_yaml()

    result = {'input': {}, 'output': {}}
    for arg in config.spec.containers.args:
        if arg.value != "":
            if sys.version_info > (3, 0):
                result['input'][arg.key] = str(arg.value)
            else:
                if type(arg.value) is unicode:
                    result['input'][arg.key] = arg.value.encode("utf-8")
                else:
                    result['input'][arg.key] = str(arg.value)

    for output in config.spec.containers.outputs:
        if output.value != "":
            if sys.version_info > (3, 0):
                result['output'][output.key] = str(output.value)
            else:
                if type(output.value) is unicode:
                    result['output'][output.key] = str(output.value.encode("utf-8"))
                else:
                    result['output'][output.key] = str(output.value)

    return result


def merge_job_args(m1, m2):
    for k, v in m2.items():
        if k in m1.keys() and v != m1[k]:
            phlogger.warning("{} 在之前的 job 中已经被定义，将使用 {} 覆盖 {}".format(k, v, m1[k]))
        m1[k] = v


def copy_job(context, **kwargs):
    # 0. init data
    config = PhYAMLConfig(context.combine_path, "/phdag.yaml")
    config.load_yaml()

    # 1. /__init.py file
    subprocess.call(["mkdir", context.dag_path + DIR_NAME])
    subprocess.call(["touch", context.dag_path + DIR_NAME + "/__init__.py"])

    # 2. /phjob.py file
    phjobs_lst = []
    for jt in config.spec.jobs:
        if jt.name.startswith('preset'):
            continue

        job_name = jt.name.replace('.', '_')
        job_full_path = context.cur_proj_dir + context.job_prefix + jt.name.replace('.', '/')
        args_map = job_conf_args2map(job_full_path)

        phjobs_tmp = ['"{}": {{'.format(job_name)]
        for scope, args in args_map.items():
            phjobs_tmp.append('\t"{}": {{'.format(scope))
            for k, v in args.items():
                phjobs_tmp.append('\t\t"{}": "{}",'.format(k, v))
            phjobs_tmp.append('\t},')
        phjobs_tmp.append('},')

        phjobs_lst += ['\t'+s for s in phjobs_tmp]
    phjobs_str = 'phjobs = {\n' + '\n'.join(['\t'+s for s in phjobs_lst]) + '\n\t}'

    job_phjob_abs_path = os.path.abspath(__file__).split('/')
    job_phjob_abs_path[-1] = "phjob.py"
    job_phjob_abs_path = '/'.join(job_phjob_abs_path)
    with open(context.dag_path + DIR_NAME + "/phjob.py", 'w') as target:
        with open(job_phjob_abs_path) as source:
            data = source.read()
            data = data.replace("phjobs = {}", phjobs_str)
            target.write(data)

    # 3. /phmain.py file
    all_jobs_args_map = {}
    for jt in config.spec.jobs:
        if jt.name.startswith('preset'):
            continue
        job_name = jt.name.replace('.', '/')
        job_full_path = context.cur_proj_dir + context.job_prefix + job_name
        result = job_conf_args2map(job_full_path)
        result = dict([(k, v) for scope, args in result.items() for k, v in args.items()])
        merge_job_args(all_jobs_args_map, result)

    must_args = [arg.strip() for arg in dv.PRESET_MUST_ARGS.split(',')]
    click_option_lst = must_args + [k for k in all_jobs_args_map]
    click_option_lst = ["@click.option('--{}')".format(k) for k in click_option_lst]
    click_option_str = '\n'.join(click_option_lst)

    job_phmain_abs_path = os.path.abspath(__file__).split('/')
    job_phmain_abs_path[-1] = "phmain.py"
    job_phmain_abs_path = '/'.join(job_phmain_abs_path)
    with open(context.dag_path + DIR_NAME + "/phmain.py", 'w') as target:
        with open(job_phmain_abs_path) as source:
            data = source.read()
            data = data.replace("@click.option('--job_id')", click_option_str)
            target.write(data)

    # 4. /args.properties file
    with open(context.dag_path + DIR_NAME + "/args.properties", "w") as f:
        for k, v in all_jobs_args_map.items():
            f.write('--{}\n'.format(k))
            f.write('{}\n'.format(v))
