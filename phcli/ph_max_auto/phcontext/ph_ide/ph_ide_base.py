import os
import sys
import ast
import subprocess
import boto3
import json
import time
import uuid
import base64
from enum import Enum

from phcli.ph_errs.ph_err import *
from phcli.ph_max_auto import define_value as dv
from phcli import define_value as phcli_dv
from phcli.ph_max_auto.ph_config.phconfig.phconfig import PhYAMLConfig
from phcli.ph_max_auto.ph_preset_jobs.preset_job_factory import preset_factory
from phcli.ph_aws.ph_sts import PhSts
from phcli.ph_logs.ph_logs import phs3logger, LOG_DEBUG_LEVEL
from phcli.ph_tools.snowflakeId.snowflake import IdWorker


class PhCompleteStrategy(Enum):
    S2C = 'special to common'
    C2S = 'common to special'
    KEEP = 'keep still'


class PhIDEBase(object):
    job_prefix = "/phjobs/"
    combine_prefix = "/phcombines/"
    dag_prefix = "/phdags/"
    upload_prefix = "/upload/"

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
        self.__dict__.update(self.get_absolute_path())
        self.logger.debug('maxauto PhIDEBase init')
        self.logger.debug(self.__dict__)

    def get_workspace_dir(self):
        return os.getenv(dv.ENV_WORKSPACE_KEY, dv.ENV_WORKSPACE_DEFAULT)

    def get_current_project_dir(self):
        return os.getenv(dv.ENV_CUR_PROJ_KEY, dv.ENV_CUR_PROJ_DEFAULT)

    def get_absolute_path(self):
        if 'name' not in self.__dict__ and 'job_full_name' in self.__dict__:
            self.name = self.job_full_name

        project_path = self.get_workspace_dir() + '/' + self.get_current_project_dir()
        job_path = project_path + self.job_prefix + (self.group + '/' if 'group' in self.__dict__.keys() else '') + self.name
        combine_path = project_path + self.combine_prefix + self.name + '/'
        dag_path = project_path + self.dag_prefix + self.name + '/'
        upload_path = project_path + self.upload_prefix + self.name + '/'

        return {
            'project_path': project_path,
            'job_path': job_path,
            'combine_path': combine_path,
            'dag_path': dag_path,
            'upload_path': upload_path,
        }

    def check_path(self, path):
        suffixs = ['', '.ipynb']
        for suffix in suffixs:
            tmp_path = path + suffix
            if os.path.exists(tmp_path):
                override = input('Job "' + tmp_path + '" already existed，want to override？(Y/*)')
                if override.upper() == 'Y':
                    subprocess.call(["rm", "-rf", tmp_path])
                else:
                    self.logger.error('Termination Create')
                    sys.exit()

    def table_driver_runtime_inst(self, runtime):
        from ..ph_runtime.ph_rt_python3 import PhRTPython3
        from ..ph_runtime.ph_rt_r import PhRTR
        table = {
            "python3": PhRTPython3,
            "r": PhRTR,
        }
        return table[runtime]

    def create(self, **kwargs):
        """
        默认的创建过程
        """
        self.logger.debug('maxauto 默认的 create 实现')
        self.logger.debug(self.__dict__)

        runtime_inst = self.table_driver_runtime_inst(self.runtime)
        runtime_inst(**self.__dict__).create()

    def choice_complete_strategy(self, special_path, common_path):
        special_last_modify_time = os.path.getmtime(special_path) if os.path.exists(special_path) else 0
        common_last_modify_time = os.path.getmtime(common_path) if os.path.exists(common_path) else 0

        if special_last_modify_time > common_last_modify_time:
            return PhCompleteStrategy.S2C
        elif special_last_modify_time < common_last_modify_time:
            return PhCompleteStrategy.C2S
        else:
            return PhCompleteStrategy.KEEP

    def complete(self, **kwargs):
        """
        默认的补全过程
        """
        self.logger.debug('maxauto 默认的 complete 实现')
        self.logger.debug(self.__dict__)

    def run(self, **kwargs):
        """
        默认的运行过程
        """
        self.logger.debug('maxauto 默认的 run 实现')
        self.logger.debug(self.__dict__)

        config = PhYAMLConfig(self.job_path)
        config.load_yaml()

        if config.spec.containers.repository == "local":
            timeout = float(config.spec.containers.timeout) * 60
            entry_runtime = config.spec.containers.runtime
            entry_runtime = self.table_driver_runtime_inst(self.runtime)().table_driver_runtime_binary(entry_runtime)
            entry_point = config.spec.containers.code
            entry_point = self.job_path + '/' + entry_point

            cb = [entry_runtime, entry_point]
            for arg in config.spec.containers.args:
                cb.append("--" + arg.key)
                cb.append(str(arg.value))
            for output in config.spec.containers.outputs:
                cb.append("--" + output.key)
                cb.append(str(output.value))
            prc = subprocess.run(cb, timeout=timeout, stderr=subprocess.PIPE)
            if prc.returncode != 0:
                raise Exception(prc.stderr.decode('utf-8'))
            return
        else:
            raise exception_function_not_implement

    def combine(self, **kwargs):
        """
        默认的关联过程
        """
        self.logger.debug('maxauto 默认的 combine 实现')
        self.logger.debug(self.__dict__)

        self.check_path(self.combine_path)
        subprocess.call(["mkdir", "-p", self.combine_path])

        def extract_jobs(jobs_str):
            jobs_lst = [job.strip() for job in jobs_str.split(',')]
            jobs = '\n    '.join(['- name: ' + job for job in jobs_lst])
            linkage = ' >> '.join(jobs_lst)
            linkage = '"' + linkage + '"'
            return jobs, linkage

        jobs_str, linkage_str = extract_jobs(self.jobs)
        f_lines = self.phs3.open_object_by_lines(dv.TEMPLATE_BUCKET, dv.CLI_VERSION + dv.TEMPLATE_PHDAG_FILE)
        with open(self.combine_path + "/phdag.yaml", "w") as file:
            for line in f_lines:
                line = line.replace("$name", self.name) \
                            .replace("$dag_owner", self.owner) \
                            .replace("$dag_tag", self.tag) \
                            .replace("$dag_timeout", self.timeout) \
                            .replace("$linkage", linkage_str) \
                            .replace("$jobs", jobs_str)
                file.write(line + "\n")

    def get_dag_py_file_name(self, key):
        return "ph_dag_" + key + ".py"

    def dag(self, **kwargs):
        """
        默认的DAG过程
        """
        self.logger.debug('maxauto 默认的 dag 实现')
        self.logger.debug(self.__dict__)

        self.check_path(self.dag_path)
        subprocess.call(["mkdir", "-p", self.dag_path])

        config = PhYAMLConfig(self.combine_path, "/phdag.yaml")
        config.load_yaml()

        def get_jobs_conf(config):
            def get_job_conf(name):
                job_full_path = self.project_path + self.job_prefix + name.replace('.', '/')
                if name.startswith('preset.'):
                    job_name = name.lstrip('preset.')
                    ipt_module = __import__('phcli.ph_max_auto.ph_preset_jobs.%s' % (job_name.lower()))
                    ipt_module = getattr(ipt_module, 'ph_max_auto')
                    ipt_module = getattr(ipt_module, 'ph_preset_jobs')
                    ipt_module = getattr(ipt_module, job_name)
                    phconf_buf = getattr(ipt_module, 'phconf_buf')

                    config = PhYAMLConfig()
                    config.load_yaml(phconf_buf(self))
                    return {
                        'name': config.metadata.name,
                        'ide': 'preset',
                        'runtime': config.spec.containers.runtime,
                        'command': config.spec.containers.command,
                        'timeout': config.spec.containers.timeout,
                    }
                elif os.path.isdir(job_full_path):
                    config = PhYAMLConfig(job_full_path)
                    config.load_yaml()
                    return {
                        'name': config.metadata.name,
                        'ide': 'c9',
                        'runtime': config.spec.containers.runtime,
                        'command': config.spec.containers.command,
                        'timeout': config.spec.containers.timeout,
                    }
                else:
                    raise Exception("{} job not found".format(name))

            result = {}
            for job in config.spec.jobs:
                result[job.name] = get_job_conf(job.name)
            return result

        def copy_jobs(jobs_conf):
            ide_dag_copy_job_func_table = {
                'c9': self.ide_table['c9'](**self.__dict__).dag_copy_job,
                'preset': preset_factory,
            }

            # 必须先 copy 所有非 preset 的 job
            for name, job_info in jobs_conf.items():
                if job_info['ide'] != 'preset':
                    func = ide_dag_copy_job_func_table[job_info['ide']]
                    func(job_name=name, **job_info)

            # 然后再 copy 所有 preset 的 job
            for name, job_info in jobs_conf.items():
                if job_info['ide'] == 'preset':
                    func = ide_dag_copy_job_func_table[job_info['ide']]
                    func(self, job_name=name, **job_info)

        def write_dag_pyfile(jobs_conf):
            timeout = config.spec.dag_timeout if config.spec.dag_timeout else sum([float(job['timeout']) for _, job in jobs_conf.items()])
            w = open(self.dag_path + self.get_dag_py_file_name(config.spec.dag_id), "a")
            f_lines = self.phs3.open_object_by_lines(dv.TEMPLATE_BUCKET, dv.CLI_VERSION + dv.TEMPLATE_PHGRAPHTEMP_FILE)
            for line in f_lines:
                line = line + "\n"
                w.write(
                    line.replace("$alfred_dag_owner", str(config.spec.owner)) \
                        .replace("$alfred_email_on_failure", str(config.spec.email_on_failure)) \
                        .replace("$alfred_email_on_retry", str(config.spec.email_on_retry)) \
                        .replace("$alfred_email", str(config.spec.email)) \
                        .replace("$alfred_retries", str(config.spec.retries)) \
                        .replace("$alfred_retry_delay", str(config.spec.retry_delay)) \
                        .replace("$alfred_dag_id", str(config.spec.dag_id)) \
                        .replace("$alfred_dag_tags", str(','.join(['"'+tag+'"' for tag in config.spec.dag_tag.split(',')]))) \
                        .replace("$alfred_schedule_interval", str(config.spec.schedule_interval)) \
                        .replace("$alfred_description", str(config.spec.description)) \
                        .replace("$alfred_dag_timeout", str(timeout)) \
                        .replace("$alfred_start_date", str(config.spec.start_date))
                )

            jf = self.phs3.open_object_by_lines(dv.TEMPLATE_BUCKET, dv.CLI_VERSION + dv.TEMPLATE_PHDAGJOB_FILE)
            for jt in config.spec.jobs:
                job_name = jt.name.replace('.', '_')

                for line in jf:
                    line = line + "\n"
                    w.write(
                        line.replace("$alfred_jobs_dir", str(self.name)) \
                            .replace("$alfred_name", str(job_name))
                    )

            for linkage in config.spec.linkage:
                w.write(linkage.replace('.', '_'))
                w.write("\n")

            w.close()

        jobs_conf = get_jobs_conf(config)
        copy_jobs(jobs_conf)
        write_dag_pyfile(jobs_conf)

    def publish(self, **kwargs):
        """
        默认的发布过程
        """
        self.logger.debug('maxauto 默认的 publish 实现')
        self.logger.debug(self.__dict__)

        def write_args(args_list, job_args):
            keys = []
            values = []
            # 获取list中的参数并 生成一个dict
            for arg in args_list:
                if args_list.index(arg) % 2 == 0:
                    keys.append(arg)
                elif args_list.index(arg) % 2 == 1:
                    values.append(arg)
            args = zip(keys, values)
            args_dict = dict(args)
            # 获取生成dict中的参数, 将传进来的dict进行替换
            for key in args_dict.keys():
                if key in job_args.keys():
                    args_dict[key] = job_args[key]
            args_dict_keys = list(args_dict)
            args_dict_values = list(args_dict.values())
            # 将替换好的dict转换成list
            final_args_list = []
            for i in range(len(args_dict_keys)):
                final_args_list.append(args_dict_keys[i])
                final_args_list.append(args_dict_values[i])
            return final_args_list

        # def write_data(jobs, definition, s3_dag_path, dag_path, excution_name, random_num=""):
        #     for job in jobs:
        #         args_path = dag_path +job + "/" + "args.properties"
        #         Parameters = {
        #             "ClusterId.$": "$.clusterId",
        #             "Step": {
        #                 "Name": "My EMR step",
        #                 "ActionOnFailure": "CONTINUE",
        #                 "HadoopJarStep": {
        #                     "Jar": "command-runner.jar",
        #                     "Args": '$.parameter.args'
        #                 }
        #             }
        #         }
        #         if job in definition['States'].keys():
        #             if jobs.index(job) + 1 == len(jobs):
        #                 definition['States'][job]['End'] = True
        #                 if not job.startswith('['):
        #                     definition['States'][job]['Type'] = "Task"
        #                     definition['States'][job]['Resource'] = Resource
        #                     # 替换args.properties中的参数
        #                     definition['States'][job]['Parameters'] = Parameters
        #                     definition['States'][job]['ResultPath'] = "$.firstStep"
        #                     definition['States'][job]['Retry'] = [
        #                                                             {
        #                                                               "ErrorEquals": [ "States.ALL" ],
        #                                                               "IntervalSeconds": 1,
        #                                                               "MaxAttempts": 1,
        #                                                               "BackoffRate": 1.0
        #                                                             }
        #                                                           ]
        #                     definition['States'][job+random_num] = definition['States'].pop(job)
        #             else:
        #
        #                 definition['States'][job]['Next'] = jobs[jobs.index(job) + 1] + random_num
        #                 if not job.startswith('['):
        #                     definition['States'][job]['Type'] = "Task"
        #                     definition['States'][job]['Resource'] = Resource
        #                     # 替换args.properties的参数
        #                     definition['States'][job]['Parameters'] = Parameters
        #                     definition['States'][job]['ResultPath'] = "$.firstStep"
        #                     definition['States'][job]['Retry'] = [
        #                         {
        #                             "ErrorEquals": ["States.ALL"],
        #                             "IntervalSeconds": 1,
        #                             "MaxAttempts": 3,
        #                             "BackoffRate": 2.0
        #                         }
        #                     ]
        #                     definition['States'][job + random_num] = definition['States'].pop(job)
        #     return definition

        # def create_parallel(states, job_name, s3_dag_path, dag_path, excution_name):
        #     if job_name.startswith('['):
        #         states[job_name] = {
        #             "Type": "Parallel",
        #             "Branches": []
        #         }
        #         # 取出[]中的并行的job
        #         if job_args["--g_month"]:
        #             job_args["--g_month"] = str(int(job_args["--g_month"]) - 1)
        #         for parallel_job in job_name.strip('[]').replace(' ', '').split(','):
        #             if job_args["--g_month"]:
        #                 job_args["--g_month"] = str(int(job_args["--g_month"]) + 1)
        #             random_num = "_" + str(uuid.uuid4())
        #             Parameters = {
        #                 "ClusterId.$": "$.clusterId",
        #                 "Step": {
        #                     "Name": "My EMR step",
        #                     "ActionOnFailure": "CONTINUE",
        #                     "HadoopJarStep": {
        #                         "Jar": "command-runner.jar",
        #                         "Args": ["spark-submit",
        #                                  "--deploy-mode", "cluster",
        #                                  "--py-files",
        #                                  "s3://ph-platform/2020-11-11/jobs/python/phcli/common/phcli-" + phcli_dv.CLI_CLIENT_VERSION + "-py3.8.egg," + s3_dag_path + parallel_job + "/phjob.py",
        #                                  s3_dag_path + parallel_job + "/phmain.py",
        #                                  "--owner", "default_owner",
        #                                  "--dag_name", s3_dag_path.split('/')[-2],
        #                                  "--run_id", s3_dag_path.split('/')[-2] + "_" + excution_name,
        #                                  "--job_full_name", parallel_job,
        #                                  "--job_id", "not_implementation",
        #                                  ]
        #                     }
        #                 }
        #             }
        #             step_tmp = {
        #                 "StartAt": "",
        #                 "States": {}
        #             }
        #             step_tmp.update({'StartAt': parallel_job + random_num})
        #             # 遍历除第一行主分支的策略
        #             for flow in flows:
        #                 # 如果有一行是以并行job开头，说明并行job还有延续，如果没有则States只有本身
        #                 if flow.startswith(parallel_job):
        #                     flow_jobs = []
        #                     # 根据 >> 进行分割
        #                     for flow_job_name in flow.split(' >> '):
        #                         flow_jobs.append(flow_job_name)
        #                         step_tmp['States'].update({flow_job_name: {}})
        #                         # 二次嵌套
        #                         # if flow_job_name.startswith('['):
        #                         #     flow_parallel_jobs = []
        #                         #     step_tmp['States'][flow_job_name] = {
        #                         #         "Type": "Parallel",
        #                         #         "Branches": []
        #                         #     }
        #                         #     for flow_parallel_job in flow_job_name.strip('[]').replace(' ', '').split(','):
        #                         #         flow_parallel_jobs.append(flow_parallel_job)
        #                         #         flow_step_tmp = {
        #                         #             "StartAt": "",
        #                         #             "States": {}
        #                         #         }
        #                         #         flow_step_tmp.update({'StartAt': flow_parallel_job})
        #                         #         for second_flow in flows:
        #                         #             if second_flow.startswith(flow_parallel_job):
        #                         #                 second_flow_jobs = []
        #                         #                 # 根据 >> 进行分割
        #                         #                 for second_flow_job_name in second_flow.split(' >> '):
        #                         #                     second_flow_jobs.append(second_flow_job_name)
        #                         #                     flow_step_tmp['States'].update({second_flow_job_name: {}})
        #                         #                 write_data(second_flow_jobs, flow_step_tmp, s3_dag_path, dag_path, excution_name)
        #                         #         if flow_step_tmp['States'] == {}:
        #                         #             flow_step_tmp['States'].update({flow_step_tmp['StartAt']: {'End': True}})
        #                         #             flow_step_tmp['States'][flow_step_tmp['StartAt']]['Type'] = "Task"
        #                         #             flow_step_tmp['States'][flow_step_tmp['StartAt']]['Resource'] = Resource
        #                         #             flow_step_tmp['States'][flow_step_tmp['StartAt']]['Parameters'] = Parameters
        #                         #             flow_step_tmp['States'][flow_step_tmp['StartAt']][
        #                         #                 'ResultPath'] = "$.firstStep"
        #                         #         step_tmp['States'][flow_job_name]['Branches'].append(flow_step_tmp)
        #                         #     write_data(flow_parallel_jobs, flow_step_tmp, s3_dag_path, dag_path, excution_name)
        #                     write_data(flow_jobs, step_tmp, s3_dag_path, dag_path, excution_name, random_num)
        #
        #             # 判断States是否为空 如果为空 说明没有其他延续 则本身作为States
        #             if step_tmp['States'] == {}:
        #                 step_tmp['States'].update({step_tmp['StartAt']: {'End': True}})
        #                 step_tmp['States'][step_tmp['StartAt']]['Type'] = "Task"
        #                 step_tmp['States'][step_tmp['StartAt']]['Resource'] = Resource
        #                 step_tmp['States'][step_tmp['StartAt']]['Parameters'] = Parameters
        #                 step_tmp['States'][step_tmp['StartAt']]['ResultPath'] = "$.firstStep"
        #             states[job_name]['Branches'].append(step_tmp)
        #
        #     return states

        def create_args_lambda(dag_name, job_name, dag_path, owner):
            lmd_path = dag_path + dag_name + "_" + job_name + ".py"
            lmd_zip_path = dag_path+dag_name + "_" + job_name +".zip"
            w = open(lmd_path, "a")
            f_lines = self.phs3.open_object_by_lines(dv.TEMPLATE_BUCKET, dv.CLI_VERSION + dv.TEMPLATE_SFN_CREATE_LMD_FILE)
            for line in f_lines:
                line = line + "\n"
                w.write(
                    line.replace("$alfred_dag_name", dag_name) \
                        .replace("$alfred_job_name", job_name) \
                        .replace("$alfred_owner", owner)
                )
            zip_cmd = "zip -r "+ lmd_zip_path + " " + lmd_path
            status = os.system(zip_cmd)
            self.phs3.upload(lmd_zip_path, dv.TEMPLATE_BUCKET, dv.CLI_VERSION + dv.DAGS_S3_LMD_PTAH + dag_name + "/"+dag_name + "_" + job_name +".zip")
            rm_lmd_cmd = "rm -f " + lmd_path
            rm_lmd_zip_cmd = "rm -f " + lmd_zip_path
            os.system(rm_lmd_cmd)
            os.system(rm_lmd_zip_cmd)

        def create_parallel_step(job_full_name):
            # 通过模板创建Parallel模型
            parallel_branches = []
            parallel_step = self.phs3.open_object(dv.TEMPLATE_BUCKET, dv.CLI_VERSION + dv.TEMPLATE_SFN_PARALLEL_STEP_FILE)

            # 替换模板里的字段
            json.loads(parallel_step.replace("$next_job_full_name", "\"$next_job_full_name\""))
            # 先进行逗号分割
            parallel_task_list = job_full_name.strip('[]').replace(' ', '').split(',')
            for parallel_task in parallel_task_list:
                # 每个parallel_task一个randoms
                randoms = []
                flows = []
                # 对每个 parallel_task 以 >> 分割
                print(parallel_task)
                for parallel_job_name in parallel_task.split('>>'):
                    print(parallel_job_name)
                    flows.append(parallel_job_name)
                    snowflake_id = IdWorker(1, 2, 0)
                    randoms.append(str(snowflake_id.get_id()))
                parallel_step = create_step(self.name, flows, randoms)
                parallel_branches.append(parallel_step)
            # print(parallel_branches)
            return parallel_branches

        def create_step(dag_name, job_full_names, randoms):
            definition = {
                        "StartAt": "",
                        "States": {}
                    }
            definition['StartAt'] = "args_" + job_full_names[0] + "_" +randoms[0]

            for current_index in range(len(job_full_names)):
                # 当前索引 job_full_name
                job_full_name = job_full_names[current_index]
                current_random = randoms[current_index]

                # 处理step的args定义
                if not job_full_name.startswith('['):
                    dag_args_step = self.phs3.open_object(dv.TEMPLATE_BUCKET, dv.CLI_VERSION + dv.TEMPLATE_SFN_LMD_STEP_FILE)
                    json_args = json.dumps(dag_args_step)
                    args_step = json_args.replace("$dag_name", dag_name) \
                                        .replace("$job_full_name", job_full_name + "_" + current_random)\
                                        .replace("$lmd_job_name", job_full_name)
                    dict_args = eval(json.loads(args_step))
                    definition['States'].update(dict_args)

                # 处理step的定义
                dag_step = self.phs3.open_object(dv.TEMPLATE_BUCKET, dv.CLI_VERSION + dv.TEMPLATE_SFN_STEP_FILE)
                json_step = json.dumps(dag_step)

                # 查询下一个step，如果没有则下一步为End
                # 如果当前job_full_name是list中最后一个元素
                if current_index == len(job_full_names) - 1:
                    # 判断是不是以'[' 开头，如果不是则不是并行的
                    if job_full_name.startswith('['):
                        dag_step = self.phs3.open_object(dv.TEMPLATE_BUCKET, dv.CLI_VERSION + dv.TEMPLATE_SFN_PARALLEL_STEP_FILE)
                        json_step = json.dumps(dag_step)
                        dag_step = json_step.replace("$dag_name", dag_name)\
                            .replace("$next_job_full_name", "True")\
                            .replace("$next_type", "End")\
                            .replace("$job_full_name", job_full_name + "_" + current_random)
                    else:
                        dag_step = json_step.replace("$dag_name", dag_name)\
                            .replace("$next_job_full_name", "True")\
                            .replace("$next_type", "End")\
                            .replace("$job_full_name", job_full_name + "_" + current_random)

                else:
                    # 如果不是 获取list中下一个job_full_name
                    next_index = current_index + 1
                    next_job_full_name = job_full_names[next_index]
                    next_random = randoms[next_index]
                    if job_full_name.startswith('['):
                        print(1)
                        dag_step = self.phs3.open_object(dv.TEMPLATE_BUCKET,
                                                         dv.CLI_VERSION + dv.TEMPLATE_SFN_PARALLEL_STEP_FILE)
                        dag_step = json.loads(dag_step.replace("$next_job_full_name", "\"$next_job_full_name\""))
                        parallel_branches = create_parallel_step(job_full_name)
                        print(2)
                        dag_step['$job_full_name']['Branches'] = parallel_branches
                        print(3)
                        json_step = json.dumps(dag_step)
                        dag_step = json_step.replace("$dag_name", dag_name) \
                            .replace("$next_job_full_name", "args_" + next_job_full_name + "_" + next_random) \
                            .replace("$next_type", "Next") \
                            .replace("$job_full_name", job_full_name + "_" + current_random)
                        # 传递进来的parallel 分割返回 parallel dag_step
                    else:
                        if next_job_full_name.startswith('['):
                            dag_step = json_step.replace("$dag_name", dag_name) \
                                .replace("$next_job_full_name", "\\\"" + next_job_full_name + "_" + next_random + "\\\"" ) \
                                .replace("$next_type", "Next")\
                                .replace("$job_full_name", job_full_name + "_" + current_random)
                        else:
                            dag_step = json_step.replace("$dag_name", dag_name) \
                                .replace("$next_job_full_name", "\\\"args_" + next_job_full_name + "_" + next_random + "\\\"" ) \
                                .replace("$next_type", "Next")\
                                .replace("$job_full_name", job_full_name + "_" + current_random)

                if type(json.loads(dag_step)) == str:
                    dict_dag_step = eval(json.loads(dag_step))
                else:
                    dict_dag_step = json.loads(dag_step)
                definition['States'].update(dict_dag_step)
            print(json.dumps(definition))
            return definition


        if self.strategy == "v2":
            for key in os.listdir(self.dag_path):
                if os.path.isfile(self.dag_path + key):
                    self.phs3.upload(
                        file=self.dag_path+key,
                        bucket_name=dv.DAGS_S3_BUCKET,
                        object_name=dv.DAGS_S3_PREV_PATH + key
                    )
                else:
                    self.phs3.upload_dir(
                        dir=self.dag_path+key,
                        bucket_name=dv.TEMPLATE_BUCKET,
                        s3_dir=dv.CLI_VERSION + dv.DAGS_S3_PHJOBS_PATH + self.name + "/" + key
                    )

        if self.strategy == "v3":
            # print(self.name)

            excution_name = self.name + "_" + time.strftime("%Y-%m-%d_%H-%M-%S", time.localtime())
            # print(excution_name)
            # job_full_names = []
            # for key in os.listdir(self.dag_path):
            #     if os.path.isdir(self.dag_path + key):
            #         job_full_names.append(key)
            # print(job_full_names)
            for key in os.listdir(self.dag_path):
                # if os.path.isfile(self.dag_path + key):
                #     pass
                #     self.phs3.upload(
                #         file=self.dag_path+key,
                #         bucket_name=dv.DAGS_S3_BUCKET,
                #         object_name=dv.DAGS_S3_PREV_PATH + key
                #     )
                # else:
                #     # 上传dag
                #     self.phs3.upload_dir(
                #         dir=self.dag_path+key,
                #         bucket_name=dv.TEMPLATE_BUCKET,
                #         s3_dir=dv.CLI_VERSION + dv.DAGS_S3_PHJOBS_PATH + self.name + "/" + key
                #     )
                #     # 上传args_lmd代码
                #     create_args_lambda(self.name, key, self.dag_path, self.owner)

                s3_dag_path = "s3://" + dv.TEMPLATE_BUCKET + "/" + dv.CLI_VERSION + dv.DAGS_S3_PHJOBS_PATH + self.name + "/"
                if os.path.isfile(self.dag_path + key):
                    # 如果是 file 则为 dag 产生的 py文件， 判断文件最后一行设置的策略 创建step流程模板
                    # 策略的每一行，存放到每一个列表
                    flows = []
                    # 第一行中每一个job的名称
                    first_flow = []
                    cp_first_flow = []
                    # 每个job对应一个随机数
                    randoms = []
                    # 状态机的所有states
                    states = {}
                    with open(self.dag_path + key, "r") as dag_file:
                        line = dag_file.readline()
                        while line:
                            while ">>" in line:
                                flows.append(line.rstrip('\n'))
                                break
                            line = dag_file.readline()

                    # 生成对应的random
                    for job_name in flows[0].split(' >> '):
                        first_flow.append(job_name)
                        cp_first_flow.append(job_name)
                        snowflake_id = IdWorker(1, 2, 0)
                        randoms.append(str(snowflake_id.get_id()))
                    # 处理整个dag的结构
                    # 如果 first_flow 是以'['开头 则是个并行的job， 然后遍历[]中的job_full_name, 如果flows中有以job_full_name开头的flow
                    # 则将flow替换job_full_name
                    dag_flow = flows[0]
                    for first_flow_job in first_flow:
                        # 如果是以'['开始则为并行的job
                        if first_flow_job.startswith('['):
                            parallel_list = first_flow_job.strip('[]').replace(' ', '').split(',')
                            # 遍历并行的job 查询并行的job是否有分支
                            for parallel_job in parallel_list:
                                parallel_job_index = first_flow.index(first_flow_job)
                                for flow in flows:
                                    if flow.startswith(parallel_job):
                                        cp_first_flow[parallel_job_index] = cp_first_flow[parallel_job_index].replace(parallel_job, flow)

                    create_step(self.name, cp_first_flow, randoms)

                    #     # 若果是以"[" 开头的job 对并行的job进行操作
                    #     if len(job_name) > 60:
                    #         states[job_name[:59]] = states.pop(job_name)
                    #         jobs = [job_name[:59] if i == job_name else i for i in jobs]

                    # step_client = boto3.client('stepfunctions')
                    # response = step_client.create_state_machine(
                    #     name=self.name,
                    #     definition=create_definition,
                    #     roleArn=os.getenv("DEFAULT_ROLE_ARN"),
                    #     type=dv.DEFAULT_MACHINE_TYPE,
                    # )
                    #
                    # ssm_client = boto3.client('ssm')
                    # ssm_response = ssm_client.get_parameter(
                    #     Name='cluster_id'
                    # )
                    #
                    # machine_input = {
                    #     'clusterId': ssm_response['Parameter']['Value']
                    # }
                    # step_client.start_execution(
                    #     stateMachineArn=response['stateMachineArn'],
                    #     name=excution_name,
                    #     input=json.dumps(machine_input)
                    # )

    def recall(self, **kwargs):
        """
        默认的召回过程
        """
        self.logger.debug('maxauto 默认的 recall 实现')
        self.logger.debug(self.__dict__)

        self.phs3.delete_dir(dv.TEMPLATE_BUCKET, dv.CLI_VERSION + dv.DAGS_S3_PHJOBS_PATH + self.name)
        self.phs3.delete_dir(dv.DAGS_S3_BUCKET, dv.DAGS_S3_PREV_PATH + self.get_dag_py_file_name(self.name))

    def online_run(self, **kwargs):
        """
        默认的 online_run 过程
        """
        self.logger.debug('maxauto 默认的 online_run 实现')
        self.logger.debug(self.__dict__)
        def write_data(definition, s3_dag_path, job_name, excution_name, job_args_name_list):
            Resource = "arn:aws-cn:states:::elasticmapreduce:addStep.sync"
            Parameters = {
                "ClusterId.$": "$.clusterId",
                "Step": {
                    "Name": "My EMR step",
                    "ActionOnFailure": "CONTINUE",
                    "HadoopJarStep": {
                        "Jar": "command-runner.jar",
                        "Args": ["spark-submit",
                                 "--name", s3_dag_path.split('/')[-2],
                                 "--deploy-mode", "cluster",
                                 "--conf", "spark.driver.cores=1",
                                 "--conf", "spark.driver.memory=1g",
                                 "--conf", "spark.executor.cores=1",
                                 "--conf", "spark.executor.memory=4g",
                                 "--conf", "spark.executor.instances=1",
                                 "--conf", "spark.sql.autoBroadcastJoinThreshold=-1"
                                 "--py-files",
                                 "s3://ph-platform/2020-11-11/jobs/python/phcli/common/phcli-" + phcli_dv.CLI_CLIENT_VERSION + "-py3.8.egg," + s3_dag_path + "phjob.py",
                                 s3_dag_path + "phmain.py",
                                 "--owner", "default_owner",
                                 "--dag_name", s3_dag_path.split('/')[-2],
                                 "--run_id", s3_dag_path.split('/')[-2] + "_" + excution_name,
                                 "--job_full_name", job_name,
                                 "--job_id", "not_implementation"
                                 ]
                    }
                }
            }
            definition['States'][job_name]['End'] = True
            definition['States'][job_name]['Type'] = "Task"
            definition['States'][job_name]['Resource'] = Resource
            Parameters['Step']['HadoopJarStep']['Args'][len(Parameters['Step']['HadoopJarStep']['Args'])
                                                        :len(
                Parameters['Step']['HadoopJarStep']['Args'])] = job_args_name_list
            definition['States'][job_name]['Parameters'] = Parameters
            definition['States'][job_name]['ResultPath'] = "$.firstStep"
            definition['States'][job_name]['Retry'] = [
                {
                    "ErrorEquals": ["States.ALL"],
                    "IntervalSeconds": 1,
                    "MaxAttempts": 1,
                    "BackoffRate": 1.0
                }
            ]
            definition['States'][job_name] = definition['States'].pop(job_name)

        def write_args(args_list, job_args):
            keys = []
            values = []
            # 获取list中的参数并 生成一个dict
            for arg in args_list:
                if args_list.index(arg) % 2 == 0:
                    keys.append(arg)
                elif args_list.index(arg) % 2 == 1:
                    values.append(arg)
            args = zip(keys, values)

            args_dict = dict(args)
            # 获取生成dict中的参数, 将传进来的dict进行替换
            for key in args_dict.keys():
                if key.lstrip('--') in job_args.keys():
                    args_dict[key] = job_args[key.lstrip('--')]

            new_key = []
            for args_key in args_dict.keys():
                new_key.append(args_key.lstrip('--'))
            new_args = dict(zip(new_key, args_dict.values()))
            return new_args

        def ast_parse(string):
            """
            解析json
            :param string: json 字符串
            :return: dict
            """
            ast_dict = {}
            if string != "":
                ast_dict = ast.literal_eval(string.replace(" ", ""))
                for k, v in ast_dict.items():
                    if isinstance(v, str) and v.startswith('{') and v.endswith('}'):
                        ast_dict[k] = ast.literal_eval(v)
            return ast_dict

        # airflow 运行
        self.context = ast_parse(self.context)
        self.args = ast_parse(self.args)

        self.s3_job_path = dv.DAGS_S3_PHJOBS_PATH + self.dag_name + "/" + self.job_full_name
        self.submit_prefix = "s3a://" + dv.TEMPLATE_BUCKET + "/" + dv.CLI_VERSION + self.s3_job_path + "/"

        # stream = self.phs3.open_object(dv.TEMPLATE_BUCKET, dv.CLI_VERSION + self.s3_job_path + "/phconf.yaml")
        # config = PhYAMLConfig()
        # config.load_yaml(stream)
        # self.runtime = config.spec.containers.runtime
        # self.command = config.spec.containers.command
        # self.timeout = config.spec.containers.timeout
        #
        # runtime_inst = self.table_driver_runtime_inst(self.runtime)
        # runtime_inst(**self.__dict__).online_run()


        # step functions 运行
        s3_dag_path = "s3://" + dv.TEMPLATE_BUCKET + "/" + dv.CLI_VERSION + self.s3_job_path + "/"
        job_name = self.name
        definition = {
            "StartAt": "",
            "States": {}
        }
        states = {}
        states[job_name] = {}
        definition['StartAt'] = job_name
        definition['States'] = states
        random_num = "_" + str(uuid.uuid4())
        job_args_name = 'args' + random_num
        # 将 job_args_name 写成list
        job_args_name_list = ['--job_args_name',job_args_name]
        excution_name = self.name + "_" + time.strftime("%Y-%m-%d_%H-%M-%S", time.localtime())
        job_args = self.args

        args_list = self.phs3.open_object_by_lines(dv.TEMPLATE_BUCKET,
                                                   dv.CLI_VERSION + self.s3_job_path + "/args.properties")

        new_args_list = []
        for arg in args_list:
            if arg.startswith('s3a'):
                arg = arg.replace('s3a:', 's3:')
            new_args_list.append(arg)
        args_dict = write_args(new_args_list, job_args)
        write_data(definition, s3_dag_path, job_name, excution_name, job_args_name_list)

        create_definition = json.dumps(definition)

        step_client = boto3.client('stepfunctions')
        # 创建状态机
        print(args_dict)
        step_create_response = step_client.create_state_machine(
            name=self.name + random_num,
            definition=create_definition,
            roleArn=dv.DEFAULT_ROLE_ARN,
            type=dv.DEFAULT_MACHINE_TYPE,
        )
        # 获取cluster_id
        ssm_client = boto3.client('ssm')
        ssm_response = ssm_client.get_parameter(
            Name='cluster_id'
        )
        machine_input = {
            'clusterId': ssm_response['Parameter']['Value']
        }
        ssm_response = ssm_client.get_parameter(
            Name='cluster_id'
        )

        # 將kwargs写入ssm
        ssm_client.put_parameter(
            Name=job_args_name,
            Value=str(args_dict),
            Type='String'
        )

        # 启动状态机
        start_response = step_client.start_execution(
            stateMachineArn=step_create_response['stateMachineArn'],
            name=excution_name,
            input=json.dumps(machine_input)
        )

        # 获取状态机的执行状态 直到成功退出
        execution_response = step_client.list_executions(
            stateMachineArn=step_create_response['stateMachineArn'],
        )

        while execution_response:
            time.sleep(60)
            if len(execution_response['executions']) == 0:
                execution_response = step_client.list_executions(
                    stateMachineArn=step_create_response['stateMachineArn'],
                )
                continue

            execution_response = step_client.list_executions(
                stateMachineArn=step_create_response['stateMachineArn'],
            )
            execution_status = execution_response['executions'][0]['status']
            if execution_status == 'SUCCEEDED':
                step_client.delete_state_machine(
                    stateMachineArn=step_create_response['stateMachineArn']
                )
                break
            if execution_status == 'FAILED':
                step_client.delete_state_machine(
                    stateMachineArn=step_create_response['stateMachineArn']
                )
                raise Exception("Job运行失败")
                break

        return 0

    def status(self, **kwargs):
        """
        默认的查看运行状态
        """
        self.logger.debug('maxauto 默认的 status 实现')
        self.logger.debug(self.__dict__)

