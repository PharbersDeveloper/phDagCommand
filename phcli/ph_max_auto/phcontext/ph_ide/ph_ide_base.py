import os
import sys
import ast
import subprocess
import boto3
import json
import time
from enum import Enum

from phcli.ph_errs.ph_err import *
from phcli.ph_max_auto import define_value as dv
from phcli.ph_max_auto.ph_config.phconfig.phconfig import PhYAMLConfig
from phcli.ph_max_auto.ph_preset_jobs.preset_job_factory import preset_factory



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

        def write_data(jobs, definition, s3_dag_path, dag_path, excution_name):
            for job in jobs:
                args_path = dag_path + "/" +job + "/" + "args.properties"
                Parameters = {
                    "ClusterId.$": "$.clusterId",
                    "Step": {
                        "Name": "My EMR step",
                        "ActionOnFailure": "CONTINUE",
                        "HadoopJarStep": {
                            "Jar": "command-runner.jar",
                            "Args": ["spark-submit",
                                     "--deploy-mode", "cluster",
                                     "--py-files",
                                     "s3://ph-platform/2020-11-11/jobs/python/phcli/common/phcli-3.0.1-py3.8.egg," + s3_dag_path + job + "/phjob.py",
                                     s3_dag_path + job + "/phmain.py",
                                     "--owner", "default_owner",
                                     "--dag_name", s3_dag_path.split('/')[-2],
                                     "--run_id", s3_dag_path.split('/')[-2] + "_" +excution_name,
                                     "--job_full_name", job,
                                     "--job_id", "not_implementation",
                                     ]
                        }
                    }
                }
                if job in definition['States'].keys():
                    if jobs.index(job) + 1 == len(jobs):
                        definition['States'][job]['End'] = True
                        if not job.startswith('['):
                            definition['States'][job]['Type'] = "Task"
                            definition['States'][job]['Resource'] = Resource
                            with open(args_path, "r") as args_file:
                                arg_line = args_file.readline()
                                while arg_line:
                                    Parameters['Step']['HadoopJarStep']['Args'].append(arg_line.rstrip('\n'))
                                    arg_line = args_file.readline()
                            definition['States'][job]['Parameters'] = Parameters
                            definition['States'][job]['ResultPath'] = "$.firstStep"
                    else:
                        definition['States'][job]['Next'] = jobs[jobs.index(job) + 1]
                        if not job.startswith('['):
                            definition['States'][job]['Type'] = "Task"
                            definition['States'][job]['Resource'] = Resource
                            with open(args_path, "r") as args_file:
                                arg_line = args_file.readline()
                                while arg_line:
                                    Parameters['Step']['HadoopJarStep']['Args'].append(arg_line.rstrip('\n'))
                                    arg_line = args_file.readline()
                            definition['States'][job]['Parameters'] = Parameters
                            definition['States'][job]['ResultPath'] = "$.firstStep"
            return definition

        def create_parallel(states, job_name, s3_dag_path, dag_path, excution_name):
            if job_name.startswith('['):
                states[job_name] = {
                    "Type": "Parallel",
                    "Branches": []
                }
                # 取出[]中的并行的job
                for parallel_job in job_name.strip('[]').replace(' ', '').split(','):
                    step_tmp = {
                        "StartAt": "",
                        "States": {}
                    }
                    step_tmp.update({'StartAt': parallel_job})
                    # 遍历除第一行主分支的策略
                    for flow in flows:
                        # 如果有一行是以并行job开头，说明并行job还有延续，如果没有则States只有本身
                        if flow.startswith(parallel_job):
                            flow_jobs = []
                            # 根据 >> 进行分割
                            for flow_job_name in flow.split(' >> '):
                                flow_jobs.append(flow_job_name)
                                step_tmp['States'].update({flow_job_name: {}})
                                # 二次嵌套
                                if flow_job_name.startswith('['):
                                    flow_parallel_jobs = []
                                    step_tmp['States'][flow_job_name] = {
                                        "Type": "Parallel",
                                        "Branches": []
                                    }
                                    for flow_parallel_job in flow_job_name.strip('[]').replace(' ', '').split(','):
                                        flow_parallel_jobs.append(flow_parallel_job)
                                        flow_step_tmp = {
                                            "StartAt": "",
                                            "States": {}
                                        }
                                        flow_step_tmp.update({'StartAt': flow_parallel_job})
                                        for second_flow in flows:
                                            if second_flow.startswith(flow_parallel_job):
                                                second_flow_jobs = []
                                                # 根据 >> 进行分割
                                                for second_flow_job_name in second_flow.split(' >> '):
                                                    second_flow_jobs.append(second_flow_job_name)
                                                    flow_step_tmp['States'].update({second_flow_job_name: {}})
                                                write_data(second_flow_jobs, flow_step_tmp, s3_dag_path, dag_path, excution_name)
                                        if flow_step_tmp['States'] == {}:
                                            flow_step_tmp['States'].update({flow_step_tmp['StartAt']: {'End': True}})
                                            flow_step_tmp['States'][flow_step_tmp['StartAt']]['Type'] = "Task"
                                            flow_step_tmp['States'][flow_step_tmp['StartAt']]['Resource'] = Resource
                                            flow_step_tmp['States'][flow_step_tmp['StartAt']]['Parameters'] = Parameters
                                            flow_step_tmp['States'][flow_step_tmp['StartAt']][
                                                'ResultPath'] = "$.firstStep"
                                        step_tmp['States'][flow_job_name]['Branches'].append(flow_step_tmp)
                                    write_data(flow_parallel_jobs, flow_step_tmp, s3_dag_path, dag_path, excution_name)
                            write_data(flow_jobs, step_tmp, s3_dag_path, dag_path, excution_name)
                    # 判断States是否为空 如果为空 说明没有其他延续 则本身作为States
                    if step_tmp['States'] == {}:
                        step_tmp['States'].update({step_tmp['StartAt']: {'End': True}})
                        step_tmp['States'][step_tmp['StartAt']]['Type'] = "Task"
                        step_tmp['States'][step_tmp['StartAt']]['Resource'] = Resource
                        step_tmp['States'][step_tmp['StartAt']]['Parameters'] = Parameters
                        step_tmp['States'][step_tmp['StartAt']]['ResultPath'] = "$.firstStep"
                    states[job_name]['Branches'].append(step_tmp)
            return states

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
            excution_name = self.name + "_" + time.strftime("%Y-%m-%d_%H-%M-%S", time.localtime())
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
                s3_dag_path = "s3://" + dv.TEMPLATE_BUCKET + "/" + dv.CLI_VERSION + dv.DAGS_S3_PHJOBS_PATH + self.name + "/"
                dag_path = self.dag_path
                if os.path.isfile(self.dag_path + key):
                    # Parameters = {
                    #     "ClusterId.$": "$.clusterId",
                    #     "Step": {
                    #         "Name": "My second EMR step",
                    #         "ActionOnFailure": "CONTINUE",
                    #         "HadoopJarStep": {
                    #             "Jar": "command-runner.jar",
                    #             "Args": ["spark-submit",
                    #                      "--deploy-mode", "cluster",
                    #                      "--py-files",
                    #                      "s3://ph-platform/2020-11-11/jobs/python/phcli/common/phcli-2.2.1-py3.8.egg," + s3_dag_path + "/phjob.py",
                    #                      s3_dag_path + "/phmain.py",
                    #                      "--owner", "$.owner",
                    #                      "--dag_name", "$.dag_name",
                    #                      "--run_id", "$.run_id",
                    #                      "--job_full_name", "$.job_full_name",
                    #                      "--job_id", "$.job_id",
                    #                      ]
                    #         }
                    #     }
                    # }
                    Resource = "arn:aws-cn:states:::elasticmapreduce:addStep.sync"
                    # 如果是 file 则为 dag 产生的 py文件， 判断文件最后一行设置的策略 创建step流程模板
                    # 策略的每一行，存放到每一个列表
                    flows = []
                    # 一行中每一个job的名称
                    jobs = []
                    # 状态机的所有states
                    states = {}
                    with open(self.dag_path + key, "r") as dag_file:
                        line = dag_file.readline()
                        while line:
                            while ">>" in line:
                                flows.append(line.rstrip('\n'))
                                break
                            line = dag_file.readline()
                    for job_name in flows[0].split(' >> '):
                        states[job_name] = {}
                        jobs.append(job_name)
                        # 若果是以"[" 开头的job 对并行的job进行操作
                        states = create_parallel(states, job_name, s3_dag_path, dag_path, excution_name)
                    definition = {
                        "StartAt": "",
                        "States": {}
                    }
                    definition['StartAt'] = list(states.keys())[0]
                    definition['States'] = states
                    write_data(jobs, definition, s3_dag_path, dag_path, excution_name)
                    create_definition = json.dumps(definition)
                    client = boto3.client('stepfunctions')
                    response = client.create_state_machine(
                        name=self.name,
                        definition=create_definition,
                        roleArn=os.getenv("DEFAULT_ROLE_ARN"),
                        type=dv.DEFAULT_MACHINE_TYPE,
                    )
                    machine_input = {
                        'clusterId': self.cluster_id
                    }
                    client.start_execution(
                        stateMachineArn=response['stateMachineArn'],
                        name=excution_name,
                        input=json.dumps(machine_input)
                    )


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

        self.context = ast_parse(self.context)
        self.args = ast_parse(self.args)

        self.s3_job_path = dv.DAGS_S3_PHJOBS_PATH + self.dag_name + "/" + self.job_full_name
        self.submit_prefix = "s3a://" + dv.TEMPLATE_BUCKET + "/" + dv.CLI_VERSION + self.s3_job_path + "/"

        stream = self.phs3.open_object(dv.TEMPLATE_BUCKET, dv.CLI_VERSION + self.s3_job_path + "/phconf.yaml")
        config = PhYAMLConfig()
        config.load_yaml(stream)
        self.runtime = config.spec.containers.runtime
        self.command = config.spec.containers.command
        self.timeout = config.spec.containers.timeout

        runtime_inst = self.table_driver_runtime_inst(self.runtime)
        runtime_inst(**self.__dict__).online_run()

    def status(self, **kwargs):
        """
        默认的查看运行状态
        """
        self.logger.debug('maxauto 默认的 status 实现')
        self.logger.debug(self.__dict__)
