import os
import sys
import ast
import subprocess
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
