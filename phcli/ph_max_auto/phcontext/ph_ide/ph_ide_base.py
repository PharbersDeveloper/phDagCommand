import os
import ast
import subprocess

from phcli.ph_errs.ph_err import *
from phcli.ph_max_auto import define_value as dv
from phcli.ph_max_auto.ph_config.phconfig.phconfig import PhYAMLConfig
from phcli.ph_max_auto.ph_preset_jobs.preset_job_factory import preset_factory


class PhIDEBase(object):
    job_prefix = "/phjobs/"
    combine_prefix = "/phcombines/"
    dag_prefix = "/phdags/"
    upload_prefix = "/upload/"

    def get_workspace_dir(self):
        return os.getenv(dv.ENV_WORKSPACE_KEY, dv.ENV_WORKSPACE_DEFAULT)

    def get_current_project_dir(self):
        return os.getenv(dv.ENV_CUR_PROJ_KEY, dv.ENV_CUR_PROJ_DEFAULT)

    def get_absolute_path(self):
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
        if os.path.exists(path):
            override = input('Job "' + path + '" already existed，want to override？(Y/*)')
            if override.upper() == 'Y':
                subprocess.call(["rm", "-rf", path])
            else:
                raise exception_file_already_exist

    def table_driver_runtime_main_code(self, runtime):
        table = {
            "python3": "phmain.py",
            "r": "phmain.R"
        }
        return table[runtime]

    def table_driver_runtime_inst(self, runtime):
        from ..ph_runtime.ph_rt_python3 import PhRTPython3
        from ..ph_runtime.ph_rt_r import PhRTR
        table = {
            "python3": PhRTPython3,
            "r": PhRTR,
        }
        return table[runtime]

    def table_driver_runtime_binary(self, runtime):
        table = {
            "bash": "/bin/bash",
            "python3": "python3",
            "r": "Rscript",
        }
        return table[runtime]

    def create(self, **kwargs):
        """
        默认的创建过程
        """
        self.logger.info('maxauto 默认的 create 实现')
        self.logger.debug(self.__dict__)

    def run(self, **kwargs):
        """
        默认的运行过程
        """
        self.logger.info('maxauto 默认的 run 实现')
        self.logger.debug(self.__dict__)

    def combine(self, **kwargs):
        """
        默认的关联过程
        """
        self.logger.info('maxauto 默认的 combine 实现')
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
                            .replace("$linkage", linkage_str) \
                            .replace("$jobs", jobs_str)
                file.write(line + "\n")

    def dag(self, **kwargs):
        """
        默认的DAG过程
        """
        self.logger.info('maxauto 默认的 dag 实现')
        self.logger.debug(self.__dict__)

        self.check_path(self.dag_path)
        subprocess.call(["mkdir", "-p", self.dag_path])

        config = PhYAMLConfig(self.combine_path, "/phdag.yaml")
        config.load_yaml()

        def write_dag_pyfile():
            w = open(self.dag_path + "ph_dag_" + config.spec.dag_id + ".py", "a")
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
                        .replace("$alfred_dag_timeout", str(config.spec.dag_timeout)) \
                        .replace("$alfred_start_date", str(config.spec.start_date))
                )

            jf = self.phs3.open_object_by_lines(dv.TEMPLATE_BUCKET, dv.CLI_VERSION + dv.TEMPLATE_PHDAGJOB_FILE)
            for jt in config.spec.jobs:
                job_name = jt.name.replace('.', '_')

                for line in jf:
                    line = line + "\n"
                    w.write(
                        line.replace("$alfred_command", str(jt.command)) \
                            .replace("$alfred_job_path", str(self.job_path)) \
                            .replace("$alfred_dag_owner", str(config.spec.owner)) \
                            .replace("$alfred_jobs_dir", str(self.name)) \
                            .replace("$alfred_name", job_name) \
                            .replace("$runtime", str(jt.command))
                    )

            for linkage in config.spec.linkage:
                w.write(linkage.replace('.', '_'))
                w.write("\n")

            w.close()

        def copy_preset_jobs():
            for jt in config.spec.jobs:
                if not jt.name.startswith('preset'):
                    continue
                preset_factory(self, jt.name)

        kwargs['copy_func'](config=config)
        write_dag_pyfile()
        copy_preset_jobs()

    def publish(self, **kwargs):
        """
        默认的发布过程
        """
        self.logger.info('maxauto 默认的 publish 实现')
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

    def online_run(self, **kwargs):
        """
        默认的 online_run 过程
        """
        self.logger.info('maxauto 默认的 online_run 实现')
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

        group = self.group + "/" if self.group else ''
        self.s3_job_path = dv.DAGS_S3_PHJOBS_PATH + group + self.name
        self.submit_prefix = "s3a://" + dv.TEMPLATE_BUCKET + "/" + dv.CLI_VERSION + self.s3_job_path + "/"

        stream = self.phs3.open_object(dv.TEMPLATE_BUCKET, dv.CLI_VERSION + self.s3_job_path + "/phconf.yaml")
        config = PhYAMLConfig()
        config.load_yaml(stream)
        self.runtime = config.spec.containers.runtime
        self.command = config.spec.containers.command

        runtime_inst = self.table_driver_runtime_inst(self.runtime)
        runtime_inst(phs3=self.phs3, **self.__dict__).online_run()

    def status(self, **kwargs):
        """
        默认的查看运行状态
        """
        self.logger.info('maxauto 默认的 status 实现')
        self.logger.debug(self.__dict__)
