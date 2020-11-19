# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This module document the usage of class pharbers command context,
"""

import os
import sys
import ast
import base64
import subprocess

from ph_aws.ph_sts import PhSts
from ph_aws.ph_s3 import PhS3
from ph_logs.ph_logs import phlogger
from ph_max_auto import define_value as dv
from ph_max_auto.ph_config.phconfig.phconfig import PhYAMLConfig
from ph_errs.ph_err import PhException, PhRuntimeError
from ph_errs.ph_err import exception_file_already_exist, exception_file_not_exist, exception_function_not_implement


phsts = PhSts().assume_role(
    base64.b64decode(dv.ASSUME_ROLE_ARN).decode(),
    dv.ASSUME_ROLE_EXTERNAL_ID,
)
phs3 = PhS3(phsts=phsts)


class PhContextFacade(object):
    """The Pharbers Max Job Command Line Interface (CLI) Command Context Entry

        Args:
            cmd: the command that you want to process
            path: the directory that you want to process
            context: the context that you want to submit
            args: the args that you want to submit
    """

    @staticmethod
    def get_workspace_dir():
        return os.getenv(dv.ENV_WORKSPACE_KEY, dv.ENV_WORKSPACE_DEFAULT)

    @staticmethod
    def get_current_project_dir():
        return os.getenv(dv.ENV_CUR_PROJ_KEY, dv.ENV_CUR_PROJ_DEFAULT)

    def __init__(self, runtime, group, path, cmd, job_id='', context='{}', args='{}'):
        self.runtime = runtime.lower()
        self.name = path.replace('/', '')
        self.group = group.replace('/', '')
        self.cmd = cmd
        self.job_id = job_id
        self.context = self.ast_parse(context)
        self.args = self.ast_parse(args)
        self.job_prefix = "phjobs/"
        self.combine_prefix = "phcombines/"
        self.dag_prefix = "phdags/"
        self.upload_prefix = "upload/"

        self.cur_proj_dir = self.get_workspace_dir() + "/" + self.get_current_project_dir() + '/'
        self.job_path = self.cur_proj_dir + self.job_prefix + (self.group + '/' if self.group else '') + self.name + '/'
        self.combine_path = self.cur_proj_dir + self.combine_prefix + self.name + '/'
        self.dag_path = self.cur_proj_dir + self.dag_prefix + self.name + "/"
        self.upload_path = self.cur_proj_dir + self.upload_prefix + self.name + "/"
        self.path = self.get_destination_path()

    def ast_parse(self, string):
        '''
        解析json
        :param string: json 字符串
        :return:
        '''
        ast_dict = {}
        if string != "":
            ast_dict = ast.literal_eval(string.replace(" ", ""))
            for k, v in ast_dict.items():
                if isinstance(v, str) and v.startswith('{') and v.endswith('}'):
                    ast_dict[k] = ast.literal_eval(v)
            if ast_dict:
                phlogger.info(ast_dict)
        return ast_dict

    def get_destination_path(self):
        if self.cmd == "create":
            return self.job_path
        elif self.cmd == "combine":
            return self.combine_path
        elif self.cmd == "dag":
            return self.combine_path
        elif self.cmd == "run":
            return self.job_path
        elif self.cmd == "publish":
            return self.job_path
        elif self.cmd == "submit":
            return self.name
        else:
            raise PhException("Something goes wrong!!!")

    def __str__(self):
        return str(self.__dict__)

    def execute(self):
        ret = 0
        self.check_dir()
        if self.cmd == "create":
            self.command_create_exec()
        elif self.cmd == "run":
            self.command_run_exec()
        elif self.cmd == "combine":
            self.command_combine_exec()
        elif self.cmd == "dag":
            self.command_dag_exec()
        elif self.cmd == "publish":
            self.command_publish_exec()
        elif self.cmd == "submit":
            ret = self.command_submit_exec()
        elif self.cmd == "logs":
            self.command_logs_exec()
        else:
            self.command_status_exec()

        return ret

    def check_dir(self):
        try:
            if (self.cmd == "create") | (self.cmd == "combine"):
                if os.path.exists(self.path):
                    raise exception_file_already_exist
            elif self.cmd == "publish":
                if not os.path.exists(self.dag_path):
                    raise exception_file_not_exist
            elif self.cmd == "submit":
                phlogger.info("command submit or status, do nothing")
            else:
                if not os.path.exists(self.path):
                    raise exception_file_not_exist
        except PhRuntimeError as e:
            phlogger.info(e.msg)
            raise e

    @staticmethod
    def get_runtime_inst(runtime):
        from ph_max_auto.ph_runtime import ph_python3
        from ph_max_auto.ph_runtime import ph_r

        table = {
            "python": ph_python3,
            "python2": ph_python3,
            "python3": ph_python3,
            "r": ph_r,
        }
        return table[runtime]

    def command_create_exec(self):
        phlogger.info("command create")
        subprocess.call(["mkdir", "-p", self.job_path])

        rc_map = {
            "python3": "phmain.py",
            "r": "phmain.R"
        }

        f_lines = phs3.open_object_by_lines(dv.TEMPLATE_BUCKET, dv.CLI_VERSION + dv.TEMPLATE_PHCONF_FILE)
        with open(self.job_path + "/phconf.yaml", "a") as file:
            for line in f_lines:
                line = line + "\n"
                line = line.replace("$name", self.name) \
                            .replace("$runtime", self.runtime) \
                            .replace("$code", rc_map[self.runtime])
                file.write(line)

        runtime_inst = self.get_runtime_inst(self.runtime)
        runtime_inst.create(
            job_path=self.job_path,
            phs3=phs3,
        )

    def command_run_exec(self):
        phlogger.info("run")

        rb_map = {  # run -> bin
            "bash": "/bin/bash",
            "python2": "python2",
            "python3": "python3",
            "r": "Rscript",
        }

        config = PhYAMLConfig(self.job_path)
        config.load_yaml()

        if config.spec.containers.repository == "local":
            entry_runtime = config.spec.containers.runtime
            entry_runtime = rb_map[entry_runtime]
            entry_point = config.spec.containers.code
            if "/" not in entry_point:
                entry_point = self.job_path + entry_point
                cb = [entry_runtime, entry_point]
                for arg in config.spec.containers.args:
                    if sys.version_info > (3, 0):
                        cb.append("--" + arg.key + "=" + str(arg.value))
                    else:
                        cb.append("--" + arg.key)
                        if type(arg.value) is unicode:
                            cb.append(arg.value.encode("utf-8"))
                        else:
                            cb.append(str(arg.value))
                for output in config.spec.containers.outputs:
                    if sys.version_info > (3, 0):
                        cb.append("--" + output.key + "=" + str(output.value))
                    else:
                        cb.append("--" + output.key)
                        if type(output.value) is unicode:
                            cb.append(output.value.encode("utf-8"))
                        else:
                            cb.append(str(output.value))
                subprocess.call(cb)
        else:
            raise exception_function_not_implement

    def command_combine_exec(self):
        phlogger.info("command combine")
        subprocess.call(["mkdir", "-p", self.combine_path])

        f_lines = phs3.open_object_by_lines(dv.TEMPLATE_BUCKET, dv.CLI_VERSION + dv.TEMPLATE_PHDAG_FILE)
        with open(self.path + "/phdag.yaml", "w") as file:
            for line in f_lines:
                line = line + "\n"
                line = line.replace("$name", self.name).replace("$runtime", self.runtime)
                file.write(line)

    def command_dag_exec(self):
        phlogger.info("command dag")

        subprocess.call(["rm", "-rf", self.dag_path])
        subprocess.call(["mkdir", "-p", self.dag_path])

        config = PhYAMLConfig(self.combine_path, "/phdag.yaml")
        config.load_yaml()

        def yaml2args(path):
            config = PhYAMLConfig(path)
            config.load_yaml()

            f = open(path + "/args.properties", "a")
            for arg in config.spec.containers.args:
                if arg.value != "":
                    f.write("--" + arg.key + "\n")
                    if sys.version_info > (3, 0):
                        f.write(str(arg.value) + "\n")
                    else:
                        if type(arg.value) is unicode:
                            f.write(arg.value.encode("utf-8") + "\n")
                        else:
                            f.write(str(arg.value) + "\n")
            for output in config.spec.containers.outputs:
                if output.value != "":
                    f.write("--" + output.key + "\n")
                    if sys.version_info > (3, 0):
                        f.write(str(output.value) + "\n")
                    else:
                        if type(output.value) is unicode:
                            f.write(output.value.encode("utf-8") + "\n")
                        else:
                            f.write(str(arg.value) + "\n")
                    # f.write(str(arg.value) + "\n")
            f.close()

        def write_dag_pyfile():
            w = open(self.dag_path + "ph_dag_" + config.spec.dag_id + ".py", "a")
            f_lines = phs3.open_object_by_lines(dv.TEMPLATE_BUCKET, dv.CLI_VERSION + dv.TEMPLATE_PHGRAPHTEMP_FILE)
            for line in f_lines:
                line = line + "\n"
                if line == "$alfred_import_jobs\n":
                    for j in config.spec.jobs:
                        w.write("from phjobs." + j.name + ".phjob import execute as " + j.name + "\n")
                        # w.write("from phjobs." + j.name + " import execute as " + j.name + "\n")
                else:
                    w.write(
                        line.replace("$alfred_dag_owner", str(config.spec.owner)) \
                            .replace("$alfred_email_on_failure", str(config.spec.email_on_failure)) \
                            .replace("$alfred_email_on_retry", str(config.spec.email_on_retry)) \
                            .replace("$alfred_email", str(config.spec.email)) \
                            .replace("$alfred_retries", str(config.spec.retries)) \
                            .replace("$alfred_retry_delay", str(config.spec.retry_delay)) \
                            .replace("$alfred_dag_id", str(config.spec.dag_id)) \
                            .replace("$alfred_schedule_interval", str(config.spec.schedule_interval)) \
                            .replace("$alfred_description", str(config.spec.description)) \
                            .replace("$alfred_dag_timeout", str(config.spec.dag_timeout)) \
                            .replace("$alfred_start_date", str(config.spec.start_date))
                    )

            jf = phs3.open_object_by_lines(dv.TEMPLATE_BUCKET, dv.CLI_VERSION + dv.TEMPLATE_PHDAGJOB_FILE)
            for jt in config.spec.jobs:
                job_name = jt.name.replace('.', '_')
                job_full_path = self.cur_proj_dir + self.job_prefix + jt.name.replace('.', '/')
                if not os.path.exists(job_full_path):
                    raise exception_file_not_exist

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

                subprocess.call(["cp", '-r',
                                 job_full_path,
                                 self.dag_path + job_name])
                yaml2args(self.dag_path + job_name)

            for linkage in config.spec.linkage:
                w.write(linkage.replace('.', '_'))
                w.write("\n")

            w.close()

        write_dag_pyfile()

    def command_publish_exec(self):
        phlogger.info("command publish")

        for key in os.listdir(self.dag_path):
            if os.path.isfile(self.dag_path + key):
                phs3.upload(
                    file=self.dag_path+key,
                    bucket_name=dv.DAGS_S3_BUCKET,
                    object_name=dv.DAGS_S3_PREV_PATH + key
                )
            else:
                phs3.upload_dir(
                    dir=self.dag_path+key,
                    bucket_name=dv.DAGS_S3_BUCKET,
                    s3_dir=dv.DAGS_S3_PHJOBS_PATH + self.name + "/" + key
                )

    def command_submit_exec(self):
        phlogger.info("submit command exec")
        phlogger.info("submit command with Job Group '" + self.group + "'")
        phlogger.info("submit command with Job Name '" + self.name + "'")
        phlogger.info("submit command with context " + str(self.context))
        phlogger.info("submit command with args " + str(self.args))

        self.group = self.group + "/" if self.group else ''
        job_path = dv.DAGS_S3_PHJOBS_PATH + self.group + self.path
        submit_prefix = "s3a://" + dv.DAGS_S3_BUCKET + "/" + job_path + "/"
        args = phs3.open_object_by_lines(dv.DAGS_S3_BUCKET, job_path + "/args.properties")

        stream = phs3.open_object(dv.DAGS_S3_BUCKET, job_path + "/phconf.yaml")
        config = PhYAMLConfig(self.path)
        config.load_yaml(stream)
        runtime = config.spec.containers.runtime
        runtime_inst = self.get_runtime_inst(runtime)

        access_key = os.getenv("AWS_ACCESS_KEY_ID", 'NULL_AWS_ACCESS_KEY_ID')
        secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", 'NULL_AWS_SECRET_ACCESS_KEY')
        current_user = os.getenv("HADOOP_PROXY_USER")
        if current_user is None:
            current_user = "airflow"

        cmd_arr = ["spark-submit",
                   "--master", "yarn",
                   "--deploy-mode", "cluster",
                   "--name", self.path+"_"+self.job_id,
                   "--proxy-user", current_user]

        conf_map = {
            "spark.driver.memory": "1g",
            "spark.driver.cores": "1",
            "spark.executor.memory": "2g",
            "spark.executor.cores": "1",
            "spark.driver.extraJavaOptions": "-Dcom.amazonaws.services.s3.enableV4",
            "spark.executor.extraJavaOptions": "-Dcom.amazonaws.services.s3.enableV4",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.access.key": access_key,
            "spark.hadoop.fs.s3a.secret.key": secret_key,
            "spark.hadoop.fs.s3a.endpoint": "s3.cn-northwest-1.amazonaws.com.cn"
        }
        conf_map.update(runtime_inst.submit_conf(self.path, phs3, runtime))
        conf_map.update(dict([(k.lstrip("CONF__"), v) for k, v in self.context.items() if k.startswith('CONF__')]))
        conf_map = [('--conf', k + '=' + v) for k, v in conf_map.items()]
        cmd_arr += [j for i in conf_map for j in i]

        other_map = {
            "num-executors": "2",
        }
        other_map.update(dict([(k.lstrip("OTHER__"), v) for k, v in self.context.items() if k.startswith('OTHER__')]))
        other_map = [('--'+k, v) for k, v in other_map.items()]
        cmd_arr += [j for i in other_map for j in i]

        file_map = runtime_inst.submit_file(submit_prefix)
        file_map = [('--'+k, v) for k, v in file_map.items()]
        cmd_arr += [j for i in file_map for j in i]

        cmd_arr += [runtime_inst.submit_main(submit_prefix)]

        cmd_arr += ['--job_id', self.job_id]

        cur_key = ""
        for it in [arg for arg in args if arg]:
            if it[0:2] == "--":
                cur_key = it[2:]
            else:
                if cur_key in self.args.keys():
                    it = self.args[cur_key]
            if it != "":
                cmd_arr.append(it)

        phlogger.info(cmd_arr)
        return subprocess.call(cmd_arr)

    def command_logs_exec(self):
        pass

    def command_status_exec(self):
        pass
