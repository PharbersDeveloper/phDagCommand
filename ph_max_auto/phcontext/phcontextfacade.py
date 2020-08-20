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
from ph_errs.ph_err import PhException, PhRuntimeError
from ph_errs.ph_err import exception_file_already_exist, exception_file_not_exist, exception_function_not_implement
from ph_max_auto import define_value as dv
from ph_max_auto.phconfig.phconfig import PhYAMLConfig

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

    def __init__(self, cmd, path, context='{}', args='{}'):
        self.cmd = cmd
        self.path = path
        self.context = self.ast_parse(context)
        self.args = self.ast_parse(args)
        self.job_path = path
        self.combine_path = path
        self.dag_path = path
        self.upload_path = path
        self.name = ""
        self.job_prefix = "phjobs"
        self.combine_prefix = "phcombines"
        self.dag_prefix = "phdags"
        self.upload_prefix = "upload"

    def execute(self):
        ret = 0
        self.check_dir()
        if self.cmd == "create":
            self.command_create_exec()
        elif self.cmd == "combine":
            self.command_combine_exec()
        elif self.cmd == "run":
            self.command_run_exec()
        elif self.cmd == "dag":
            self.command_dag_exec()
        elif self.cmd == "submit":
            ret = self.command_submit_exec()
        # elif self.cmd == "":
        #     ret = self.command_status_exec()
        else:
            self.command_publish_exec()

        return ret

    @staticmethod
    def get_workspace_dir():
        return os.getenv(dv.ENV_WORKSPACE_KEY, dv.ENV_WORKSPACE_DEFAULT)

    @staticmethod
    def get_current_project_dir():
        return os.getenv(dv.ENV_CUR_PROJ_KEY, dv.ENV_CUR_PROJ_DEFAULT)

    def get_destination_path(self):
        self.job_path = self.get_workspace_dir() + "/" + self.get_current_project_dir() + "/" + self.job_prefix + "/" + self.name
        self.combine_path = self.get_workspace_dir() + "/" + self.get_current_project_dir() + "/" + self.combine_prefix + "/" + self.name
        self.dag_path = self.get_workspace_dir() + "/" + self.get_current_project_dir() + "/" + self.dag_prefix + "/"
        self.upload_path = self.get_workspace_dir() + "/" + self.get_current_project_dir() + "/" + self.upload_prefix + "/"
        if self.cmd == "create":
            return self.get_workspace_dir() + "/" + self.get_current_project_dir() + "/" + self.job_prefix + "/" + self.name
        elif self.cmd == "combine":
            return self.get_workspace_dir() + "/" + self.get_current_project_dir() + "/" + self.combine_prefix + "/" + self.name
        elif self.cmd == "dag":
            return self.get_workspace_dir() + "/" + self.get_current_project_dir() + "/" + self.combine_prefix + "/" + self.name
        elif self.cmd == "run":
            return self.get_workspace_dir() + "/" + self.get_current_project_dir() + "/" + self.job_prefix + "/" + self.name
        elif self.cmd == "publish":
            return self.get_workspace_dir() + "/" + self.get_current_project_dir() + "/" + self.job_prefix + "/" + self.name
        elif self.cmd == "submit":
            return self.path
        else:
            raise PhException("Something goes wrong!!!")

    def check_dag_dir(self, dag_id):
        if os.path.exists(self.dag_path + "/" + dag_id):
            raise exception_file_already_exist

    def clean_dag_dir(self):
        subprocess.call(["rm", "-rf", self.dag_path])

    def check_dir(self):
        if "/" not in self.path:
            self.name = self.path
            self.path = self.get_destination_path()
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

    def ast_parse(self, string):
        ast_dict = {}
        if string != "":
            ast_dict = ast.literal_eval(string.replace(" ", ""))
            for k, v in ast_dict.items():
                if isinstance(v, str) and v.startswith('{') and v.endswith('}'):
                    ast_dict[k] = ast.literal_eval(v)
            if ast_dict:
                phlogger.info(ast_dict)
        return ast_dict

    def command_create_exec(self):
        phlogger.info("command create")
        subprocess.call(["mkdir", "-p", self.path])
        subprocess.call(["touch", self.path + "/__init__.py"])

        phs3.download(dv.TEMPLATE_BUCKET, dv.CLI_VERSION + dv.TEMPLATE_PHJOB_FILE, self.path + "/phjob.py")
        phs3.download(dv.TEMPLATE_BUCKET, dv.CLI_VERSION + dv.TEMPLATE_PHCONF_FILE, self.path + "/phconf.yaml")

        config = PhYAMLConfig(self.path)
        config.load_yaml()

        with open(self.path + "/phjob.py", "a") as file:
            file.write("def execute(")
            for arg_index in range(len(config.spec.containers.args)):
                arg = config.spec.containers.args[arg_index]
                if arg_index == len(config.spec.containers.args) - 1:
                    file.write(arg.key)
                else:
                    file.write(arg.key + ", ")
            file.write("):\n")
            file.write('\t"""\n')
            file.write('\t\tplease input your code below\n')
            file.write('\t"""\n')

        with open(self.path + "/phmain.py", "w") as file:
            f_lines = phs3.open_object_by_lines(dv.TEMPLATE_BUCKET, dv.CLI_VERSION + dv.TEMPLATE_PHMAIN_FILE)

            s = []
            for arg in config.spec.containers.args:
                s.append(arg.key)

            for line in f_lines:
                line = line + "\n"
                if line == "$alfred_debug_execute\n":
                    file.write("@click.command()\n")
                    for arg in config.spec.containers.args:
                        file.write("@click.option('--" + arg.key + "')\n")
                    # e.write("def debug_execute():\n")
                    file.write("def debug_execute(")
                    for arg_index in range(len(config.spec.containers.args)):
                        arg = config.spec.containers.args[arg_index]
                        if arg_index == len(config.spec.containers.args) - 1:
                            file.write(arg.key)
                        else:
                            file.write(arg.key + ", ")
                    file.write("):\n")
                    file.write("\texecute(")
                    for arg_index in range(len(config.spec.containers.args)):
                        arg = config.spec.containers.args[arg_index]
                        if arg_index == len(config.spec.containers.args) - 1:
                            file.write(arg.key)
                        else:
                            file.write(arg.key + ", ")
                    file.write(")\n")
                else:
                    file.write(line)

    def command_combine_exec(self):
        phlogger.info("command combine")
        subprocess.call(["mkdir", "-p", self.path])

        phs3.download(dv.TEMPLATE_BUCKET, dv.CLI_VERSION+dv.TEMPLATE_PHDAG_FILE, self.path + "/phdag.yaml")

    def command_publish_exec(self):
        phlogger.info("command publish")

        for _, dirs, _ in os.walk(self.dag_path):
            for key in dirs:
                if (not key.startswith(".")) and (not key.startswith("__pycache__")):
                    phs3.upload(self.dag_path + key + "/phmain.py",
                                dv.DAGS_S3_BUCKET,
                                dv.DAGS_S3_PHJOBS_PATH + key + "/phmain.py")
                    phs3.upload(self.dag_path + key + "/phjob.py",
                                dv.DAGS_S3_BUCKET,
                                dv.DAGS_S3_PHJOBS_PATH + key + "/phjob.py")
                    phs3.upload(self.dag_path + key + "/args.properties",
                                dv.DAGS_S3_BUCKET,
                                dv.DAGS_S3_PHJOBS_PATH + key + "/args.properties")

        for key in os.listdir(self.dag_path):
            if os.path.isfile(self.dag_path + key):
                phs3.upload(self.dag_path + key, dv.DAGS_S3_BUCKET, dv.DAGS_S3_PREV_PATH + key)

    def command_run_exec(self):
        phlogger.info("run")

        config = PhYAMLConfig(self.job_path)
        config.load_yaml()

        if config.spec.containers.repository == "local":
            entry_point = config.spec.containers.code
            if "/" not in entry_point:
                entry_point = self.path + "/" + entry_point
                cb = ["python", entry_point]
                for arg in config.spec.containers.args:
                    if sys.version_info > (3, 0):
                        cb.append("--" + arg.key + "=" + str(arg.value))
                    else:
                        cb.append("--" + arg.key)
                        if type(arg.value) is unicode:
                            cb.append(arg.value.encode("utf-8"))
                        else:
                            cb.append(str(arg.value))
                subprocess.call(cb)
        else:
            raise exception_function_not_implement

    def command_dag_exec(self):
        phlogger.info("command dag")

        self.clean_dag_dir()

        config = PhYAMLConfig(self.combine_path, "/phdag.yaml")
        config.load_yaml()
        self.check_dag_dir(config.spec.dag_id)

        subprocess.call(["mkdir", "-p", self.dag_path])

        w = open(self.dag_path + "/ph_dag_" + config.spec.dag_id + ".py", "a")
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

        def yaml2args(path):
            config = PhYAMLConfig(path)
            config.load_yaml()
            phlogger.info(config.spec.containers.args)

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
                    # f.write(str(arg.value) + "\n")
            f.close()

        jf = phs3.open_object_by_lines(dv.TEMPLATE_BUCKET, dv.CLI_VERSION + dv.TEMPLATE_PHDAGJOB_FILE)
        for jt in config.spec.jobs:
            # jf.seek(0)
            for line in jf:
                line = line + "\n"
                w.write(
                    line.replace("$alfred_command", str(jt.command)) \
                        .replace("$alfred_job_path", str(self.job_path[0:self.job_path.rindex("/") + 1]))
                        .replace("$alfred_dag_owner", str(config.spec.owner)) \
                        .replace("$alfred_name", str(jt.name))
                )
            subprocess.call(["cp", "-r",
                             self.job_path[0:self.job_path.rindex("/") + 1] + jt.name,
                             self.dag_path + jt.name])
            # subprocess.call(["zip", self.dag_path + jt.name + "/phjob.zip", self.dag_path + jt.name + "/phjob.py"])
            yaml2args(self.dag_path + jt.name)

        w.write(config.spec.linkage)
        w.write("\n")
        w.close()

    def command_submit_exec(self):
        phlogger.info("submit command exec")
        phlogger.info("submit command with Job name '" + self.path + "'")
        phlogger.info("submit command with context " + str(self.context))
        phlogger.info("submit command with args " + str(self.args))

        submit_prefix = "s3a://" + dv.DAGS_S3_BUCKET + "/" + dv.DAGS_S3_PHJOBS_PATH + self.path + "/"
        args = phs3.open_object_by_lines(dv.DAGS_S3_BUCKET, dv.DAGS_S3_PHJOBS_PATH + self.path + "/args.properties")

        access_key = os.getenv("AWS_ACCESS_KEY_ID", 'NULL_AWS_ACCESS_KEY_ID')
        secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", 'NULL_AWS_SECRET_ACCESS_KEY')
        current_user = os.getenv("HADOOP_PROXY_USER")
        if current_user is None:
            current_user = "airflow"

        cmd_arr = ["spark-submit",
                   "--master", "yarn",
                   "--deploy-mode", "cluster",
                   "--name", self.path,
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
        conf_map.update(dict([(k.lstrip("CONF__"), v) for k, v in self.context.items() if k.startswith('CONF__')]))
        conf_map = [('--conf', k + '=' + v) for k, v in conf_map.items()]
        cmd_arr += [j for i in conf_map for j in i]

        other_map = {
            "num-executors": "2",
            "jars": "s3a://ph-stream/jars/aws/aws-java-sdk-1.11.682.jar,"
                    "s3a://ph-stream/jars/aws/aws-java-sdk-core-1.11.682.jar,"
                    "s3a://ph-stream/jars/aws/aws-java-sdk-s3-1.11.682.jar,"
                    "s3a://ph-stream/jars/hadoop/hadoop-aws-2.9.2.jar",
            "py-files": "s3a://" + dv.DAGS_S3_BUCKET + "/" + dv.DAGS_S3_PHJOBS_PATH + "common/click.zip," +
                        "s3a://" + dv.DAGS_S3_BUCKET + "/" + dv.DAGS_S3_PHJOBS_PATH + "common/phcli.zip," +
                        submit_prefix + "phjob.py",
        }
        other_map.update(dict([(k.lstrip("OTHER__"), v) for k, v in self.context.items() if k.startswith('OTHER__')]))
        other_map = [('--'+k, v) for k, v in other_map.items()]
        cmd_arr += [j for i in other_map for j in i]

        cmd_arr += [submit_prefix + "phmain.py"]

        cur_key = ""
        for it in args:
            if it[0:2] == "--":
                cur_key = it[2:]
            else:
                if cur_key in self.args.keys():
                    it = self.args[cur_key]

            if it != "":
                cmd_arr.append(it)

        phlogger.info(cmd_arr)
        return subprocess.call(cmd_arr)
