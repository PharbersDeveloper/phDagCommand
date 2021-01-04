import os
import ast
import base64
import subprocess

from phcli.ph_aws.ph_s3 import PhS3
from phcli.ph_aws.ph_sts import PhSts
from phcli.ph_logs.ph_logs import phs3logger
from phcli.ph_errs.ph_err import *
from phcli.ph_max_auto import define_value as dv
from phcli.ph_max_auto.ph_config.phconfig.phconfig import PhYAMLConfig
from phcli.ph_max_auto.ph_preset_jobs.preset_job_factory import preset_factory


logger = phs3logger()
phsts = PhSts().assume_role(
    base64.b64decode(dv.ASSUME_ROLE_ARN).decode(),
    dv.ASSUME_ROLE_EXTERNAL_ID,
)
phs3 = PhS3(phsts=phsts)


class PhBase(object):
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
        dag_path = project_path + self.dag_prefix + self.name + "/"
        upload_path = project_path + self.upload_prefix + self.name + "/"

        return {
            'project_path': project_path,
            'job_path': job_path,
            'combine_path': combine_path,
            'dag_path': dag_path,
            'upload_path': upload_path,
        }

    def create(self, **kwargs):
        """
        默认的创建过程
        """
        logger.info('maxauto 默认的 create 实现')
        logger.debug(self.__dict__)

    def run(self, **kwargs):
        """
        默认的运行过程
        """
        logger.info('maxauto 默认的 run 实现')
        logger.debug(self.__dict__)

    def combine(self, **kwargs):
        """
        默认的关联过程
        """
        logger.info('maxauto 默认的 combine 实现')
        logger.debug(self.__dict__)

        if os.path.exists(self.combine_path):
            raise exception_file_already_exist
        else:
            subprocess.call(["mkdir", "-p", self.combine_path])

        f_lines = phs3.open_object_by_lines(dv.TEMPLATE_BUCKET, dv.CLI_VERSION + dv.TEMPLATE_PHDAG_FILE)
        with open(self.combine_path + "/phdag.yaml", "w") as file:
            for line in f_lines:
                line = line + "\n"
                line = line.replace("$name", self.name) \
                            .replace("$dag_owner", self.owner) \
                            .replace("$dag_tag", self.tag) \
                            .replace("$runtime", self.runtime) \
                            .replace("$command", 'submit')
                file.write(line)

    def dag(self, **kwargs):
        """
        默认的DAG过程
        """
        logger.info('maxauto 默认的 dag 实现')
        logger.debug(self.__dict__)

        if os.path.exists(self.dag_path):
            raise exception_file_already_exist
        else:
            subprocess.call(["mkdir", "-p", self.dag_path])

        config = PhYAMLConfig(self.combine_path, "/phdag.yaml")
        config.load_yaml()

        def write_dag_pyfile():
            w = open(self.dag_path + "ph_dag_" + config.spec.dag_id + ".py", "a")
            f_lines = phs3.open_object_by_lines(dv.TEMPLATE_BUCKET, dv.CLI_VERSION + dv.TEMPLATE_PHGRAPHTEMP_FILE)
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

            jf = phs3.open_object_by_lines(dv.TEMPLATE_BUCKET, dv.CLI_VERSION + dv.TEMPLATE_PHDAGJOB_FILE)
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
        logger.info('maxauto 默认的 publish 实现')
        logger.debug(self.__dict__)

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
                    bucket_name=dv.TEMPLATE_BUCKET,
                    s3_dir=dv.CLI_VERSION + dv.DAGS_S3_PHJOBS_PATH + self.name + "/" + key
                )

    def online_run(self, **kwargs):
        """
        默认的 online_run 过程
        """
        logger.info('maxauto 默认的 online_run 实现')
        logger.debug(self.__dict__)

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

        context = ast_parse(self.context)
        args = ast_parse(self.args)

        group = self.group + "/" if self.group else ''
        job_path = dv.DAGS_S3_PHJOBS_PATH + group + self.name
        submit_prefix = "s3a://" + dv.TEMPLATE_BUCKET + "/" + dv.CLI_VERSION + job_path + "/"
        args = phs3.open_object_by_lines(dv.TEMPLATE_BUCKET, dv.CLI_VERSION + job_path + "/args.properties")


    #
    #         stream = phs3.open_object(dv.TEMPLATE_BUCKET, dv.CLI_VERSION + job_path + "/phconf.yaml")
    #         config = PhYAMLConfig(self.path)
    #         config.load_yaml(stream)
    #         runtime = config.spec.containers.runtime
    #         runtime_inst = self.get_runtime_inst(runtime)
    #
    #         access_key = os.getenv("AWS_ACCESS_KEY_ID", 'NULL_AWS_ACCESS_KEY_ID')
    #         secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", 'NULL_AWS_SECRET_ACCESS_KEY')
    #         current_user = os.getenv("HADOOP_PROXY_USER")
    #         if current_user is None:
    #             current_user = "airflow"
    #
    #         cmd_arr = ["spark-submit",
    #                    "--master", "yarn",
    #                    "--deploy-mode", "cluster",
    #                    "--name", self.path+"_"+self.job_id,
    #                    "--proxy-user", current_user]
    #
    #         conf_map = {
    #             "spark.driver.memory": "1g",
    #             "spark.driver.cores": "1",
    #             "spark.executor.memory": "2g",
    #             "spark.executor.cores": "1",
    #             "spark.driver.extraJavaOptions": "-Dfile.encoding=UTF-8 "
    #                                              "-Dsun.jnu.encoding=UTF-8 "
    #                                              "-Dcom.amazonaws.services.s3.enableV4",
    #             "spark.executor.extraJavaOptions": "-Dfile.encoding=UTF-8 "
    #                                                "-Dsun.jnu.encoding=UTF-8 "
    #                                                "-Dcom.amazonaws.services.s3.enableV4",
    #             "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    #             "spark.hadoop.fs.s3a.access.key": access_key,
    #             "spark.hadoop.fs.s3a.secret.key": secret_key,
    #             "spark.hadoop.fs.s3a.endpoint": "s3.cn-northwest-1.amazonaws.com.cn"
    #         }
    #         conf_map.update(runtime_inst.submit_conf(self.path, phs3, runtime))
    #         conf_map.update(dict([(k.lstrip("CONF__"), v) for k, v in self.context.items() if k.startswith('CONF__')]))
    #         conf_map = [('--conf', k + '=' + v) for k, v in conf_map.items()]
    #         cmd_arr += [j for i in conf_map for j in i]
    #
    #         other_map = {
    #             "num-executors": "2",
    #         }
    #         other_map.update(dict([(k.lstrip("OTHER__"), v) for k, v in self.context.items() if k.startswith('OTHER__')]))
    #         other_map = [('--'+k, v) for k, v in other_map.items()]
    #         cmd_arr += [j for i in other_map for j in i]
    #
    #         file_map = runtime_inst.submit_file(submit_prefix)
    #         file_map = [('--'+k, v) for k, v in file_map.items()]
    #         cmd_arr += [j for i in file_map for j in i]
    #
    #         cmd_arr += [runtime_inst.submit_main(submit_prefix)]
    #
    #         cmd_arr += ['--owner', self.owner]
    #         cmd_arr += ['--run_id', self.run_id]
    #         cmd_arr += ['--job_id', self.job_id]
    #
    #         # dag_run 优先 phconf 默认参数
    #         must_args = [arg.strip() for arg in dv.PRESET_MUST_ARGS.split(",")]
    #         cur_key = ""
    #         for it in [arg for arg in args if arg]:
    #             # 如果是 key，记录这个key
    #             if it[0:2] == "--":
    #                 cur_key = it[2:]
    #                 # 必须参数，不使用用户的配置，用系统注入的
    #                 if it[2:] in must_args:
    #                     continue
    #                 cmd_arr.append(it)
    #             else:
    #                 # 必须参数的 value 不处理
    #                 if cur_key in must_args:
    #                     continue
    #                 if cur_key in self.args.keys():
    #                     it = self.args[cur_key]
    #                 if it:
    #                     cmd_arr.append(it)
    #
    #         phlogger.info(cmd_arr)
    #         return subprocess.call(cmd_arr)


    def status(self, **kwargs):
        """
        默认的查看运行状态
        """
        logger.info('maxauto 默认的 status 实现')
        logger.debug(self.__dict__)
