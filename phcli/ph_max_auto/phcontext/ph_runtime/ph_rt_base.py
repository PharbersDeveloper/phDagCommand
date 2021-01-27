import os
import subprocess

from phcli.ph_max_auto import define_value as dv


class PhRTBase(object):
    """
    Runtime Base Class
    """
    def c9_create_phmain(self, path=None):
        raise NotImplementedError

    def jupyter_to_c9(self, dag_full_path, **kwargs):
        raise NotImplementedError

    def create(self, **kwargs):
        raise NotImplementedError

    def table_driver_command_run(self, command):
        table = {
            'submit': self.submit_run,
            'script': self.script_run,
        }
        return table[command]

    def submit_run(self, **kwargs):
        access_key = os.getenv("AWS_ACCESS_KEY_ID", 'NULL_AWS_ACCESS_KEY_ID')
        secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", 'NULL_AWS_SECRET_ACCESS_KEY')
        current_user = os.getenv("HADOOP_PROXY_USER", 'airflow')

        cmd_arr = ["spark-submit",
                   "--master", "yarn",
                   "--deploy-mode", "cluster",
                   "--name", self.job_id,
                   "--proxy-user", current_user,
                   "--queue", 'airflow']

        conf_map = {
            "spark.driver.cores": "1",
            "spark.driver.memory": "4g",
            "spark.executor.cores": "1",
            "spark.executor.memory": "4g",
            "spark.executor.instances": "1",
            "spark.driver.extraJavaOptions": "-Dfile.encoding=UTF-8 "
                                             "-Dsun.jnu.encoding=UTF-8 "
                                             "-Dcom.amazonaws.services.s3.enableV4",
            "spark.executor.extraJavaOptions": "-Dfile.encoding=UTF-8 "
                                               "-Dsun.jnu.encoding=UTF-8 "
                                               "-Dcom.amazonaws.services.s3.enableV4",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.access.key": access_key,
            "spark.hadoop.fs.s3a.secret.key": secret_key,
            "spark.hadoop.fs.s3a.endpoint": "s3.cn-northwest-1.amazonaws.com.cn"
        }
        conf_map.update(kwargs['submit_conf'])
        conf_map.update(dict([(k.lstrip("CONF__"), v) for k, v in self.context.items() if k.startswith('CONF__')]))
        conf_map = [('--conf', k + '=' + v) for k, v in conf_map.items()]
        cmd_arr += [j for i in conf_map for j in i]

        other_map = {}
        other_map.update(dict([(k.lstrip("OTHER__"), v) for k, v in self.context.items() if k.startswith('OTHER__')]))
        other_map = [('--'+k, v) for k, v in other_map.items()]
        cmd_arr += [j for i in other_map for j in i]

        file_map = kwargs['submit_file']
        file_map = [('--'+k, v) for k, v in file_map.items()]
        cmd_arr += [j for i in file_map for j in i]

        cmd_arr += [kwargs['submit_main']]

        cmd_arr += ['--owner', self.owner]
        cmd_arr += ['--run_id', self.run_id]
        cmd_arr += ['--job_id', self.job_id]

        # dag_run 优先 phconf 默认参数
        default_args = self.phs3.open_object_by_lines(dv.TEMPLATE_BUCKET, dv.CLI_VERSION + self.s3_job_path + "/args.properties")
        must_args = [arg.strip() for arg in dv.PRESET_MUST_ARGS.split(",")]
        cur_key = ""
        for it in [arg for arg in default_args if arg]:
            # 如果是 key，记录这个key
            if it[0:2] == "--":
                cur_key = it[2:]
                # 必须参数，不使用用户的配置，用系统注入的
                if it[2:] in must_args:
                    continue
                cmd_arr.append(it)
            else:
                # 必须参数的 value 不处理
                if cur_key in must_args:
                    continue
                if cur_key in self.args.keys():
                    it = self.args[cur_key]
                if it:
                    cmd_arr.append(it)

        return subprocess.check_output(cmd_arr, timeout=float(self.timeout) * 60)

    def script_run(self, **kwargs):
        cmd_arr = []
        cmd_arr += kwargs['entrypoint']

        cmd_arr += ['--owner', self.owner]
        cmd_arr += ['--run_id', self.run_id]
        cmd_arr += ['--job_id', self.job_id]

        # dag_run 优先 phconf 默认参数
        default_args = self.phs3.open_object_by_lines(dv.TEMPLATE_BUCKET, dv.CLI_VERSION + self.s3_job_path + "/args.properties")
        must_args = [arg.strip() for arg in dv.PRESET_MUST_ARGS.split(",")]
        cur_key = ""
        for it in [arg for arg in default_args if arg]:
            # 如果是 key，记录这个key
            if it[0:2] == "--":
                cur_key = it[2:]
                # 必须参数，不使用用户的配置，用系统注入的
                if it[2:] in must_args:
                    continue
                cmd_arr.append(it)
            else:
                # 必须参数的 value 不处理
                if cur_key in must_args:
                    continue
                if cur_key in self.args.keys():
                    it = self.args[cur_key]
                if it:
                    cmd_arr.append(it)

        return subprocess.check_output(cmd_arr, timeout=float(self.timeout) * 60)

    def online_run(self, **kwargs):
        self.table_driver_command_run(self.command)()
