import os
import subprocess

from .ph_ide_base import PhIDEBase, dv, exception_file_not_exist, exception_function_not_implement, PhYAMLConfig


class PhIDEC9(PhIDEBase):
    """
    针对 C9 环境的执行策略
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger.debug('maxauto PhIDEC9 init')
        self.logger.debug(self.__dict__)

    def create(self, **kwargs):
        """
        c9的创建过程
        """
        self.logger.info('maxauto ide=c9 的 create 实现')
        self.logger.debug(self.__dict__)

        self.check_path(self.job_path)
        subprocess.call(["mkdir", "-p", self.job_path])

        input_str = [k.strip() for k in self.inputs.split(',')]
        input_str = ["- key: " + i + "\n        value: \"abc\"" for i in input_str]
        input_str = '\n      '.join(input_str)
        output_str = [k.strip() for k in self.outputs.split(',')]
        output_str = ["- key: " + i + "\n        value: \"abc\"" for i in output_str]
        output_str = '\n      '.join(output_str)

        f_lines = self.phs3.open_object_by_lines(dv.TEMPLATE_BUCKET, dv.CLI_VERSION + dv.TEMPLATE_PHCONF_FILE)
        with open(self.job_path + "/phconf.yaml", "a") as file:
            for line in f_lines:
                line = line + "\n"
                line = line.replace("$name", self.name) \
                    .replace("$runtime", self.runtime) \
                    .replace("$command", self.command) \
                    .replace("$timeout", str(self.timeout)) \
                    .replace("$code", self.table_driver_runtime_main_code(self.runtime)) \
                    .replace("$input", input_str) \
                    .replace("$output", output_str)
                file.write(line)

        super().create()

    def run(self, **kwargs):
        """
        c9的运行过程
        """
        self.logger.info('maxauto ide=c9 的 run 实现')
        self.logger.debug(self.__dict__)

        config = PhYAMLConfig(self.job_path)
        config.load_yaml()

        if config.spec.containers.repository == "local":
            timeout = float(config.spec.containers.timeout) * 60
            entry_runtime = config.spec.containers.runtime
            entry_runtime = self.table_driver_runtime_binary(entry_runtime)
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

    def dag_copy_job(self, **kwargs):
        """
        maxauto dag 时 copy c9 环境下生成的 job
        """
        self.logger.info('maxauto ide=c9 的 dag_copy_job 实现')
        self.logger.debug(self.__dict__)

        job_name = kwargs['job_name'].replace('.', '_')
        job_full_path = self.project_path + self.job_prefix + kwargs['job_name'].replace('.', '/')

        if not os.path.exists(job_full_path):
            raise exception_file_not_exist

        subprocess.call(["cp", '-r', job_full_path, self.dag_path + job_name])
        self.yaml2args(self.dag_path + job_name)
