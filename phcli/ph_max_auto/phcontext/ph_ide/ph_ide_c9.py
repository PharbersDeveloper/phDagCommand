import os
import subprocess

from phcli.ph_errs.ph_err import *
from .ph_ide_base import PhIDEBase, logger, phs3, dv
from phcli.ph_max_auto.ph_config.phconfig.phconfig import PhYAMLConfig


class PhIDEC9(PhIDEBase):
    """
    针对 C9 环境的执行策略
    """

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
        self.__dict__.update(super().get_absolute_path())

    def create(self, **kwargs):
        """
        c9的创建过程
        """
        logger.info('maxauto ide=c9 的 create 实现')
        logger.debug(self.__dict__)

        if os.path.exists(self.job_path):
            raise exception_file_already_exist
        else:
            subprocess.call(["mkdir", "-p", self.job_path])

        f_lines = phs3.open_object_by_lines(dv.TEMPLATE_BUCKET, dv.CLI_VERSION + dv.TEMPLATE_PHCONF_FILE)
        with open(self.job_path + "/phconf.yaml", "a") as file:
            for line in f_lines:
                line = line + "\n"
                line = line.replace("$name", self.name) \
                    .replace("$runtime", self.runtime) \
                    .replace("$command", self.command) \
                    .replace("$code", self.table_driver_runtime_main_code(self.runtime))
                file.write(line)

        runtime_inst = self.table_driver_runtime_inst(self.runtime)
        runtime_inst(
            job_path=self.job_path,
            phs3=phs3,
            command=self.command,
        ).create()

    def run(self, **kwargs):
        """
        c9的运行过程
        """
        logger.info('maxauto ide=c9 的 run 实现')
        logger.debug(self.__dict__)

        config = PhYAMLConfig(self.job_path)
        config.load_yaml()

        if config.spec.containers.repository == "local":
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
            subprocess.call(cb)
        else:
            raise exception_function_not_implement

    def dag(self, **kwargs):
        """
        c9的DAG过程
        """
        logger.info('maxauto ide=c9 的 dag 实现')
        logger.debug(self.__dict__)

        def copy_jobs(**kwargs):
            def yaml2args(path):
                config = PhYAMLConfig(path)
                config.load_yaml()

                f = open(path + "/args.properties", "a")
                for arg in config.spec.containers.args:
                    if arg.value != "":
                        f.write("--" + arg.key + "\n")
                        f.write(str(arg.value) + "\n")

                for output in config.spec.containers.outputs:
                    if output.value != "":
                        f.write("--" + output.key + "\n")
                        f.write(str(output.value) + "\n")
                f.close()

            config = kwargs['config']
            for jt in config.spec.jobs:
                if jt.name.startswith('preset'):
                    continue

                job_name = jt.name.replace('.', '_')
                job_full_path = self.project_path + self.job_prefix + jt.name.replace('.', '/')
                if not os.path.exists(job_full_path):
                    raise exception_file_not_exist

                subprocess.call(["cp", '-r', job_full_path, self.dag_path + job_name])
                yaml2args(self.dag_path + job_name)

        super().dag(copy_func=copy_jobs)
