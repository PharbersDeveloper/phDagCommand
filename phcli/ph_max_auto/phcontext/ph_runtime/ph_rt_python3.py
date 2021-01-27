import os
import re
import subprocess

from phcli.ph_errs.ph_err import *
from .ph_rt_base import PhRTBase
from phcli.define_value import CLI_CLIENT_VERSION
from phcli.ph_max_auto import define_value as dv
from phcli.ph_max_auto.ph_config.phconfig.phconfig import PhYAMLConfig


class PhRTPython3(PhRTBase):
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def c9_create_init(self, path=None):
        if not path:
            path = self.job_path + "/__init__.py"
        subprocess.call(["touch", path])

    def c9_create_phmain(self, path=None):
        if not path:
            path = self.job_path

        config = PhYAMLConfig(path)
        config.load_yaml()
        f_lines = self.phs3.open_object_by_lines(dv.TEMPLATE_BUCKET, dv.CLI_VERSION + dv.TEMPLATE_PHMAIN_FILE_PY)
        with open(path + "/phmain.py", "w") as file:
            s = []
            for arg in config.spec.containers.args:
                s.append(arg.key)

            for line in f_lines:
                line = line + "\n"
                if line == "$alfred_debug_execute\n":
                    file.write("@click.command()\n")
                    for must in dv.PRESET_MUST_ARGS.split(","):
                        file.write("@click.option('--{}')\n".format(must.strip()))
                    for arg in config.spec.containers.args:
                        file.write("@click.option('--" + arg.key + "')\n")
                    for output in config.spec.containers.outputs:
                        file.write("@click.option('--" + output.key + "')\n")
                    file.write("""def debug_execute(**kwargs):
    try:
        args = {"name": "$alfred_name"}
        outputs = [$alfred_outputs]

        args.update(kwargs)
        result = exec_before(**args)

        args.update(result if isinstance(result, dict) else {})
        result = execute(**args)

        args.update(result if isinstance(result, dict) else {})
        result = exec_after(outputs=outputs, **args)

        return result
    except Exception as e:
        logger = phs3logger(kwargs["job_id"])
        logger.error(traceback.format_exc())
        print(traceback.format_exc())
        raise e
"""
                               .replace('$alfred_outputs', ', '.join(['"'+output.key+'"' for output in config.spec.containers.outputs])) \
                               .replace('$alfred_name', config.metadata.name)
                               )
                else:
                    file.write(line)

    def jupyter_to_c9(self, dag_full_path, **kwargs):
        im = kwargs['im']
        om = kwargs['om']
        ipynb_dict = kwargs['ipynb_dict']

        self.phs3.download(dv.TEMPLATE_BUCKET, dv.CLI_VERSION + dv.TEMPLATE_PHJOB_FILE_PY, dag_full_path + "/phjob.py")
        with open(dag_full_path + "/phjob.py", "a") as file:
            file.write("""def execute(**kwargs):
    \"\"\"
        please input your code below
        get spark session: spark = kwargs["spark"]()
    \"\"\"
    spark = kwargs['spark']()
    logger = phs3logger(kwargs["job_id"], LOG_DEBUG_LEVEL)

""")
            # 取参数
            for input in im:
                file.write("    {key} = kwargs['{key}']\n".format(key=input))
            for output in om:
                file.write("    {key} = kwargs['{key}']\n".format(key=output))
            file.write("\n")

            # copy 逻辑代码
            for cell in ipynb_dict['cells'][2:]:
                for row in cell['source']:
                    row = re.sub(r'(^\s*)print(\(.*)', r"\1logger.debug\2", row)
                    file.write('    '+row)
                file.write('\r\n')
                file.write('\r\n')

    def c9_create(self, **kwargs):
        # 1. /__init__.py file
        self.c9_create_init()

        # 2. /phjob.py file
        self.phs3.download(dv.TEMPLATE_BUCKET, dv.CLI_VERSION + dv.TEMPLATE_PHJOB_FILE_PY, self.job_path + "/phjob.py")
        with open(self.job_path + "/phjob.py", "a") as file:
            file.write("""def execute(**kwargs):
    \"\"\"
        please input your code below\n""")

            if self.command == 'submit':
                file.write('        get spark session: spark = kwargs["spark"]()\n')

            file.write("""    \"\"\"
    logger = phs3logger(kwargs["job_id"], LOG_DEBUG_LEVEL)
    logger.info("当前 owner 为 " + str(kwargs["owner"]))
    logger.info("当前 run_id 为 " + str(kwargs["run_id"]))
    logger.info("当前 job_id 为 " + str(kwargs["job_id"]))
""")

            if self.command == 'submit':
                file.write('    spark = kwargs["spark"]()')

            file.write("""
    logger.info(kwargs["a"])
    logger.info(kwargs["b"])
    logger.info(kwargs["c"])
    logger.info(kwargs["d"])
    return {}
""")

        # 3. /phmain.py file
        self.c9_create_phmain()

    def jupyter_create(self, **kwargs):
        path = self.job_path + ".ipynb"
        dir_path = "/".join(path.split('/')[:-1])
        subprocess.call(['mkdir', '-p', dir_path])

        f_lines = self.phs3.open_object_by_lines(dv.TEMPLATE_BUCKET, dv.CLI_VERSION + dv.TEMPLATE_JUPYTER_PYTHON_FILE)
        with open(path, "w") as file:
            for line in f_lines:
                line = line.replace('$name', self.name) \
                            .replace('$runtime', self.runtime) \
                            .replace('$command', self.command) \
                            .replace('$timeout', str(self.timeout)) \
                            .replace('$user', os.getenv('USER', 'unknown')) \
                            .replace('$group', self.group) \
                            .replace('$ide', self.ide)
                file.write(line)

    def create(self, **kwargs):
        if self.ide == 'c9':
            self.c9_create(**kwargs)
        elif self.ide == 'jupyter':
            self.jupyter_create(**kwargs)
        else:
            raise exception_function_not_implement

    def submit_run(self, **kwargs):
        submit_conf = {
            "jars": "s3a://ph-platform/2020-11-11/jobs/python/phcli/common/aws-java-sdk-bundle-1.11.828.jar,"
                    "s3a://ph-platform/2020-11-11/jobs/python/phcli/common/hadoop-aws-3.2.1.jar",
        }
        submit_file = {
            "py-files": "s3a://" + dv.TEMPLATE_BUCKET + "/" + dv.CLI_VERSION + dv.DAGS_S3_PHJOBS_PATH + "common/phcli-{}-py3.8.egg,".format(CLI_CLIENT_VERSION) +
                        self.submit_prefix + "phjob.py",
        }
        submit_main = self.submit_prefix + "phmain.py"

        super().submit_run(submit_conf=submit_conf,
                           submit_file=submit_file,
                           submit_main=submit_main)

    def script_run(self, **kwargs):
        self.phs3.download(dv.TEMPLATE_BUCKET, dv.CLI_VERSION + self.s3_job_path + "/phmain.py", 'phmain.py')
        self.phs3.download(dv.TEMPLATE_BUCKET, dv.CLI_VERSION + self.s3_job_path + "/phjob.py", 'phjob.py')
        entrypoint = ['python3', './phmain.py']
        super().script_run(entrypoint=entrypoint)


