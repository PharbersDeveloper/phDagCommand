import subprocess

from .ph_rt_base import PhRTBase
from phcli.ph_max_auto import define_value as dv
from phcli.ph_max_auto.ph_config.phconfig.phconfig import PhYAMLConfig


class PhRTPython3(PhRTBase):
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def create(self, **kwargs):
        # 1. /__init.py file
        subprocess.call(["touch", self.job_path + "/__init__.py"])

        # 2. /phjob.py file
        self.phs3.download(dv.TEMPLATE_BUCKET, dv.CLI_VERSION + dv.TEMPLATE_PHJOB_FILE_PY, self.job_path + "/phjob.py")
        config = PhYAMLConfig(self.job_path)
        config.load_yaml()

        with open(self.job_path + "/phjob.py", "a") as file:
            file.write("""def execute(**kwargs):
    \"\"\"
        please input your code below\n""")

            if self.command == 'submit':
                file.write('        get spark session: spark = kwargs["spark"]()\n')

            file.write("""        \"\"\"
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
        f_lines = self.phs3.open_object_by_lines(dv.TEMPLATE_BUCKET, dv.CLI_VERSION + dv.TEMPLATE_PHMAIN_FILE_PY)
        with open(self.job_path + "/phmain.py", "w") as file:
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
        raise e
"""
                           .replace('$alfred_outputs', ', '.join(['"'+output.key+'"' for output in config.spec.containers.outputs])) \
                           .replace('$alfred_name', config.metadata.name)
                           )
                else:
                    file.write(line)

    def submit_run(self, **kwargs):
        submit_conf = {
            "jars": "s3a://ph-platform/2020-11-11/jobs/python/phcli/common/aws-java-sdk-bundle-1.11.828.jar,"
                    "s3a://ph-platform/2020-11-11/jobs/python/phcli/common/hadoop-aws-3.2.1.jar",
        }
        submit_file = {
            "py-files": "s3a://" + dv.TEMPLATE_BUCKET + "/" + dv.CLI_VERSION + dv.DAGS_S3_PHJOBS_PATH + "common/phcli-2.0.0-py3.8.egg," +
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


