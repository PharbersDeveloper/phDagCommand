# -*- coding: utf-8 -*-

import subprocess

from ph_max_auto import define_value as dv
from ph_max_auto.ph_config.phconfig.phconfig import PhYAMLConfig


def create(job_path, phs3):
    # 1. /__init.py file
    subprocess.call(["touch", job_path + "/__init__.py"])

    # 2. /phjob.py file
    phs3.download(dv.TEMPLATE_BUCKET, dv.CLI_VERSION + dv.TEMPLATE_PHJOB_FILE_PY, job_path + "/phjob.py")
    config = PhYAMLConfig(job_path)
    config.load_yaml()

    with open(job_path + "/phjob.py", "a") as file:
        file.write("""def execute(**kwargs):
    \"\"\"
        please input your code below
        get spark session: spark = kwargs["spark"]()
    \"\"\"
    logger = phs3logger(kwargs["job_id"])
    spark = kwargs["spark"]()
    logger.info(kwargs["a"])
    logger.info(kwargs["b"])
    logger.info(kwargs["c"])
    logger.info(kwargs["d"])
    return {}
""")

    # 3. /phmain.py file
    f_lines = phs3.open_object_by_lines(dv.TEMPLATE_BUCKET, dv.CLI_VERSION + dv.TEMPLATE_PHMAIN_FILE_PY)
    with open(job_path + "/phmain.py", "w") as file:
        s = []
        for arg in config.spec.containers.args:
            s.append(arg.key)

        for line in f_lines:
            line = line + "\n"
            if line == "$alfred_debug_execute\n":
                file.write("@click.command()\n")
                file.write("@click.option('--job_id')\n")
                for arg in config.spec.containers.args:
                    file.write("@click.option('--" + arg.key + "')\n")
                for output in config.spec.containers.outputs:
                    file.write("@click.option('--" + output.key + "')\n")
                file.write("""def debug_execute(**kwargs):
    try:
        exec_after(outputs=[$alfred_outputs], **dict(
            kwargs,
            **execute(**dict(
                kwargs,
                **exec_before(**dict(
                    kwargs,
                    **{'name': '$alfred_name'}
                ))
            ))
        ))
    except Exception:
        logger = phs3logger(kwargs["job_id"])
        logger.error(traceback.format_exc())
"""
                           .replace('$alfred_outputs', ', '.join(['"'+output.key+'"' for output in config.spec.containers.outputs])) \
                           .replace('$alfred_name', config.metadata.name)
                )
            else:
                file.write(line)


def submit_conf(path, phs3, runtime):
    return {
        "spark.pyspark.python": "/usr/bin/"+runtime,
        "jars": "s3a://ph-stream/jars/aws/aws-java-sdk-1.11.682.jar,"
                "s3a://ph-stream/jars/aws/aws-java-sdk-core-1.11.682.jar,"
                "s3a://ph-stream/jars/aws/aws-java-sdk-s3-1.11.682.jar,"
                "s3a://ph-stream/jars/hadoop/hadoop-aws-2.9.2.jar",
    }


def submit_file(submit_prefix):
    return {
        "py-files": "s3a://" + dv.DAGS_S3_BUCKET + "/" + dv.DAGS_S3_PHJOBS_PATH + "common/click.zip," +
                    "s3a://" + dv.DAGS_S3_BUCKET + "/" + dv.DAGS_S3_PHJOBS_PATH + "common/phcli-1.0.1-py3.8.egg," +
                    submit_prefix + "phjob.py",
    }


def submit_main(submit_prefix):
    return submit_prefix + "phmain.py"
