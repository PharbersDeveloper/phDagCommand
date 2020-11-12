# -*- coding: utf-8 -*-

import os
import subprocess

from ph_max_auto import define_value as dv
from ph_max_auto.phconfig.phconfig import PhYAMLConfig


def create(job_path, phs3):
    # 1. /__init.py file
    subprocess.call(["touch", job_path + "/__init__.py"])

    # 2. /phjob.py file
    phs3.download(dv.TEMPLATE_BUCKET, dv.CLI_VERSION + dv.TEMPLATE_PHJOB_FILE_PY, job_path + "/phjob.py")
    config = PhYAMLConfig(job_path)
    config.load_yaml()

    with open(job_path + "/phjob.py", "a") as file:
        file.write("\ndef execute(**kwargs):\n")
        file.write('\t"""\n')
        file.write('\t\tplease input your code below\n')
        file.write('\t"""\n')
        file.write('\tprint(kwargs["a"])\n')
        file.write('\tprint(kwargs["b"])\n')

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
                for arg in config.spec.containers.args:
                    file.write("@click.option('--" + arg.key + "')\n")
                file.write("def debug_execute(**kwargs):\n")
                file.write("\texecute(**kwargs)")
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
                    "s3a://" + dv.DAGS_S3_BUCKET + "/" + dv.DAGS_S3_PHJOBS_PATH + "common/phcli.zip," +
                    submit_prefix + "phjob.py",
    }


def submit_main(submit_prefix):
    return submit_prefix + "phmain.py"
