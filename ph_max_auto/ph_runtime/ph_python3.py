# -*- coding: utf-8 -*-

import os
import subprocess

from ph_max_auto import define_value as dv
from ph_max_auto.phconfig.phconfig import PhYAMLConfig


def create(path, phs3):
    # 1. /__init.py file
    subprocess.call(["touch", path + "/__init__.py"])

    # 2. /phjob.py file
    phs3.download(dv.TEMPLATE_BUCKET, dv.CLI_VERSION + dv.TEMPLATE_PHJOB_FILE_PY, path + "/phjob.py")
    config = PhYAMLConfig(path)
    config.load_yaml()

    with open(path + "/phjob.py", "a") as file:
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
        file.write('\tprint(a)\n')
        file.write('\tprint(b)\n')

    # 3. /phmain.py file
    f_lines = phs3.open_object_by_lines(dv.TEMPLATE_BUCKET, dv.CLI_VERSION + dv.TEMPLATE_PHMAIN_FILE_PY)
    with open(path + "/phmain.py", "w") as file:
        s = []
        for arg in config.spec.containers.args:
            s.append(arg.key)

        for line in f_lines:
            line = line + "\n"
            if line == "$alfred_debug_execute\n":
                file.write("@click.command()\n")
                for arg in config.spec.containers.args:
                    file.write("@click.option('--" + arg.key + "')\n")
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


def publish(dag_path, phs3):
    for _, dirs, _ in os.walk(dag_path):
        for key in dirs:
            if (not key.startswith(".")) and (not key.startswith("__pycache__")):
                phs3.upload(dag_path + key + "/phmain.py",
                            dv.DAGS_S3_BUCKET,
                            dv.DAGS_S3_PHJOBS_PATH + key + "/phmain.py")
                phs3.upload(dag_path + key + "/phjob.py",
                            dv.DAGS_S3_BUCKET,
                            dv.DAGS_S3_PHJOBS_PATH + key + "/phjob.py")
                phs3.upload(dag_path + key + "/phconf.yaml",
                            dv.DAGS_S3_BUCKET,
                            dv.DAGS_S3_PHJOBS_PATH + key + "/phconf.yaml")
                phs3.upload(dag_path + key + "/args.properties",
                            dv.DAGS_S3_BUCKET,
                            dv.DAGS_S3_PHJOBS_PATH + key + "/args.properties")


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
