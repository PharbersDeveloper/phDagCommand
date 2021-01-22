import os
import base64
from datetime import datetime
from phcli.ph_db.ph_pg import PhPg
from phcli.ph_max_auto.ph_models.data_set import DataSet


def exec_before(*args, **kwargs):
    name = kwargs.pop('name', None)
    job_id = kwargs.pop('job_id', name)

    def spark():
        from pyspark.sql import SparkSession
        os.environ["PYSPARK_PYTHON"] = "python3"
        spark = SparkSession.builder \
            .master("yarn") \
            .appName(str(job_id)) \
            .config('spark.sql.codegen.wholeStage', False) \
            .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
            .enableHiveSupport() \
            .getOrCreate()

        access_key = os.getenv("AWS_ACCESS_KEY_ID")
        secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        if access_key is not None:
            spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
            spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)
            spark._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
            spark._jsc.hadoopConfiguration().set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
            # spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider")
            spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.cn-northwest-1.amazonaws.com.cn")
        return spark

    return {'spark': spark}


def exec_after(*args, **kwargs):
    owner = kwargs.pop('owner', None)
    run_id = kwargs.pop('run_id', None)
    job_id = kwargs.pop('job_id', None)

    # job_id 为空判定为测试环境，不管理血统
    if not job_id:
        return

    outputs = kwargs.pop('outputs', [])
    inputs = list(set(kwargs.keys()).difference(outputs))
    outputs = [output for output in outputs if kwargs[output] and str(kwargs[output]).startswith('s3a://')]
    inputs = [input for input in inputs if kwargs[input] and str(kwargs[input]).startswith('s3a://')]

    # 没有输出需要记录，直接退出
    if not outputs:
        return

    pg = PhPg(
        base64.b64decode('cGgtZGItbGFtYmRhLmNuZ2sxamV1cm1udi5yZHMuY24tbm9ydGh3ZXN0LTEuYW1hem9uYXdzLmNvbS5jbgo=').decode('utf8')[:-1],
        base64.b64decode('NTQzMgo=').decode('utf8')[:-1],
        base64.b64decode('cGhhcmJlcnMK').decode('utf8')[:-1],
        base64.b64decode('QWJjZGUxOTYxMjUK').decode('utf8')[:-1],
        db=base64.b64decode('cGhlbnRyeQo=').decode('utf8')[:-1],
    )

    input_ids = []
    for input in inputs:
        obj = pg.query(DataSet(), source=kwargs[input])
        if obj:
            obj_id = obj[0].id
        else:
            obj = pg.insert(DataSet(job=job_id, name=input, source=kwargs[input]))
            obj_id = obj.id
        input_ids.append(obj_id)

    for output in outputs:
        obj = pg.query(DataSet(), source=kwargs[output])
        if obj:
            obj = obj[0]
            obj.parent = input_ids
            obj.modified = datetime.now()
            pg.update(obj)
        else:
            pg.insert(DataSet(parent=input_ids, job=job_id, name=output, source=kwargs[output]))

    pg.commit()
    return kwargs
