import os
from ph_db.ph_postgresql.ph_pg import PhPg
from ph_max_auto.ph_models.phentry import DataSet


def exec_before(*args, **kwargs):
    name = kwargs.pop('name', '')
    job_id = kwargs.pop('job_id', '')
    os.environ["PYSPARK_PYTHON"] = "python3"

    def spark():
        from pyspark.sql import SparkSession
        spark = SparkSession.builder \
            .master("yarn") \
            .appName(name+'_'+job_id) \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.cores", "2") \
            .config("spark.executor.instances", "2") \
            .config("spark.executor.memory", "2g") \
            .config('spark.sql.codegen.wholeStage', False) \
            .config("spark.sql.execution.arrow.enabled", "true") \
            .getOrCreate()
        access_key = os.getenv("AWS_ACCESS_KEY_ID")
        secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        if access_key is not None:
            spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
            spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)
            spark._jsc.hadoopConfiguration().set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
            spark._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
            # spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider")
            spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.cn-northwest-1.amazonaws.com.cn")
        return spark

    return {'spark': spark}


def exec_after(*args, **kwargs):
    job_id = kwargs.pop('job_id', None)
    if not job_id: # job_id 为空判定位测试环境，不管理血统
        return None

    outputs = kwargs.pop('outputs', [])
    inputs = set(kwargs.keys()).difference(outputs)

    pg = PhPg()
    for input in inputs:
        obj = pg.insert(DataSet(job=job_id, name=input, source=kwargs[input]))
        kwargs[input] = obj
    inputs = [kwargs[input].id for input in inputs]

    for output in outputs:
        pg.insert(DataSet(parent=inputs, job=job_id, name=output, source=kwargs[output]))

    return None
