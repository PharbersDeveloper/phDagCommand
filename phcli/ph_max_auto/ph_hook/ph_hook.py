import os
import base64
import boto3
import time
from datetime import datetime
from phcli.ph_db.ph_pg import PhPg
from phcli.ph_max_auto.ph_models.data_set import DataSet
from phcli.ph_max_auto import define_value as dv
from phcli.ph_max_auto.ph_models.data_save_path import get_result_path_prefix,get_target_path



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
    
    result_path_prefix = get_result_path_prefix(kwargs)


    return {
        'spark': spark,
        'result_path_prefix': result_path_prefix
        }


def exec_after(*args, **kwargs):
    owner = kwargs.pop('owner', None)
    run_id = kwargs.pop('run_id', None)
    job_id = kwargs.pop('job_id', None)
    
    
    
    # job_id 为空判定为测试环境，不管理系统
    if not job_id:
        return

    outputs = kwargs.pop('outputs', [])
    inputs = list(set(kwargs.keys()).difference(outputs))
    outputs = [output for output in outputs if kwargs[output] and str(kwargs[output]).startswith('s3a://')]
    inputs = [input for input in inputs if kwargs[input] and str(kwargs[input]).startswith('s3a://')]

    # 没有输出需要记录，直接退出
    if not outputs:
        return
    
    source_bucket_name = kwargs['path_prefix'].split("/")[2]
    source_path_prefix = '/'.join(kwargs['result_path'].split('/')[3:])
    target_path = get_target_path(kwargs)
    
    # 在s3进行copy
    s3_resource = boto3.resource('s3')
    s3_client = boto3.client("s3")
    res = s3_client.list_objects(Bucket=source_bucket_name, Prefix=source_path_prefix)
    for item in res['Contents']:
        copy_source = {
            'Bucket': bucket_name,
            'Key': item['Key']
        }
        source_filename = item['Key'].split('/')[-1]
        s3_resource.meta.client.copy(copy_source, dv.TARGET_BUCKET_NAME, target_path + source_filename)


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

if __name__ == '__main__':
    
    kwargs = {
        'path_prefix': 's3://ph-max-auto/2020-08-11/data_matching/refactor/runs',
        'result_path': 's3://ph-max-auto/2020-08-11/data_matching/refactor/runs/manual__2021-01-18T04:49:20.117595+00:00/cleaning_data_normalization/cleaning_result/',
        'dag_name': 'test_dag',
        'job_name': 'test_job',
        'run_id': 'test_run_id'
    }
    
    source_bucket_name = kwargs['path_prefix'].split("/")[2]
    source_path_prefix = '/'.join(kwargs['result_path'].split('/')[3:])
    target_path = get_target_path(kwargs)
    
    # 在s3进行copy
    s3_resource = boto3.resource('s3')
    s3_client = boto3.client("s3")
    res = s3_client.list_objects(Bucket=source_bucket_name, Prefix=source_path_prefix)
    for item in res['Contents']:
        copy_source = {
            'Bucket': bucket_name,
            'Key': item['Key']
        }
        source_filename = item['Key'].split('/')[-1]
        s3_resource.meta.client.copy(copy_source, dv.TARGET_BUCKET_NAME, target_path + source_filename)
