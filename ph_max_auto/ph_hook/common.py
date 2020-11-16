import os

def exec_before(*args, **kwargs):
    name = kwargs['name']
    os.environ["PYSPARK_PYTHON"] = "python3"

    def spark():
        print('abc')
        from pyspark.sql import SparkSession
        spark = SparkSession.builder \
            .master("yarn") \
            .appName(name) \
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
    return kwargs
