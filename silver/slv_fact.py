import sys
import os
project_root = os.environ.get("PROJECT_ROOT", r"N:\DE\CC Transaction Pipeline")
sys.path.append(project_root)

import cfg.config as config

from pyspark.sql import SparkSession


from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, TimestampType, FloatType, DoubleType
import pyspark.sql.functions as F

if os.name == 'nt':  # nt Windows posix Linux/Mac
    os.environ['HADOOP_HOME'] = r'C:\Users\nisha\hadoop'

spark = SparkSession.builder \
    .appName("CCPipeline") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.2,com.amazonaws:aws-java-sdk-bundle:1.12.367") \
    .config("spark.hadoop.fs.s3a.access.key", config.ACCESS_KEY_ID) \
    .config("spark.hadoop.fs.s3a.secret.key", config.ACCESS_KEY_SECRET) \
    .config("spark.hadoop.fs.s3a.fast.upload.buffer", "bytebuffer") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

slv_fact_transactions = spark.read \
        .format('parquet') \
        .load('s3a://cc-transaction-pipeline/brz/brz_fact_transactions/')

slv_fact_transactions = slv_fact_transactions[['trans_num', 'merch_uuid', 'cust_uuid', 'trans_date_trans_time', 'cc_num', 'category', 'amt', 'unix_time', 'is_fraud']]

slv_fact_transactions = slv_fact_transactions.withColumn('cc_num',F.col('cc_num').cast(StringType()))

slv_fact_transactions.write \
    .format('parquet') \
    .mode('overwrite') \
    .save('s3a://cc-transaction-pipeline/slv/slv_fact_transactions/')

