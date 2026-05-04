import sys
import os
project_root = os.environ.get("PROJECT_ROOT", r"N:\DE\CC Transaction Pipeline")
sys.path.append(project_root)

import cfg.config as config

from pyspark.sql import SparkSession


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

brz_dim_customer = spark.read \
        .format('csv') \
        .option('header','True') \
        .load(os.path.join(config.LANDING_DIR, 'dim_customer.csv'))

brz_dim_customer.write \
    .format('parquet') \
    .mode('overwrite') \
    .save('s3a://cc-transaction-pipeline/brz/brz_dim_customer/')

brz_dim_merchant = spark.read \
        .format('csv') \
        .option('header','True') \
        .load(os.path.join(config.LANDING_DIR, 'dim_merchant.csv'))

brz_dim_merchant.write \
    .format('parquet') \
    .mode('overwrite') \
    .save('s3a://cc-transaction-pipeline/brz/brz_dim_merchant/')

brz_dim_card = spark.read \
        .format('csv') \
        .option('header','True') \
        .load(os.path.join(config.LANDING_DIR, 'dim_card.csv'))

brz_dim_card.write \
    .format('parquet') \
    .mode('overwrite') \
    .save('s3a://cc-transaction-pipeline/brz/brz_dim_card/')

brz_fact_transactions = spark.read \
        .format('csv') \
        .option('header','True') \
        .option('inferSchema','True') \
        .load(os.path.join(config.LANDING_DIR, 'fact_transactions.csv'))

brz_fact_transactions = brz_fact_transactions.withColumn('year',brz_fact_transactions['trans_date_trans_time'].substr(1,4)) \
                        .withColumn('month',brz_fact_transactions['trans_date_trans_time'].substr(6,2)) \
                        .withColumn('day',brz_fact_transactions['trans_date_trans_time'].substr(9,2))

brz_fact_transactions.write \
    .format('parquet') \
    .mode('overwrite') \
    .partitionBy('year','month','day') \
    .save('s3a://cc-transaction-pipeline/brz/brz_fact_transactions/')
