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

#dim Customer

slv_dim_customer = spark.read \
        .format('parquet') \
        .load('s3a://cc-transaction-pipeline/brz/brz_dim_customer/')

slv_dim_customer = slv_dim_customer.withColumn('zip',F.col('zip').cast(IntegerType()))
slv_dim_customer = slv_dim_customer.withColumn('lat',F.col('lat').cast(DoubleType()))
slv_dim_customer = slv_dim_customer.withColumn('long',F.col('long').cast(DoubleType()))
slv_dim_customer = slv_dim_customer.withColumn('city_pop',F.col('city_pop').cast(IntegerType()))
slv_dim_customer = slv_dim_customer.withColumn('dob',F.col('dob').cast(DateType()))

#Checking Nulls
# slv_dim_customer.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in slv_dim_customer.columns]).show()

slv_dim_customer.write \
    .format('parquet') \
    .mode('overwrite') \
    .save('s3a://cc-transaction-pipeline/slv/slv_dim_customer/')

#dim Merchant

slv_dim_merchant = spark.read \
        .format('parquet') \
        .load('s3a://cc-transaction-pipeline/brz/brz_dim_merchant/')

slv_dim_merchant = slv_dim_merchant.withColumn('merch_lat',F.col('merch_lat').cast(DoubleType()))
slv_dim_merchant = slv_dim_merchant.withColumn('merch_long',F.col('merch_long').cast(DoubleType()))

#Checking Nulls
# slv_dim_merchant.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in slv_dim_merchant.columns]).show()

# slv_dim_merchant.select('merchant').distinct().show(truncate=False)

slv_dim_merchant = slv_dim_merchant.withColumn('merchant',F.regexp_replace("merchant", "fraud_", " "))

slv_dim_merchant.write \
    .format('parquet') \
    .mode('overwrite') \
    .save('s3a://cc-transaction-pipeline/slv/slv_dim_merchant/')

#dim Card

slv_dim_card = spark.read \
        .format('parquet') \
        .load('s3a://cc-transaction-pipeline/brz/brz_dim_card/')

# slv_dim_card = slv_dim_card.withColumn('cc_num',F.col('cc_num').cast(IntegerType()))
slv_dim_card = slv_dim_card.withColumn('expiration_date',F.col('expiration_date').cast(DateType()))
slv_dim_card = slv_dim_card.withColumn('issue_date',F.col('issue_date').cast(DateType()))
slv_dim_card = slv_dim_card.withColumn('credit_limit',F.col('credit_limit').cast(IntegerType()))

#Checking Nulls
# slv_dim_card.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in slv_dim_card.columns]).show()

slv_dim_card.write \
    .format('parquet') \
    .mode('overwrite') \
    .save('s3a://cc-transaction-pipeline/slv/slv_dim_card/')


