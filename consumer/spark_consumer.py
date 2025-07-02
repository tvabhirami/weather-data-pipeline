from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, regexp_replace
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
aws_region = os.getenv("AWS_REGION")
s3_bucket = os.getenv("S3_BUCKET")
s3_prefix = os.getenv("S3_KEY_PREFIX")
kafka_topic = os.getenv("KAFKA_TOPIC")
kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS")

schema = StructType() \
    .add("city", StringType()) \
    .add("temperature", DoubleType()) \
    .add("description", StringType()) \
    .add("timestamp", StringType())

spark = SparkSession.builder \
    .appName("KafkaSparkWeatherConsumer") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.11.901") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# S3 access configs
hadoop_conf = spark._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.access.key", aws_access_key)
hadoop_conf.set("fs.s3a.secret.key", aws_secret_key)
hadoop_conf.set("fs.s3a.endpoint", "s3.amazonaws.com")
hadoop_conf.set("fs.s3a.region", aws_region)
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

# Read from Kafka
df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .load()

json_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Replace spaces in city names
json_df = json_df.withColumn("city", regexp_replace("city", " ", "_"))

# Optional: view data
json_df.show(5)

# Write to S3
json_df.write \
    .mode("append") \
    .format("csv") \
    .option("path", f"s3a://{s3_bucket}/{s3_prefix}/spark_output/") \
    .option("header", "true") \
    .partitionBy("city") \
    .save()

spark.stop()
