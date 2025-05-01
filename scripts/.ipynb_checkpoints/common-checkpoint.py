from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import uuid, time
from datetime import datetime

# Spark session
spark = SparkSession.builder \
    .appName("AriesConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,org.postgresql:postgresql:42.2.24") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "10") \
    .config("spark.executor.memory", "2g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

kafka_base_opts = {
    "kafka.bootstrap.servers": "localhost:9092",
    "startingOffsets": "earliest",
    "failOnDataLoss": "false",
    "maxOffsetsPerTrigger": "10000",
    "kafka.request.timeout.ms": "60000",
    "kafka.session.timeout.ms": "60000",
    "kafka.group.id": "aries-consumer-group"
}

jdbc_url = "jdbc:postgresql://localhost:5432/aries_db"
db_properties = {
    "user": "aries_user",
    "password": "admin",
    "driver": "org.postgresql.Driver",
    "batchsize": "1000"
}

valid_protocols = ["TCP", "UDP"]
valid_call_types = ["voice_in", "voice_out", "sms_in", "sms_out"]
valid_event_types = ["app_launch", "login", "logout", "payment", "SIM switch", "roaming event", "reboot"]
valid_network_types = ["4G", "5G", "3G", "WIFI", "LTE"]

cdr_schema = StructType([
    StructField("caller_id", StringType()), StructField("callee_id", StringType()),
    StructField("call_start", StringType()), StructField("call_duration", IntegerType()),
    StructField("call_type", StringType()), StructField("cell_id", StringType()),
    StructField("location", StringType()), StructField("imei", StringType()),
    StructField("is_fraud", BooleanType())
])

ipdr_schema = StructType([
    StructField("user_id", StringType()), StructField("timestamp", StringType()),
    StructField("domain", StringType()), StructField("ip_dst", StringType()),
    StructField("port", IntegerType()), StructField("protocol", StringType()),
    StructField("duration", IntegerType()), StructField("bytes_sent", IntegerType()),
    StructField("bytes_received", IntegerType()), StructField("vpn_usage", BooleanType()),
    StructField("is_fraud", BooleanType())
])

edr_schema = StructType([
    StructField("event_id", StringType()), StructField("user_id", StringType()),
    StructField("device_model", StringType()), StructField("os_type", StringType()),
    StructField("roaming_status", BooleanType()), StructField("network_type", StringType()),
    StructField("event_time", StringType()), StructField("location", StringType()),
    StructField("event_type", StringType()), StructField("is_fraud", BooleanType())
])

def parse_stream(df, schema):
    return df.selectExpr("CAST(value AS STRING) as json") \
             .select(from_json("json", schema).alias("data")) \
             .select("data.*")

def clean(df, required):
    for col_name in required:
        if col_name in df.columns:
            df = df.filter(col(col_name).isNotNull() & (trim(col(col_name)) != ""))
    return df.dropDuplicates()

def write_stream(df, table):
    def foreach_batch(batch_df, _):
        if not batch_df.rdd.isEmpty():
            batch_df.write.jdbc(jdbc_url, table, mode="append", properties=db_properties)
    return df.writeStream.foreachBatch(foreach_batch) \
             .option("checkpointLocation", f"/tmp/checkpoints/{table}") \
             .trigger(processingTime="10 seconds") \
             .start()
