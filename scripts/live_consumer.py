from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

import uuid
import json
import time
from datetime import datetime

spark = SparkSession.builder \
    .appName("AriesLiveConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,org.postgresql:postgresql:42.2.24") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "10") \
    .config("spark.executor.memory", "2g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

kafka_options = {
    "kafka.bootstrap.servers": "localhost:9092",
    "subscribe": "cdr,edr,ipdr",
    "startingOffsets": "earliest",
    "failOnDataLoss": "false",
    "maxOffsetsPerTrigger": "10000"
}

jdbc_url = "jdbc:postgresql://localhost:5432/aries_db"
db_properties = {
    "user": "aries_user",
    "password": "admin",
    "driver": "org.postgresql.Driver",
    "batchsize": "1000",
    "rewriteBatchedStatements": "true"
}

processing_time = "30 seconds"
checkpoint_dir = "/tmp/aries_live_checkpoints/"

cdr_schema = StructType([
    StructField("caller_id", StringType(), True),
    StructField("callee_id", StringType(), True),
    StructField("call_start", StringType(), True),
    StructField("call_duration", IntegerType(), True),
    StructField("call_type", StringType(), True),
    StructField("cell_id", StringType(), True),
    StructField("location", StringType(), True),
    StructField("imei", StringType(), True),
    StructField("is_fraud", BooleanType(), True)
])

ipdr_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("domain", StringType(), True),
    StructField("ip_dst", StringType(), True),
    StructField("port", IntegerType(), True),
    StructField("protocol", StringType(), True),
    StructField("duration", IntegerType(), True),
    StructField("bytes_sent", IntegerType(), True),
    StructField("bytes_received", IntegerType(), True),
    StructField("vpn_usage", BooleanType(), True),
    StructField("is_fraud", BooleanType(), True)
])

edr_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("device_model", StringType(), True),
    StructField("os_type", StringType(), True),
    StructField("roaming_status", BooleanType(), True),
    StructField("network_type", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("location", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("is_fraud", BooleanType(), True)
])

valid_protocols = ["TCP", "UDP"]
valid_call_types = ["voice_in", "voice_out", "sms_in", "sms_out"]
valid_event_types = ["app_launch", "login", "logout", "payment", "SIM switch", "roaming event"]
valid_network_types = ["4G", "5G", "3G", "WIFI", "LTE"]

def log_metrics(batch_id, table, count_before, count_after, start_time):
    processing_time = time.time() - start_time
    metrics = {
        "batch_id": batch_id,
        "table": table,
        "records_before_cleaning": count_before,
        "records_after_cleaning": count_after,
        "dropped_records": count_before - count_after,
        "processing_time_seconds": processing_time,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    print(json.dumps(metrics))

def clean_dataframe(df, record_type):

    # Define required fields per type
    if record_type == "cdr":
        key_fields = ["caller_id", "callee_id", "call_start"]
        required_fields = [
            "caller_id", "callee_id", "call_start", "call_duration", "call_type",
            "cell_id", "location", "imei", "is_fraud"
        ]
    elif record_type == "ipdr":
        key_fields = ["user_id", "timestamp", "domain"]
        required_fields = [
            "user_id", "timestamp", "domain", "ip_dst", "port", "protocol",
            "duration", "bytes_sent", "bytes_received", "vpn_usage", "is_fraud"
        ]
    elif record_type == "edr":
        key_fields = ["event_id", "user_id", "event_time"]
        required_fields = [
            "event_id", "user_id", "device_model", "os_type", "roaming_status",
            "network_type", "event_time", "location", "event_type", "is_fraud"
        ]
    else:
        key_fields = df.columns[:3]
        required_fields = df.columns

    # Clean all columns based on datatype
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(
                field.name,
                when((col(field.name).isNull()) | (trim(col(field.name)) == "") | 
                     (lower(col(field.name)) == "nan") | (lower(col(field.name)) == '"nan"'),
                     lit("unknown"))
                .otherwise(trim(regexp_replace(lower(col(field.name)), r'^"|"$', '')))
            )
        elif isinstance(field.dataType, IntegerType):
            df = df.withColumn(
                field.name,
                when(col(field.name).isNull() | isnan(col(field.name)), lit(0)).otherwise(col(field.name).cast("int"))
            )
        elif isinstance(field.dataType, BooleanType):
            df = df.withColumn(
                field.name,
                when(col(field.name).isNull(), lit(False)).otherwise(col(field.name))
            )
        elif isinstance(field.dataType, TimestampType):
            df = df.withColumn(
                field.name,
                when(col(field.name).isNull(), lit(None)).otherwise(col(field.name))
            )

    # Drop rows with null or "unknown" keys
    for key in key_fields:
        df = df.filter((col(key).isNotNull()) & (col(key) != "unknown"))

    # Drop duplicates
    df = df.dropDuplicates(key_fields)

    # Cast and correct data types (only for known fields)
    if record_type == "cdr":
        df = df.withColumn("call_duration", col("call_duration").cast("int"))
        df = df.withColumn("is_fraud", col("is_fraud").cast("boolean"))

    elif record_type == "ipdr":
        df = df.withColumn("port", col("port").cast("int"))
        df = df.withColumn("duration", col("duration").cast("int"))
        df = df.withColumn("bytes_sent", col("bytes_sent").cast("int"))
        df = df.withColumn("bytes_received", col("bytes_received").cast("int"))
        df = df.withColumn("vpn_usage", col("vpn_usage").cast("boolean"))
        df = df.withColumn("is_fraud", col("is_fraud").cast("boolean"))

    elif record_type == "edr":
        df = df.withColumn("roaming_status", col("roaming_status").cast("boolean"))
        df = df.withColumn("is_fraud", col("is_fraud").cast("boolean"))

    # Keep only the required fields
    df = df.select(*required_fields)

    return df


def write_to_postgres(df, table_name):
    def batch_func(batch_df, batch_id):
        start_time = time.time()
        batch_id_str = str(uuid.uuid4())

        if batch_df.rdd.isEmpty():
            print(f"[INFO] Skipping empty batch for table: {table_name}")
            return

        try:
            count_before = batch_df.count()

            # Enrich batch with metadata
            processed_df = batch_df.withColumn("batch_id", lit(batch_id_str)) \
                                   .withColumn("processing_timestamp", current_timestamp())

            processed_df.write \
                .option("batchsize", 1000) \
                .jdbc(url=jdbc_url, table=table_name, mode="append", properties=db_properties)

            metrics_df = spark.createDataFrame([(
                batch_id_str,
                table_name,
                count_before,
                count_before,  # assuming deduplication is already done earlier
                0,
                round(time.time() - start_time, 2),
                current_timestamp()
            )], [
                "batch_id", "table", "records_before", "records_after", "dropped_records",
                "processing_time_seconds", "timestamp"
            ])

            metrics_df.write \
                .jdbc(url=jdbc_url, table="batch_metrics", mode="append", properties=db_properties)

        except Exception as e:
            error_metrics = {
                "batch_id": batch_id_str,
                "table": table_name,
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
            print(json.dumps(error_metrics))

    return df.writeStream \
        .trigger(processingTime=processing_time) \
        .option("checkpointLocation", f"{checkpoint_dir}{table_name}") \
        .foreachBatch(batch_func) \
        .outputMode("append") \
        .start()


def process_streaming_data():
    print("Starting streaming data processing from Kafka")

    raw_df = spark.readStream.format("kafka").options(**kafka_options).load()

    # --- CDR ---
    cdr_df = raw_df.filter(col("topic") == "cdr") \
                   .selectExpr("CAST(value AS STRING)") \
                   .select(from_json("value", cdr_schema).alias("data")) \
                   .select("data.*")

    cdr_df = cdr_df.withColumn("call_start", to_timestamp(col("call_start"), "yyyy-MM-dd HH:mm:ss")) \
                   .withColumn("call_duration", col("call_duration").cast("int")) \
                   .withColumn("is_fraud", col("is_fraud").cast("boolean"))

    cdr_df = clean_dataframe(cdr_df, "cdr")

    # --- IPDR ---
    ipdr_df = raw_df.filter(col("topic") == "ipdr") \
                    .selectExpr("CAST(value AS STRING)") \
                    .select(from_json("value", ipdr_schema).alias("data")) \
                    .select("data.*")

    ipdr_df = ipdr_df.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")) \
                     .withColumn("port", col("port").cast("int")) \
                     .withColumn("duration", col("duration").cast("int")) \
                     .withColumn("bytes_sent", col("bytes_sent").cast("int")) \
                     .withColumn("bytes_received", col("bytes_received").cast("int")) \
                     .withColumn("vpn_usage", col("vpn_usage").cast("boolean")) \
                     .withColumn("is_fraud", col("is_fraud").cast("boolean"))

    ipdr_df = clean_dataframe(ipdr_df, "ipdr")

    # --- EDR ---
    edr_df = raw_df.filter(col("topic") == "edr") \
                   .selectExpr("CAST(value AS STRING)") \
                   .select(from_json("value", edr_schema).alias("data")) \
                   .select("data.*")

    edr_df = edr_df.withColumn("event_time", to_timestamp(col("event_time"), "yyyy-MM-dd HH:mm:ss")) \
                   .withColumn("roaming_status", col("roaming_status").cast("boolean")) \
                   .withColumn("is_fraud", col("is_fraud").cast("boolean"))

    edr_df = clean_dataframe(edr_df, "edr")

    standardize_schema(cdr_df, "cdr")
    standardize_schema(ipdr_df, "ipdr")
    standardize_schema(edr_df, "edr")

    # Write streams
    cdr_stream = write_to_postgres(cdr_df, "cdr")
    ipdr_stream = write_to_postgres(ipdr_df, "ipdr")
    edr_stream = write_to_postgres(edr_df, "edr")

    spark.streams.awaitAnyTermination()


def standardize_schema(df, record_type):
    if record_type == "cdr":
        df = df.select(
            col("caller_id").cast(StringType()).alias("caller_id"),
            col("callee_id").cast(StringType()).alias("callee_id"),
            col("call_start").cast(TimestampType()).alias("call_start"),
            col("call_duration").cast(IntegerType()).alias("call_duration"),
            col("call_type").cast(StringType()).alias("call_type"),
            col("cell_id").cast(StringType()).alias("cell_id"),
            col("location").cast(StringType()).alias("location"),
            col("imei").cast(StringType()).alias("imei"),
            col("is_fraud").cast(BooleanType()).alias("is_fraud")
        )

    elif record_type == "ipdr":
        df = df.select(
            col("user_id").cast(StringType()).alias("user_id"),
            col("timestamp").cast(TimestampType()).alias("timestamp"),
            col("domain").cast(StringType()).alias("domain"),
            col("ip_dst").cast(StringType()).alias("ip_dst"),
            col("port").cast(IntegerType()).alias("port"),
            col("protocol").cast(StringType()).alias("protocol"),
            col("duration").cast(IntegerType()).alias("duration"),
            col("bytes_sent").cast(IntegerType()).alias("bytes_sent"),
            col("bytes_received").cast(IntegerType()).alias("bytes_received"),
            col("vpn_usage").cast(BooleanType()).alias("vpn_usage"),
            col("is_fraud").cast(BooleanType()).alias("is_fraud")
        )

    elif record_type == "edr":
        df = df.select(
            col("event_id").cast(StringType()).alias("event_id"),
            col("user_id").cast(StringType()).alias("user_id"),
            col("device_model").cast(StringType()).alias("device_model"),
            col("os_type").cast(StringType()).alias("os_type"),
            col("roaming_status").cast(BooleanType()).alias("roaming_status"),
            col("network_type").cast(StringType()).alias("network_type"),
            col("event_time").cast(TimestampType()).alias("event_time"),
            col("location").cast(StringType()).alias("location"),
            col("event_type").cast(StringType()).alias("event_type"),
            col("is_fraud").cast(BooleanType()).alias("is_fraud")
        )

    else:
        raise ValueError(f"Unknown record type: {record_type}")

    return df


if __name__ == "__main__":
    print("Starting Aries Live Consumer")
    process_streaming_data()