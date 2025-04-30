from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, when, isnan, lit, length
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, TimestampType
from pyspark.sql.functions import regexp_replace

spark = SparkSession.builder \
    .appName("Aries") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,org.postgresql:postgresql:42.2.24") \
    .getOrCreate()

kafka_options = {
    "kafka.bootstrap.servers": "localhost:9092",
    "subscribe": "cdr,edr,ipdr",
    "startingOffsets": "earliest"
}

jdbc_url = "jdbc:postgresql://localhost:5432/aries_db"
db_properties = {
    "user": "aries_user",
    "password": "admin",
    "driver": "org.postgresql.Driver"
}

raw_df = spark.readStream.format("kafka").options(**kafka_options).load()

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

valid_protocols = ["TCP", "UDP", "HTTP", "HTTPS", "ICMP"]
valid_call_types = ["voice_in", "voice_out", "sms_in", "sms_out", "conference", "video_in", "video_out"]
valid_event_types = ["app_launch", "login", "logout", "payment", "SIM switch", "roaming event"]

def clean_dataframe(df, record_type):
    if record_type == "cdr":
        key_fields = ["caller_id", "callee_id", "call_start"]
    elif record_type == "ipdr":
        key_fields = ["user_id", "timestamp", "domain"]
    elif record_type == "edr":
        key_fields = ["event_id", "user_id", "event_time"]
    else:
        key_fields = df.columns[:3]

    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            default_value = "Unknown"
            if field.name == "protocol":
                default_value = "TCP"
            elif field.name == "call_type":
                default_value = "voice_in"
            elif field.name == "event_type":
                default_value = "app_launch"

            df = df.withColumn(field.name,
                when(
                    (col(field.name).isNull()) | 
                    (col(field.name) == "") | 
                    (col(field.name) == "NaN") | 
                    (col(field.name) == '"NaN"'),
                    lit(None)
                ).otherwise(
                    regexp_replace(col(field.name), r'^"|"$', '')
                )
            )
        elif isinstance(field.dataType, IntegerType):
            df = df.withColumn(field.name,
                when(col(field.name).isNull() | isnan(col(field.name)), 0)
                .otherwise(col(field.name))
            )
        elif isinstance(field.dataType, BooleanType):
            df = df.withColumn(field.name,
                when(col(field.name).isNull(), False)
                .otherwise(col(field.name))
            )

    if record_type == "ipdr":
        df = df.withColumn("protocol", 
            when(~col("protocol").isin(valid_protocols), "TCP")
            .otherwise(col("protocol"))
        )
    elif record_type == "cdr":
        df = df.withColumn("call_type", 
            when(~col("call_type").isin(valid_call_types), "voice_in")
            .otherwise(col("call_type"))
        )
    elif record_type == "edr":
        df = df.withColumn("event_type", 
            when(~col("event_type").isin(valid_event_types), "app_launch")
            .otherwise(col("event_type"))
        )

    # Drop rows where key fields are null or "Unknown"
    for key in key_fields:
        df = df.filter((col(key).isNotNull()) & (col(key) != "Unknown"))

    # Drop duplicates based on key fields
    df = df.dropDuplicates()

    return df


cdr_df = raw_df.filter(col("topic") == "cdr") \
               .selectExpr("CAST(value AS STRING)") \
               .select(from_json("value", cdr_schema).alias("data")) \
               .select("data.*")

cdr_df = cdr_df.withColumn("call_start", to_timestamp(col("call_start"), "yyyy-MM-dd HH:mm:ss")) \
               .withColumn("is_fraud", col("is_fraud").cast("boolean"))

cdr_df = clean_dataframe(cdr_df, "cdr")

ipdr_df = raw_df.filter(col("topic") == "ipdr") \
                .selectExpr("CAST(value AS STRING)") \
                .select(from_json("value", ipdr_schema).alias("data")) \
                .select("data.*")

ipdr_df = ipdr_df.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")) \
                 .withColumn("port", when(col("port").isNull(), 0).otherwise(col("port"))) \
                 .withColumn("duration", when(col("duration").isNull(), 0).otherwise(col("duration"))) \
                 .withColumn("bytes_sent", when(col("bytes_sent").isNull(), 0).otherwise(col("bytes_sent"))) \
                 .withColumn("bytes_received", when(col("bytes_received").isNull(), 0).otherwise(col("bytes_received"))) \
                 .withColumn("vpn_usage", col("vpn_usage").cast("boolean")) \
                 .withColumn("is_fraud", col("is_fraud").cast("boolean"))

ipdr_df = clean_dataframe(ipdr_df, "ipdr")

edr_df = raw_df.filter(col("topic") == "edr") \
               .selectExpr("CAST(value AS STRING)") \
               .select(from_json("value", edr_schema).alias("data")) \
               .select("data.*")

edr_df = edr_df.withColumn("event_time", to_timestamp(col("event_time"), "yyyy-MM-dd HH:mm:ss")) \
               .withColumn("roaming_status", col("roaming_status").cast("boolean")) \
               .withColumn("is_fraud", col("is_fraud").cast("boolean"))

edr_df = clean_dataframe(edr_df, "edr")

def write_to_postgres(df, table_name):
    def batch_func(batch_df, batch_id):
        try:
            clean_batch = batch_df.dropDuplicates()
            if clean_batch.count() > 0:
                clean_batch.write \
                    .option("batchsize", 1000) \
                    .jdbc(url=jdbc_url, table=table_name, mode="append", properties=db_properties)
        except Exception as e:
            print(f"Error in batch {batch_id}: {str(e)}")
            raise

    df.writeStream \
        .foreachBatch(batch_func) \
        .outputMode("append") \
        .start()

write_to_postgres(cdr_df, "cdr")
write_to_postgres(ipdr_df, "ipdr")
write_to_postgres(edr_df, "edr")

spark.streams.awaitAnyTermination()
