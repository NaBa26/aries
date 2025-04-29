from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, unix_timestamp, split

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

cdr_schema = "caller_id STRING, callee_id STRING, call_start STRING, call_duration INT, call_type STRING, " \
             "cell_id STRING, location STRING, imei STRING, is_fraud BOOLEAN"
ipdr_schema = "user_id STRING, timestamp STRING, domain STRING, ip_dst STRING, port INT, protocol STRING, " \
              "duration INT, bytes_sent INT, bytes_received INT, vpn_usage BOOLEAN, is_fraud BOOLEAN"
edr_schema = "event_id STRING, user_id STRING, device_model STRING, os_type STRING, roaming_status BOOLEAN, " \
             "network_type STRING, event_time STRING, location STRING, event_type STRING, is_fraud BOOLEAN"

# Process CDR
cdr_df = raw_df.filter(col("topic") == "cdr").selectExpr("CAST(value AS STRING)").select(from_json("value", cdr_schema).alias("data")).select("data.*")
cdr_df = cdr_df.withColumn("call_start", unix_timestamp("call_start", "yyyy-MM-dd HH:mm:ss").cast("timestamp")) \
               .withColumn("latitude", split(col("location"), ",").getItem(0).cast("double")) \
               .withColumn("longitude", split(col("location"), ",").getItem(1).cast("double")) \
               .withColumn("is_fraud", col("is_fraud").cast("boolean"))

# Process IPDR
ipdr_df = raw_df.filter(col("topic") == "ipdr").selectExpr("CAST(value AS STRING)").select(from_json("value", ipdr_schema).alias("data")).select("data.*")
ipdr_df = ipdr_df.withColumn("timestamp", unix_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss").cast("timestamp")) \
                 .withColumn("is_fraud", col("is_fraud").cast("boolean"))

# Process EDR
edr_df = raw_df.filter(col("topic") == "edr").selectExpr("CAST(value AS STRING)").select(from_json("value", edr_schema).alias("data")).select("data.*")
edr_df = edr_df.withColumn("event_time", unix_timestamp("event_time", "yyyy-MM-dd HH:mm:ss").cast("timestamp")) \
               .withColumn("latitude", split(col("location"), ",").getItem(0).cast("double")) \
               .withColumn("longitude", split(col("location"), ",").getItem(1).cast("double")) \
               .withColumn("is_fraud", col("is_fraud").cast("boolean"))

def write_to_postgres(df, table_name):
    df.writeStream \
        .foreachBatch(lambda batch_df, batch_id: batch_df.write \
                      .option("batchsize", 1000)  # Adding a batch size
                      .jdbc(url=jdbc_url, table=table_name, mode="append", properties=db_properties)) \
        .outputMode("append") \
        .start()

write_to_postgres(cdr_df, "cdr")
write_to_postgres(ipdr_df, "ipdr")
write_to_postgres(edr_df, "edr")

spark.streams.awaitAnyTermination()
