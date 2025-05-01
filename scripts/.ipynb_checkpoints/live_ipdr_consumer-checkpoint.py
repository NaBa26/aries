from common import *

df = spark.readStream.format("kafka").options(**{**kafka_base_opts, "subscribe": "ipdr"}).load()
parsed = parse_stream(df, ipdr_schema)

def standardize(df):
    df = df.withColumn("timestamp", to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss")) \
           .withColumn("protocol", when(lower("protocol").isin(valid_protocols), lower("protocol")).otherwise("tcp"))
    for field in ["port", "duration", "bytes_sent", "bytes_received"]:
        df = df.withColumn(field, coalesce(col(field), lit(0)).cast("int"))
    for field in ["vpn_usage", "is_fraud"]:
        df = df.withColumn(field, coalesce(col(field), lit(False)))
    return df

cleaned = clean(standardize(parsed), ["user_id", "timestamp", "domain"])
query = write_stream(cleaned, "ipdr")
spark.streams.awaitAnyTermination()
