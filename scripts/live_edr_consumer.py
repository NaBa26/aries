from common import *

df = spark.readStream.format("kafka").options(**{**kafka_base_opts, "subscribe": "edr"}).load()
parsed = parse_stream(df, edr_schema)

def standardize(df):
    df = df.withColumn("event_time", to_timestamp("event_time", "yyyy-MM-dd HH:mm:ss")) \
           .withColumn("event_type", when(lower("event_type").isin(valid_event_types), lower("event_type")).otherwise("app_launch")) \
           .withColumn("network_type", when(lower("network_type").isin(valid_network_types), lower("network_type")).otherwise("4g"))
    for field in ["roaming_status", "is_fraud"]:
        df = df.withColumn(field, coalesce(col(field), lit(False)))
    return df

cleaned = clean(standardize(parsed), ["event_id", "user_id", "event_time"])
query = write_stream(cleaned, "edr")
spark.streams.awaitAnyTermination()
