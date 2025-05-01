from common import *

df = spark.readStream.format("kafka").options(**{**kafka_base_opts, "subscribe": "cdr"}).load()
parsed = parse_stream(df, cdr_schema)

def standardize(df):
    return df.withColumn("call_start", to_timestamp("call_start", "yyyy-MM-dd HH:mm:ss")) \
             .withColumn("call_type", when(lower("call_type").isin(valid_call_types), lower("call_type")).otherwise("voice_in")) \
             .withColumn("is_fraud", coalesce("is_fraud", lit(False)))

cleaned = clean(standardize(parsed), ["caller_id", "callee_id", "call_start"])
query = write_stream(cleaned, "cdr")
spark.streams.awaitAnyTermination()
