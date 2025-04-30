from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

import uuid
import json
import time
from datetime import datetime
import os
import traceback


def get_batch_data_path():
    # Get directory where this script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    # Go one level up and into "final"
    return os.path.join(script_dir, "..", "final")

def main():
    batch_source = get_batch_data_path()
    print(f"Using auto-detected batch source: {batch_source}")
    process_batch_data(batch_source)

spark = SparkSession.builder \
    .appName("AriesBatchConsumer") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.2.24") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "10") \
    .config("spark.executor.memory", "2g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

jdbc_url = "jdbc:postgresql://localhost:5432/aries_db"
db_properties = {
    "user": "aries_user",
    "password": "admin",
    "driver": "org.postgresql.Driver",
    "batchsize": "1000",
    "rewriteBatchedStatements": "true"
}

# Constants based on specifications
valid_protocols = ["TCP", "UDP"]
valid_call_types = ["voice_in", "voice_out", "sms_in", "sms_out"]
valid_event_types = ["app_launch", "login", "logout", "payment", "SIM switch", "roaming event", "reboot"]
valid_network_types = ["4G", "5G", "3G", "WIFI", "LTE"]

def log_metrics(batch_id, table, count_before, count_after, processing_time):
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
    batch_uuid = str(uuid.uuid4())
    df = df.withColumn("batch_id", lit(batch_uuid))
    df = df.withColumn("processing_timestamp", current_timestamp())

    # Define key fields per record type
    key_fields_map = {
        "cdr": ["caller_id", "callee_id", "call_start"],
        "ipdr": ["user_id", "timestamp", "domain"],
        "edr": ["event_id", "user_id", "event_time"]
    }
    key_fields = key_fields_map.get(record_type, df.columns[:3])

    # Diagnostics
    print(f"Initial record count: {df.count()}")
    for key in key_fields:
        if key in df.columns:
            null_count = df.filter(col(key).isNull() | (col(key) == "unknown")).count()
            print(f"Null or 'unknown' in {key}: {null_count}")

    # Clean fields by type
    for field in df.schema.fields:
        fname, ftype = field.name, field.dataType
        if isinstance(ftype, StringType):
            default_str = {
                "protocol": "TCP", "call_type": "voice_in", "event_type": "app_launch",
                "network_type": "4G", "os_type": "Android"
            }.get(fname, "unknown")
            df = df.withColumn(fname, when(
                col(fname).isNull() | (trim(col(fname)) == "") |
                (lower(col(fname)).isin("nan", '"nan"')),
                lit(default_str)
            ).otherwise(trim(regexp_replace(lower(col(fname)), r'^"|"$', ''))))
        elif isinstance(ftype, IntegerType):
            df = df.withColumn(fname, when(col(fname).isNull() | isnan(col(fname)), 0).otherwise(col(fname)))
        elif isinstance(ftype, BooleanType):
            df = df.withColumn(fname, when(col(fname).isNull(), False).otherwise(col(fname)))

    # Fix key fields
    for key in key_fields:
        if key in df.columns:
            if "time" in key or "start" in key:
                df = df.withColumn(key, when(col(key).isNull(), current_timestamp()).otherwise(col(key)))
            else:
                df = df.withColumn(key, when(col(key).isNull() | (col(key) == "unknown"),
                                             lit(f"default_{key.replace('_id','')}")).otherwise(col(key)))

    # Record-type-specific logic
    df = apply_record_specific_cleaning(df, record_type)

    df = df.dropDuplicates()
    fraud_count = df.filter(col("is_fraud") == True).count()

    print(f"Final record count: {df.count()}")
    return df


def apply_record_specific_cleaning(df, record_type):
    if record_type == "cdr":
        return clean_cdr(df)
    elif record_type == "ipdr":
        return clean_ipdr(df)
    elif record_type == "edr":
        return clean_edr(df)
    else:
        # Fallback to standardization for unknown types
        return standardize_schema(df, record_type)

def clean_cdr(df):
    valid_fields = [
        "caller_id", "callee_id", "call_start", "call_duration", "call_type", 
        "cell_id", "location", "imei", "is_fraud"
    ]

    # Fill missing columns
    for field in valid_fields:
        if field not in df.columns:
            df = df.withColumn(field, lit(False if field == "is_fraud" else "unknown"))

    # Fix call_duration naming if needed
    if "duration" in df.columns and "call_duration" not in df.columns:
        df = df.withColumnRenamed("duration", "call_duration")
    elif "call_duration" not in df.columns:
        df = df.withColumn("call_duration", lit(0))

    # Validate call_type values
    df = df.withColumn(
        "call_type",
        when(~col("call_type").isin(valid_call_types), "voice_in").otherwise(col("call_type"))
    )

    return standardize_schema(df, "cdr")

def clean_ipdr(df):
    valid_fields = [
        "user_id", "timestamp", "domain", "ip_dst", "port", "protocol", 
        "duration", "bytes_sent", "bytes_received", "vpn_usage", "is_fraud"
    ]

    # Fill missing columns
    for field in valid_fields:
        if field not in df.columns:
            df = df.withColumn(field, lit(False if field == "is_fraud" else "unknown"))

    # Fix bytes_sent and bytes_received consistency
    if "bytes" in df.columns and "bytes_sent" not in df.columns:
        df = df.withColumnRenamed("bytes", "bytes_sent")

    # Ensure vpn_usage is consistent
    if "vpn" in df.columns and "vpn_usage" not in df.columns:
        df = df.withColumnRenamed("vpn", "vpn_usage")

    return standardize_schema(df, "ipdr")

def clean_edr(df):
    valid_fields = [
        "event_id", "user_id", "device_model", "os_type", "roaming_status", 
        "network_type", "event_time", "location", "event_type", "is_fraud"
    ]

    # Fill missing columns
    for field in valid_fields:
        if field not in df.columns:
            df = df.withColumn(field, lit(False if field == "is_fraud" else "unknown"))

    # Ensure event_type is valid
    df = df.withColumn(
        "event_type",
        when(col("event_type").isNull(), "unknown").otherwise(col("event_type"))
    )

    return standardize_schema(df, "edr")

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




def parse_boolean_column(df, col_name):
    return df.withColumn(col_name,
        when(lower(col(col_name)).isin("true", "t", "yes", "y", "1"), True)
        .otherwise(False)
    )

def safe_cast_column(df, col_name, target_type, default_value=None):
    if col_name in df.columns:
        try:
            return df.withColumn(col_name, col(col_name).cast(target_type))
        except:
            if default_value is not None:
                return df.withColumn(col_name, 
                    when(col(col_name).cast(target_type).isNull(), default_value)
                    .otherwise(col(col_name).cast(target_type))
                )
    return df

def load_and_prepare_data(file_path, alt_path, columns, timestamp_cols, bool_cols, int_cols, tag):
    try:
        try:
            df = spark.read.csv(file_path, header=True, inferSchema=True)
        except:
            df = spark.read.csv(alt_path if alt_path else file_path, header=True, inferSchema=True)
    except Exception as e:
        print(f"Error loading {tag} data: {e}")
        traceback.print_exc()
        try:
            df = spark.read.csv(file_path)
        except:
            df = spark.read.csv(alt_path if alt_path else file_path)
        for i, name in enumerate(columns):
            if i < len(df.columns):
                df = df.withColumnRenamed(df.columns[i], name)

    print(f"{tag.upper()} data loaded: {df.count()} records")
    df.printSchema()

    for col_name in timestamp_cols:
        if col_name in df.columns:
            try:
                df = df.withColumn(col_name, to_timestamp(col(col_name), "yyyy-MM-dd HH:mm:ss"))
            except:
                df = df.withColumn(col_name, to_timestamp(col(col_name)))

    for col_name in bool_cols:
        df = parse_boolean_column(df, col_name)

    for col_name in int_cols:
        df = safe_cast_column(df, col_name, "integer", default_value=0)

    df = clean_dataframe(df, tag)

    return df

def process_batch_data(batch_source):
    print(f"Starting batch processing from {batch_source}")
    batch_uuid = str(uuid.uuid4())
    start_time_total = time.time()

    try:
        datasets = [
            {
                "name": "cdr",
                "file": f"{batch_source}/final_cdr.csv",
                "alt": None,
                "columns": ["caller_id", "callee_id", "call_start", "call_duration", "call_type", "cell_id", "location", "imei", "is_fraud"],
                "timestamps": ["call_start"],
                "booleans": ["is_fraud"],
                "integers": ["call_duration"],
                "table": "cdr"
            },
            {
                "name": "ipdr",
                "file": f"{batch_source}/final_ipdr.csv",
                "alt": None,
                "columns": ["user_id", "timestamp", "domain", "ip_dst", "port", "protocol", "duration", "bytes_sent", "bytes_received", "vpn_usage", "is_fraud"],
                "timestamps": ["timestamp"],
                "booleans": ["vpn_usage", "is_fraud"],
                "integers": ["port", "duration", "bytes_sent", "bytes_received"],
                "table": "ipdr"
            },
            {
                "name": "edr",
                "file": f"{batch_source}/final_edr.csv",
                "alt": f"{batch_source}/finaledr.csv",
                "columns": ["event_id", "user_id", "device_model", "os_type", "roaming_status", "network_type", "event_time", "location", "event_type", "is_fraud"],
                "timestamps": ["event_time"],
                "booleans": ["roaming_status", "is_fraud"],
                "integers": [],
                "table": "edr"
            }
        ]

        results = {}
        for data in datasets:
            print(f"Processing {data['name'].upper()} data...")
            start = time.time()
            df = load_and_prepare_data(
                file_path=data["file"],
                alt_path=data["alt"],
                columns=data["columns"],
                timestamp_cols=data["timestamps"],
                bool_cols=data["booleans"],
                int_cols=data["integers"],
                tag=data["name"]
            )
            count_after = df.count()
            log_metrics(batch_uuid, data["name"], count_after, count_after, time.time() - start)

            print(f"Writing {data['name'].upper()} data to database...")
            df.write.jdbc(url=jdbc_url, table=data["table"], mode="append", properties=db_properties)
            results[data["name"]] = df



        print(f"Batch processing completed in {time.time() - start_time_total:.2f} seconds")

    except Exception as e:
        print(f"Error processing batch data: {str(e)}")
        traceback.print_exc()
        raise e

if __name__ == "__main__":
    main()