from kafka import KafkaProducer
import pandas as pd
import json
import time
from pathlib import Path

# Kafka producer config
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
    acks='all',
    retries=3,
    linger_ms=5,
    compression_type='gzip'
)

# Paths to the generated CSVs
csv_files = {
    'cdr': 'output/cdr/cdr.csv',
    'edr': 'output/edr/edr.csv',
    'ipdr': 'output/ipdr/ipdr.csv'
}

# Streaming config
batch_size = 1000
sleep_between_records = 0.01  # simulate streaming

def stream_csv_to_kafka(csv_file, topic_name, batch_size):
    print(f"Streaming {csv_file} → Kafka topic: {topic_name}")
    if not Path(csv_file).exists():
        print(f"File not found: {csv_file}")
        return
    
    for chunk in pd.read_csv(csv_file, chunksize=batch_size):
        for _, row in chunk.iterrows():
            data = row.to_dict()
            data['source'] = topic_name  # add a source field for metadata
            try:
                future = producer.send(topic_name, value=data)
                record_metadata = future.get(timeout=10)
                print(f"[✓] Sent to {record_metadata.topic} | partition {record_metadata.partition} | offset {record_metadata.offset}")
            except Exception as e:
                print(f"[✗] Failed to send message: {e}")
            time.sleep(sleep_between_records)

try:
    for topic, csv_path in csv_files.items():
        stream_csv_to_kafka(csv_path, topic, batch_size)
    producer.flush()
finally:
    producer.close()
