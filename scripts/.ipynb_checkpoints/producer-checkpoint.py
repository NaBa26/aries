from kafka import KafkaProducer
import pandas as pd
import json
import time
import os
from pathlib import Path

script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(script_dir)

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
    acks='all',
    retries=3,
    linger_ms=5,
    compression_type='gzip'
)

csv_files = {
    'cdr': os.path.join(parent_dir, 'final', 'final_cdr.csv'),
    'edr': os.path.join(parent_dir, 'final', 'final_edr.csv'),
    'ipdr': os.path.join(parent_dir, 'final', 'final_ipdr.csv')
}

batch_size = 1000
sleep_between_records = 0.01 

def stream_csv_to_kafka(csv_file, topic_name, batch_size):
    print(f"Streaming {csv_file} → Kafka topic: {topic_name}")
    
    if not Path(csv_file).exists():
        print(f"File not found: {csv_file}")
        return
    
    record_count = 0
    try:
        for chunk in pd.read_csv(csv_file, chunksize=batch_size):
            for _, row in chunk.iterrows():
                data = row.to_dict()
                data['source'] = topic_name 
                
                try:
                    future = producer.send(topic_name, value=data)
                    record_metadata = future.get(timeout=10)
                    record_count += 1

                    if record_count % 1000 == 0:
                        print(f"[✓] {topic_name}: Sent {record_count} records | Latest: partition {record_metadata.partition} | offset {record_metadata.offset}")
                        
                    time.sleep(sleep_between_records)
                    
                except Exception as e:
                    print(f"[✗] Failed to send message: {e}")
                    
        print(f"Completed streaming {topic_name}: Total records sent: {record_count}")
        
    except Exception as e:
        print(f"Error processing {csv_file}: {str(e)}")

def validate_paths():
    """Validate that all CSV files exist and print their stats"""
    all_valid = True
    
    print("\nValidating CSV files:")
    for topic, path in csv_files.items():
        if Path(path).exists():
            file_size = Path(path).stat().st_size / (1024 * 1024)
            try:
                with open(path, 'r') as f:
                    row_count = sum(1 for _ in f) - 1 
                
                print(f"✓ {topic}: {path} ({file_size:.2f} MB, {row_count:,} records)")
            except Exception as e:
                print(f"✓ {topic}: {path} ({file_size:.2f} MB) - Error counting rows: {str(e)}")
        else:
            print(f"✗ {topic}: {path} - FILE NOT FOUND")
            all_valid = False
    
    return all_valid


def generate_cdr():
    return {
        "caller_id": str(fake.random_number(digits=13)),
        "callee_id": str(fake.random_number(digits=13)),
        "call_start": fake.date_time_this_year().strftime("%Y-%m-%d %H:%M:%S"),
        "call_duration": random.randint(1, 3600),
        "call_type": random.choice(["voice_in", "voice_out", "sms_in", "sms_out", "conference", "video_in", "video_out"]),
        "cell_id": f"CELL_{random.randint(100, 999)}",
        "location": f"{fake.latitude():.4f},{fake.longitude():.4f}",
        "imei": f"IMEI_{random.randint(100000, 999999)}",
        "is_fraud": random.choice([True, False])
    }

while True:
    cdr_record = generate_cdr()
    producer.send('cdr', cdr_record)
    print("Sent CDR:", cdr_record)
    time.sleep(1)

if __name__ == "__main__":
    print("Telecom Data Kafka Producer")
    print("===========================")
    print(f"Script location: {script_dir}")
    print(f"Looking for files in: {parent_dir}/final/")

    if validate_paths():
        try:
            for topic, csv_path in csv_files.items():
                stream_csv_to_kafka(csv_path, topic, batch_size)

            print("Flushing remaining messages...")
            producer.flush()
            print("All messages sent successfully")
            
        except KeyboardInterrupt:
            print("\nProcess interrupted by user. Flushing messages and shutting down...")
        
        finally:
            producer.close()
            print("Kafka producer closed")
    else:
        print("\nError: Some CSV files were not found. Please check the paths and try again.")