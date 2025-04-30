from kafka import KafkaProducer
import pandas as pd
import json
import time
import os
import uuid
from datetime import datetime
from pathlib import Path

script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(script_dir)

# Configure Kafka producer with optimal settings for ML data pipeline
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
    acks='all',
    retries=5,
    retry_backoff_ms=500,
    linger_ms=10,
    batch_size=16384,
    buffer_memory=33554432,
    compression_type='gzip'
)

# CSV file paths
csv_files = {
    'cdr': os.path.join(parent_dir, 'final', 'final_cdr.csv'),
    'edr': os.path.join(parent_dir, 'final', 'final_edr.csv'),
    'ipdr': os.path.join(parent_dir, 'final', 'final_ipdr.csv')
}

# Configuration settings
batch_size = 1000
sleep_between_records = 0.01

# Valid values for standardization (must match consumer expectations)
valid_call_types = ["voice_in", "voice_out", "sms_in", "sms_out", "conference", "video_in", "video_out"]
valid_event_types = ["app_launch", "login", "logout", "payment", "SIM switch", "roaming event"]
valid_protocols = ["TCP", "UDP", "HTTP", "HTTPS", "ICMP"]
valid_network_types = ["4G", "5G", "3G", "WIFI", "LTE"]

def normalize_data(data, topic_name):
    """Normalize data to match consumer expectations"""
    normalized = {}
    
    # Convert all string fields to lowercase for consistency
    for key, value in data.items():
        if isinstance(value, str):
            normalized[key] = value.lower().strip()
        else:
            normalized[key] = value
    
    # Remove source field if present (added by previous version)
    if 'source' in normalized:
        del normalized['source']
    
    # Standardize values based on topic
    if topic_name == 'cdr':
        if 'call_type' in normalized and normalized['call_type'] not in [ct.lower() for ct in valid_call_types]:
            normalized['call_type'] = 'voice_in'
        
        # Ensure boolean fields are proper booleans
        if 'is_fraud' in normalized:
            if isinstance(normalized['is_fraud'], str):
                normalized['is_fraud'] = normalized['is_fraud'].lower() in ['true', 't', '1', 'yes']
    
    elif topic_name == 'ipdr':
        if 'protocol' in normalized and normalized['protocol'] not in [p.lower() for p in valid_protocols]:
            normalized['protocol'] = 'tcp'
            
        # Ensure numeric fields are integers
        for field in ['port', 'duration', 'bytes_sent', 'bytes_received']:
            if field in normalized:
                try:
                    normalized[field] = int(normalized[field])
                except (ValueError, TypeError):
                    normalized[field] = 0
        
        # Ensure boolean fields are proper booleans
        for field in ['vpn_usage', 'is_fraud']:
            if field in normalized:
                if isinstance(normalized[field], str):
                    normalized[field] = normalized[field].lower() in ['true', 't', '1', 'yes']
    
    elif topic_name == 'edr':
        if 'event_type' in normalized and normalized['event_type'] not in [et.lower() for et in valid_event_types]:
            normalized['event_type'] = 'app_launch'
            
        if 'network_type' in normalized and normalized['network_type'] not in [nt.lower() for nt in valid_network_types]:
            normalized['network_type'] = '4g'
        
        # Ensure boolean fields are proper booleans
        for field in ['roaming_status', 'is_fraud']:
            if field in normalized:
                if isinstance(normalized[field], str):
                    normalized[field] = normalized[field].lower() in ['true', 't', '1', 'yes']
    
    return normalized

def stream_csv_to_kafka(csv_file, topic_name, batch_size):
    """Stream data from CSV to Kafka with proper normalization"""
    print(f"Streaming {csv_file} → Kafka topic: {topic_name}")
    
    if not Path(csv_file).exists():
        print(f"File not found: {csv_file}")
        return
    
    record_count = 0
    error_count = 0
    start_time = time.time()
    batch_id = str(uuid.uuid4())
    
    try:
        for chunk in pd.read_csv(csv_file, chunksize=batch_size):
            chunk_time = time.time()
            chunk_records = 0
            
            for _, row in chunk.iterrows():
                data = row.to_dict()
                
                # Normalize data to match consumer expectations
                normalized_data = normalize_data(data, topic_name)
                
                try:
                    future = producer.send(topic_name, value=normalized_data)
                    record_metadata = future.get(timeout=10)
                    record_count += 1
                    chunk_records += 1
                    
                    time.sleep(sleep_between_records)
                    
                except Exception as e:
                    error_count += 1
                    print(f"[✗] Failed to send message: {e}")
            
            # Log progress with metrics
            elapsed = time.time() - chunk_time
            total_elapsed = time.time() - start_time
            records_per_second = chunk_records / elapsed if elapsed > 0 else 0
            
            log_entry = {
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "topic": topic_name,
                "batch_id": batch_id,
                "records_sent": record_count,
                "errors": error_count,
                "batch_size": chunk_records,
                "records_per_second": round(records_per_second, 2),
                "elapsed_seconds": round(total_elapsed, 2)
            }
            
            print(json.dumps(log_entry))
        
        print(f"Completed streaming {topic_name}: Total records sent: {record_count}, Errors: {error_count}")
        
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
                    row_count = sum(1 for _ in f) - 1  # Subtract header
                
                print(f"✓ {topic}: {path} ({file_size:.2f} MB, {row_count:,} records)")
            except Exception as e:
                print(f"✓ {topic}: {path} ({file_size:.2f} MB) - Error counting rows: {str(e)}")
        else:
            print(f"✗ {topic}: {path} - FILE NOT FOUND")
            all_valid = False
    
    return all_valid

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
