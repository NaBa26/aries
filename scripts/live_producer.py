from kafka import KafkaProducer
from faker import Faker
import random
import json
import time
import uuid
from datetime import datetime, timedelta

fake = Faker()

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks='all',
    retries=5,
    retry_backoff_ms=500,
    linger_ms=10,
    batch_size=16384,
    buffer_memory=33554432,
    compression_type='gzip'
)

valid_call_types = ["voice_in", "voice_out", "sms_in", "sms_out", "conference", "video_in", "video_out"]
valid_event_types = ["app_launch", "login", "logout", "payment", "SIM switch", "roaming event"]
valid_protocols = ["TCP", "UDP", "HTTP", "HTTPS", "ICMP"]
valid_network_types = ["4G", "5G", "3G", "WIFI", "LTE"]
valid_os_types = ["ios", "android", "windows", "harmonyos"]
valid_device_models = [
    "iphone 14", "iphone 15", "galaxy s22", "galaxy s23", 
    "pixel 7", "pixel 8", "oneplus 11", "xiaomi 13"
]

domain_categories = {
    "social": ["facebook.com", "instagram.com", "twitter.com", "linkedin.com", "tiktok.com"],
    "video": ["youtube.com", "netflix.com", "hulu.com", "disney.com", "vimeo.com"],
    "gaming": ["steam.com", "epicgames.com", "roblox.com", "minecraft.net", "ea.com"],
    "financial": ["paypal.com", "chase.com", "bankofamerica.com", "wellsfargo.com", "coinbase.com"],
    "other": ["amazon.com", "google.com", "microsoft.com", "apple.com", "wikipedia.org"]
}

def get_random_domain_by_category():
    """Get random domain based on category distributions for more realistic data"""
    category = random.choices(
        list(domain_categories.keys()), 
        weights=[30, 25, 15, 10, 20],
        k=1
    )[0]
    return random.choice(domain_categories[category])

def generate_cdr():
    """Generate Call Detail Record with proper field formats"""
    call_start = fake.date_time_this_year()
    call_duration = random.randint(1, 3600)
    
    imei = f"{random.randint(100000000000000, 999999999999999)}"
    if random.random() < 0.05:
        imei = random.choice(["", "NaN", None])
    
    caller_id = f"+{random.randint(1, 9)}{random.randint(100000000, 999999999)}"
    callee_id = f"+{random.randint(1, 9)}{random.randint(100000000, 999999999)}"
    
    lat = fake.latitude()
    lng = fake.longitude()
    location = f"{lat:.6f},{lng:.6f}"
    
    is_fraud = random.random() < 0.08
    
    return {
        "caller_id": caller_id,
        "callee_id": callee_id,
        "call_start": call_start.strftime("%Y-%m-%d %H:%M:%S"),
        "call_duration": call_duration,
        "call_type": random.choice(valid_call_types).lower(),
        "cell_id": f"cell_{random.randint(100, 999)}",
        "location": location,
        "imei": imei,
        "is_fraud": is_fraud,
    }

def generate_ipdr():
    """Generate Internet Protocol Detail Record with proper field formats"""
    event_time = fake.date_time_this_year()
    
    duration = random.randint(1, 300)
    bytes_sent = random.randint(100, 100000)
    is_streaming = random.random() < 0.3
    bytes_received = random.randint(100000, 1000000) if is_streaming else random.randint(100, 200000)
    
    domain = get_random_domain_by_category()
    
    vpn_usage = random.random() < 0.2 
    is_fraud = random.random() < 0.3 if vpn_usage else random.random() < 0.05
    
    return {
        "user_id": f"{random.randint(100000000000, 999999999999)}",
        "timestamp": event_time.strftime("%Y-%m-%d %H:%M:%S"),
        "domain": domain,
        "ip_dst": fake.ipv4_public(),
        "port": random.randint(1, 65535),
        "protocol": random.choice(valid_protocols).lower(),
        "duration": duration,
        "bytes_sent": bytes_sent,
        "bytes_received": bytes_received,
        "vpn_usage": vpn_usage,
        "is_fraud": is_fraud,
    }

def generate_edr():
    """Generate Event Detail Record with proper field formats"""
    event_time = fake.date_time_this_year()
    
    device_model = random.choice(valid_device_models)
    os_type = random.choice(valid_os_types)
    
    event_type = random.choice(valid_event_types).lower()
    
    is_high_risk = event_type in ["payment", "sim switch"]
    is_fraud = random.random() < 0.25 if is_high_risk else random.random() < 0.05
    
    lat = fake.latitude()
    lng = fake.longitude()
    location = f"{lat:.6f},{lng:.6f}"
    
    roaming_status = random.random() < 0.15
    network_type = random.choice(valid_network_types).lower()
    
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": f"{random.randint(100000000000, 999999999999)}",
        "device_model": device_model,
        "os_type": os_type,
        "roaming_status": roaming_status,
        "network_type": network_type,
        "event_time": event_time.strftime("%Y-%m-%d %H:%M:%S"),
        "location": location,
        "event_type": event_type,
        "is_fraud": is_fraud,
    }

def generate_correlated_events(base_count=10):
    """Generate correlated events across record types for the same user"""
    user_id = f"{random.randint(100000000000, 999999999999)}"
    base_time = fake.date_time_this_month()
    is_fraudulent_user = random.random() < 0.1 
    
    events = []
    
    for _ in range(random.randint(1, base_count)):
        time_offset = timedelta(minutes=random.randint(0, 60*24))
        cdr = generate_cdr()
        cdr['caller_id'] = user_id
        cdr['call_start'] = (base_time + time_offset).strftime("%Y-%m-%d %H:%M:%S")
        cdr['is_fraud'] = is_fraudulent_user and random.random() < 0.7
        events.append(('cdr', cdr))
    
    for _ in range(random.randint(1, base_count * 2)):
        time_offset = timedelta(minutes=random.randint(0, 60*24))
        ipdr = generate_ipdr()
        ipdr['user_id'] = user_id
        ipdr['timestamp'] = (base_time + time_offset).strftime("%Y-%m-%d %H:%M:%S")
        ipdr['is_fraud'] = is_fraudulent_user and random.random() < 0.7
        events.append(('ipdr', ipdr))
    
    for _ in range(random.randint(1, base_count)):
        time_offset = timedelta(minutes=random.randint(0, 60*24))
        edr = generate_edr()
        edr['user_id'] = user_id
        edr['event_time'] = (base_time + time_offset).strftime("%Y-%m-%d %H:%M:%S")
        edr['is_fraud'] = is_fraudulent_user and random.random() < 0.7
        events.append(('edr', edr))
    
    return sorted(events, key=lambda x: x[1].get('call_start', x[1].get('timestamp', x[1].get('event_time'))))

def print_stats(batch_id, record_count, start_time):
    """Log statistics every 100 records"""
    elapsed = time.time() - start_time
    records_per_second = record_count / elapsed if elapsed > 0 else 0
    log_entry = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "batch_id": batch_id,
        "records_sent": record_count,
        "records_per_second": round(records_per_second, 2),
        "elapsed_seconds": round(elapsed, 2)
    }
    print(json.dumps(log_entry))

def main():
    print("Live Kafka Producer for CDR, IPDR, and EDR")
    print("==========================================")
    print("Generating realistic telecom data for ML pipeline")
    
    record_count = 0
    start_time = time.time()
    batch_id = str(uuid.uuid4())
    
    try:
        while True:
            if random.random() < 0.2:
                correlated_events = generate_correlated_events()
                
                for topic, record in correlated_events:
                    try:
                        key = str(record.get('user_id', record.get('caller_id', ''))).encode('utf-8')
                        future = producer.send(topic, key=key, value=record)
                        future.get(timeout=10)
                        record_count += 1
                        
                        if record_count % 100 == 0:
                            print_stats(batch_id, record_count, start_time)
                        
                        time.sleep(0.01)
                    except Exception as e:
                        print(f"[✗] Failed to send correlated event: {e}")
            
            else:
                topic = random.choice(["cdr", "ipdr", "edr"])
                
                if topic == "cdr":
                    record = generate_cdr()
                elif topic == "ipdr":
                    record = generate_ipdr()
                else:
                    record = generate_edr()
                
                try:
                    key = str(record.get('user_id', record.get('caller_id', ''))).encode('utf-8')
                    future = producer.send(topic, key=key, value=record)
                    future.get(timeout=10)
                    record_count += 1
                    
                    if record_count % 100 == 0:
                        print_stats(batch_id, record_count, start_time)
                except Exception as e:
                    print(f"[✗] Failed to send message: {e}")
            
            time.sleep(0.1)
            
    except KeyboardInterrupt:
        print("\nStopped by user.")
    finally:
        producer.flush()
        producer.close()
        
        total_time = time.time() - start_time
        avg_rate = record_count / total_time if total_time > 0 else 0
        
        print("\nFinal Statistics:")
        print(f"Total Records: {record_count}")
        print(f"Total Time: {total_time:.2f} seconds")
        print(f"Average Rate: {avg_rate:.2f} records/second")
        print("Kafka producer closed.")

if __name__ == "__main__":
    main()
