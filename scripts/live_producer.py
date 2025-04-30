from kafka import KafkaProducer
from faker import Faker
import random
import json
import time
import logging

fake = Faker()

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks='all',
    retries=3,
    linger_ms=10,
    compression_type='gzip'
)

def generate_live_cdr():
    return {
        "caller_id": str(fake.random_number(digits=13)),
        "callee_id": str(fake.random_number(digits=13)),
        "call_start": fake.date_time_this_year().strftime("%Y-%m-%d %H:%M:%S"),
        "call_duration": random.randint(1, 3600),
        "call_type": random.choice([
            "voice_in", "voice_out", "sms_in", "sms_out",
            "conference", "video_in", "video_out"
        ]),
        "cell_id": f"CELL_{random.randint(100, 999)}",
        "location": f"{fake.latitude():.4f},{fake.longitude():.4f}",
        "imei": f"IMEI_{random.randint(100000000000000, 999999999999999)}",
        "is_fraud": random.choice([True, False])
    }

def main():
    print("Starting live CDR producer...")
    record_count = 0

    try:
        while True:
            record = generate_live_cdr()
            future = producer.send("cdr", value=record)
            metadata = future.get(timeout=10)

            record_count += 1
            print(f"[✓] Sent {record_count}: partition {metadata.partition} | offset {metadata.offset} | {record['call_type']}")

            time.sleep(1)  # Adjust frequency as needed

    except KeyboardInterrupt:
        print("\nInterrupted by user. Closing producer...")
    finally:
        producer.flush()
        producer.close()
        print("Kafka producer closed.")

if __name__ == "__main__":
    main()
