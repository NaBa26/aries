import json
import csv
import random
import datetime
import uuid
import xml.dom.minidom
import xml.etree.ElementTree as ET
import os
import gzip
import zipfile
import time
from typing import List, Dict, Any, Tuple

IPDR_COUNT = 4000
EDR_COUNT = 5000
CDR_COUNT = 2500

os.makedirs("generated_data", exist_ok=True)

random.seed(42)

DOMAINS = [
    "fileshare.cc", "youtube.com", "netflix.com", "zoom.us", "facebook.com", 
    "instagram.com", "amazon.in", "flipkart.com", "twitter.com", "snapchat.com"
]

DEVICE_MODELS = [
    "Pixel 6", "Pixel 7", "iPhone 13", "iPhone 14", "Samsung Galaxy S22", 
    "OnePlus 10", "Xiaomi Mi 11", "Realme GT", "Oppo F21", "Vivo V25"
]

OS_TYPES = [
    "Android 12", "Android 13", "iOS 15", "iOS 16", "Android 11"
]

NETWORK_TYPES = ["WiFi", "4G", "5G", "3G", "2G"]
CALL_TYPES = ["voice_in", "voice_out", "video_in", "video_out", "conference"]
PROTOCOLS = ["TCP", "UDP", "HTTP", "HTTPS", "DNS"]

TIMESTAMP_FORMATS = [
    lambda dt: dt.strftime("%Y-%m-%d %H:%M:%S"),              # Standard
    lambda dt: dt.strftime("%d/%m/%Y %H:%M:%S"),              # DD/MM/YYYY
    lambda dt: dt.strftime("%m/%d/%Y %H:%M:%S"),              # MM/DD/YYYY
    lambda dt: str(int(dt.timestamp())),                      # Unix timestamp
    lambda dt: dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z" # ISO format
]

# Field naming variations
IPDR_FIELD_VARIATIONS = [
    {
        "user_id": ["user_id", "subscriber_id", "customer_id", "msisdn"],
        "timestamp": ["timestamp", "access_time", "connection_time", "event_timestamp"],
        "domain": ["domain", "destination_domain", "host", "url"],
        "ip_dst": ["ip_dst", "destination_ip", "remote_ip", "server_ip"],
        "port": ["port", "dest_port", "server_port", "remote_port"],
        "protocol": ["protocol", "service_protocol", "conn_protocol", "transport_protocol"],
        "duration": ["duration", "session_duration", "connection_time", "time_spent"],
        "bytes_sent": ["bytes_sent", "upload_bytes", "upstream_data", "tx_bytes"],
        "bytes_received": ["bytes_received", "download_bytes", "downstream_data", "rx_bytes"],
        "vpn_usage": ["vpn_usage", "vpn_detected", "is_vpn", "encrypted_tunnel"]
    }
]

EDR_FIELD_VARIATIONS = [
    {
        "event_id": ["event_id", "session_id", "transaction_id", "tracking_id"],
        "user_id": ["user_id", "subscriber_id", "msisdn", "phone_number"],
        "device_model": ["device_model", "device_type", "handset", "terminal_model"],
        "os_type": ["os_type", "os_version", "platform", "device_os"],
        "roaming_status": ["roaming_status", "is_roaming", "roaming_active", "home_network"],
        "network_type": ["network_type", "connection_type", "access_technology", "bearer_type"],
        "event_time": ["event_time", "timestamp", "log_time", "recorded_at"],
        "location": ["location", "geo_coordinates", "position", "cell_location"],
        "event_type": ["event_type", "action", "event_name", "service_action"]
    }
]

CDR_FIELD_VARIATIONS = [
    {
        "caller_id": ["caller_id", "calling_party", "a_number", "originating_number"],
        "callee_id": ["callee_id", "called_party", "b_number", "terminating_number"],
        "call_start": ["call_start", "start_time", "setup_time", "connection_time"],
        "call_duration": ["call_duration", "duration", "call_length", "billable_seconds"],
        "call_type": ["call_type", "service_type", "call_category", "communication_type"],
        "cell_id": ["cell_id", "cell_identity", "tower_id", "bts_id"],
        "location": ["location", "geo_coordinates", "tower_location", "subscriber_location"],
        "imei": ["imei", "device_id", "terminal_id", "equipment_id"]
    }
]

# Helper functions
def generate_phone_number() -> str:
    """Generate a random 13-digit phone number."""
    return str(random.randint(1000000000000, 9999999999999))

def generate_ipv4() -> str:
    """Generate a random IPv4 address."""
    return f"{random.randint(1, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}"

def generate_location() -> str:
    """Generate random geo coordinates."""
    lat = random.uniform(-90, 90)
    lon = random.uniform(-180, 180)
    return f"{lat:.4f},{lon:.4f}"

def generate_imei() -> str:
    """Generate a fake IMEI identifier."""
    return f"IMEI_{random.randint(100000, 999999)}"

def generate_cell_id() -> str:
    """Generate a cell tower identifier."""
    return f"CELL_{random.randint(100, 999)}"

def maybe_null(value, probability=0.05):
    """Return None with a small probability, otherwise return the value."""
    return None if random.random() < probability else value

def maybe_duplicate(records: List[Dict], probability=0.02) -> List[Dict]:
    """Duplicate some records with a given probability."""
    duplicated_records = records.copy()
    for record in records:
        if random.random() < probability:
            duplicated_records.append(record.copy())
    return duplicated_records

def maybe_corrupt(value, probability=0.01):
    """Corrupt a value with a small probability."""
    if random.random() < probability:
        if isinstance(value, str):
            # Corrupt string by dropping a character
            if len(value) > 1:
                pos = random.randint(0, len(value) - 1)
                return value[:pos] + value[pos+1:]
            return value
        elif isinstance(value, int):
            # Corrupt number by adding noise
            return value + random.randint(-10, 10)
        elif isinstance(value, float):
            # Corrupt float by adding noise
            return value + random.uniform(-1, 1)
        elif isinstance(value, bool):
            # Flip boolean
            return not value
    return value

def random_date(start_date, end_date):
    """Generate a random date between start_date and end_date."""
    delta = end_date - start_date
    random_days = random.randrange(delta.days)
    random_seconds = random.randrange(86400)  # seconds in a day
    return start_date + datetime.timedelta(days=random_days, seconds=random_seconds)


def generate_ipdr_records(count: int) -> List[Dict]:
    """Generate IPDR (Internet Protocol Detail Record) data."""
    records = []
    
    for _ in range(count):
        field_variation = random.choice(IPDR_FIELD_VARIATIONS)

        user_id_field = random.choice(field_variation["user_id"])
        timestamp_field = random.choice(field_variation["timestamp"])
        domain_field = random.choice(field_variation["domain"])
        ip_dst_field = random.choice(field_variation["ip_dst"])
        port_field = random.choice(field_variation["port"])
        protocol_field = random.choice(field_variation["protocol"])
        duration_field = random.choice(field_variation["duration"])
        bytes_sent_field = random.choice(field_variation["bytes_sent"])
        bytes_received_field = random.choice(field_variation["bytes_received"])
        vpn_usage_field = random.choice(field_variation["vpn_usage"])
        
        # Generate base data
        user_id = generate_phone_number()
        timestamp = random_date(
            datetime.datetime(2023, 1, 1), 
            datetime.datetime(2023, 12, 31)
        )
        
        # Format timestamp in a random format
        timestamp_formatter = random.choice(TIMESTAMP_FORMATS)
        formatted_timestamp = timestamp_formatter(timestamp)
        
        # Generate record with the chosen field names
        record = {
            user_id_field: user_id,
            timestamp_field: formatted_timestamp,
            domain_field: maybe_null(random.choice(DOMAINS)),
            ip_dst_field: maybe_null(generate_ipv4()),
            port_field: maybe_null(random.randint(1, 65535)),
            protocol_field: maybe_null(random.choice(PROTOCOLS)),
            duration_field: maybe_corrupt(random.randint(1, 3600)),
            bytes_sent_field: maybe_corrupt(random.randint(10, 10000)),
            bytes_received_field: maybe_corrupt(random.randint(10, 100000)),
            vpn_usage_field: maybe_corrupt(random.choice([True, False])),
            "is_fraud": random.random() < 0.05,  # 5% chance of fraud
        }
        
        records.append(record)
    
    # Add some duplicates
    records = maybe_duplicate(records)
    
    return records

def generate_edr_records(count: int) -> List[Dict]:
    """Generate EDR (Event Detail Record) data."""
    records = []
    
    for _ in range(count):
        # For each record, pick one field name variation consistently
        field_variation = random.choice(EDR_FIELD_VARIATIONS)
        
        # Choose one consistent field name for this record
        event_id_field = random.choice(field_variation["event_id"])
        user_id_field = random.choice(field_variation["user_id"])
        device_model_field = random.choice(field_variation["device_model"])
        os_type_field = random.choice(field_variation["os_type"])
        roaming_status_field = random.choice(field_variation["roaming_status"])
        network_type_field = random.choice(field_variation["network_type"])
        event_time_field = random.choice(field_variation["event_time"])
        location_field = random.choice(field_variation["location"])
        event_type_field = random.choice(field_variation["event_type"])
        
        # Generate base data
        user_id = generate_phone_number()
        event_time = random_date(
            datetime.datetime(2023, 1, 1), 
            datetime.datetime(2023, 12, 31)
        )
        
        # Format timestamp in a random format
        timestamp_formatter = random.choice(TIMESTAMP_FORMATS)
        formatted_timestamp = timestamp_formatter(event_time)
        
        # Generate record with the chosen field names
        record = {
            event_id_field: maybe_null(str(uuid.uuid4())),
            user_id_field: user_id,
            device_model_field: maybe_null(random.choice(DEVICE_MODELS)),
            os_type_field: maybe_null(random.choice(OS_TYPES)),
            roaming_status_field: maybe_corrupt(random.choice([True, False])),
            network_type_field: maybe_null(random.choice(NETWORK_TYPES)),
            event_time_field: formatted_timestamp,
            location_field: maybe_null(generate_location()),
            event_type_field: maybe_null(random.choice(["SIM switch", "power_on", "app_launch", "location_change"])),
            "is_fraud": random.random() < 0.05,  # 5% chance of fraud
        }
        
        records.append(record)
    
    # Add some duplicates
    records = maybe_duplicate(records)
    
    return records

def generate_cdr_records(count: int) -> List[Dict]:
    """Generate CDR (Call Detail Record) data."""
    records = []
    
    for _ in range(count):
        # For each record, pick one field name variation consistently
        field_variation = random.choice(CDR_FIELD_VARIATIONS)
        
        # Choose one consistent field name for this record
        caller_id_field = random.choice(field_variation["caller_id"])
        callee_id_field = random.choice(field_variation["callee_id"])
        call_start_field = random.choice(field_variation["call_start"])
        call_duration_field = random.choice(field_variation["call_duration"])
        call_type_field = random.choice(field_variation["call_type"])
        cell_id_field = random.choice(field_variation["cell_id"])
        location_field = random.choice(field_variation["location"])
        imei_field = random.choice(field_variation["imei"])
        
        # Generate base data
        caller_id = generate_phone_number()
        callee_id = generate_phone_number()
        call_start = random_date(
            datetime.datetime(2023, 1, 1), 
            datetime.datetime(2023, 12, 31)
        )
        
        # Format timestamp in a random format
        timestamp_formatter = random.choice(TIMESTAMP_FORMATS)
        formatted_timestamp = timestamp_formatter(call_start)
        
        # Generate record with the chosen field names
        record = {
            caller_id_field: caller_id,
            callee_id_field: callee_id,
            call_start_field: formatted_timestamp,
            call_duration_field: maybe_corrupt(random.randint(1, 3600)),
            call_type_field: maybe_null(random.choice(CALL_TYPES)),
            cell_id_field: maybe_null(generate_cell_id()),
            location_field: maybe_null(generate_location()),
            imei_field: maybe_null(generate_imei()),
            "is_fraud": random.random() < 0.05,  # 5% chance of fraud
        }
        
        records.append(record)
    
    # Add some duplicates
    records = maybe_duplicate(records)
    
    return records

# Export functions for various formats
def export_to_json(records: List[Dict], filename: str):
    """Export records to a JSON file."""
    with open(filename, 'w') as f:
        json.dump(records, f, indent=2)
    print(f"Exported {len(records)} records to {filename}")

def export_to_csv(records: List[Dict], filename: str):
    """Export records to a CSV file."""
    if not records:
        return
    
    # Collect all possible field names across all records
    all_fields = set()
    for record in records:
        all_fields.update(record.keys())
    
    with open(filename, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=list(all_fields))
        writer.writeheader()
        
        # For each record, ensure all fields are present (with None for missing fields)
        for record in records:
            row = {field: record.get(field, None) for field in all_fields}
            writer.writerow(row)
            
    print(f"Exported {len(records)} records to {filename}")

def export_to_xml(records: List[Dict], filename: str, root_tag: str, item_tag: str):
    """Export records to an XML file."""
    root = ET.Element(root_tag)
    
    for record in records:
        item = ET.SubElement(root, item_tag)
        for key, value in record.items():
            # Handle None values
            if value is None:
                continue
                
            # Format boolean values as lowercase strings
            if isinstance(value, bool):
                value = str(value).lower()
                
            # Create element with text content
            field = ET.SubElement(item, key)
            field.text = str(value)
    
    # Pretty print the XML
    xml_str = ET.tostring(root, encoding='utf-8')
    dom = xml.dom.minidom.parseString(xml_str)
    pretty_xml = dom.toprettyxml(indent="  ")
    
    with open(filename, 'w') as f:
        f.write(pretty_xml)
    print(f"Exported {len(records)} records to {filename}")

def export_to_gzip(records: List[Dict], filename: str, format_func, *args):
    """Export records to a gzipped file."""
    # Create a temporary file
    temp_filename = filename.replace('.gz', '')
    format_func(records, temp_filename, *args)
    
    # Compress the file
    with open(temp_filename, 'rb') as f_in:
        with gzip.open(filename, 'wb') as f_out:
            f_out.write(f_in.read())
    
    # Remove the temporary file
    os.remove(temp_filename)
    print(f"Compressed {len(records)} records to {filename}")

def export_to_zip(records: List[Dict], filename: str, format_func, *args):
    """Export records to a zipped file."""
    # Create a temporary file
    temp_filename = os.path.basename(filename).replace('.zip', '')
    format_func(records, temp_filename, *args)
    
    # Compress the file
    with zipfile.ZipFile(filename, 'w') as zipf:
        zipf.write(temp_filename)
    
    # Remove the temporary file
    os.remove(temp_filename)
    print(f"Compressed {len(records)} records to {filename}")

# Main execution
def main():
    # Generate records
    print("Generating IPDR records...")
    ipdr_records = generate_ipdr_records(IPDR_COUNT)
    
    print("Generating EDR records...")
    edr_records = generate_edr_records(EDR_COUNT)
    
    print("Generating CDR records...")
    cdr_records = generate_cdr_records(CDR_COUNT)
    
    # Export IPDR records in different formats
    export_to_json(ipdr_records, "generated_data/ipdr_records.json")
    export_to_csv(ipdr_records, "generated_data/ipdr_records.csv")
    export_to_xml(ipdr_records, "generated_data/ipdr_records.xml", "ipdr_data", "ipdr_record")
    export_to_gzip(ipdr_records, "generated_data/ipdr_records.json.gz", export_to_json)
    export_to_zip(ipdr_records, "generated_data/ipdr_records.csv.zip", export_to_csv)
    
    # Export EDR records in different formats
    export_to_json(edr_records, "generated_data/edr_records.json")
    export_to_csv(edr_records, "generated_data/edr_records.csv")
    export_to_xml(edr_records, "generated_data/edr_records.xml", "edr_data", "edr_record")
    export_to_gzip(edr_records, "generated_data/edr_records.json.gz", export_to_json)
    export_to_zip(edr_records, "generated_data/edr_records.csv.zip", export_to_csv)
    
    # Export CDR records in different formats
    export_to_json(cdr_records, "generated_data/cdr_records.json")
    export_to_csv(cdr_records, "generated_data/cdr_records.csv")
    export_to_xml(cdr_records, "generated_data/cdr_records.xml", "cdr_data", "cdr_record")
    export_to_gzip(cdr_records, "generated_data/cdr_records.json.gz", export_to_json)
    export_to_zip(cdr_records, "generated_data/cdr_records.csv.zip", export_to_csv)
    
    print("\nData generation complete. Files are in the 'generated_data' directory.")
    print("\nTarget formats for transformation should be:")
    print("IPDR: user_id timestamp domain ip_dst port protocol duration bytes_sent bytes_received vpn_usage is_fraud")
    print("EDR: event_id user_id device_model os_type roaming_status network_type event_time location event_type is_fraud")
    print("CDR: caller_id callee_id call_start call_duration call_type cell_id location imei is_fraud")

if __name__ == "__main__":
    main()