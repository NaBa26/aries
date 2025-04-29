import os
import json
import csv
import gzip
import zipfile
import xml.etree.ElementTree as ET
import datetime
import pandas as pd
from typing import List, Dict, Any, Tuple, Optional

# Define the target schemas
TARGET_SCHEMAS = {
    'ipdr': [
        'user_id', 'timestamp', 'domain', 'ip_dst', 'port', 'protocol', 
        'duration', 'bytes_sent', 'bytes_received', 'vpn_usage', 'is_fraud'
    ],
    'edr': [
        'event_id', 'user_id', 'device_model', 'os_type', 'roaming_status',
        'network_type', 'event_time', 'location', 'event_type', 'is_fraud'
    ],
    'cdr': [
        'caller_id', 'callee_id', 'call_start', 'call_duration', 'call_type',
        'cell_id', 'location', 'imei', 'is_fraud'
    ]
}

# Field mapping dictionaries for each record type
# This maps all possible field names to the target field name
IPDR_FIELD_MAPPING = {
    # user_id variations
    'user_id': 'user_id',
    'subscriber_id': 'user_id',
    'customer_id': 'user_id',
    'msisdn': 'user_id',
    
    # timestamp variations
    'timestamp': 'timestamp',
    'access_time': 'timestamp',
    'connection_time': 'timestamp',
    'event_timestamp': 'timestamp',
    
    # domain variations
    'domain': 'domain',
    'destination_domain': 'domain',
    'host': 'domain',
    'url': 'domain',
    
    # ip_dst variations
    'ip_dst': 'ip_dst',
    'destination_ip': 'ip_dst',
    'remote_ip': 'ip_dst',
    'server_ip': 'ip_dst',
    
    # port variations
    'port': 'port',
    'dest_port': 'port',
    'server_port': 'port',
    'remote_port': 'port',
    
    # protocol variations
    'protocol': 'protocol',
    'service_protocol': 'protocol',
    'conn_protocol': 'protocol',
    'transport_protocol': 'protocol',
    
    # duration variations
    'duration': 'duration',
    'session_duration': 'duration',
    'connection_time': 'duration',
    'time_spent': 'duration',
    
    # bytes_sent variations
    'bytes_sent': 'bytes_sent',
    'upload_bytes': 'bytes_sent',
    'upstream_data': 'bytes_sent',
    'tx_bytes': 'bytes_sent',
    
    # bytes_received variations
    'bytes_received': 'bytes_received',
    'download_bytes': 'bytes_received',
    'downstream_data': 'bytes_received',
    'rx_bytes': 'bytes_received',
    
    # vpn_usage variations
    'vpn_usage': 'vpn_usage',
    'vpn_detected': 'vpn_usage',
    'is_vpn': 'vpn_usage',
    'encrypted_tunnel': 'vpn_usage',
    
    # is_fraud
    'is_fraud': 'is_fraud'
}

EDR_FIELD_MAPPING = {
    # event_id variations
    'event_id': 'event_id',
    'session_id': 'event_id',
    'transaction_id': 'event_id',
    'tracking_id': 'event_id',
    
    # user_id variations
    'user_id': 'user_id',
    'subscriber_id': 'user_id',
    'msisdn': 'user_id',
    'phone_number': 'user_id',
    
    # device_model variations
    'device_model': 'device_model',
    'device_type': 'device_model',
    'handset': 'device_model',
    'terminal_model': 'device_model',
    
    # os_type variations
    'os_type': 'os_type',
    'os_version': 'os_type',
    'platform': 'os_type',
    'device_os': 'os_type',
    
    # roaming_status variations
    'roaming_status': 'roaming_status',
    'is_roaming': 'roaming_status',
    'roaming_active': 'roaming_status',
    'home_network': 'roaming_status',
    
    # network_type variations
    'network_type': 'network_type',
    'connection_type': 'network_type',
    'access_technology': 'network_type',
    'bearer_type': 'network_type',
    
    # event_time variations
    'event_time': 'event_time',
    'timestamp': 'event_time',
    'log_time': 'event_time',
    'recorded_at': 'event_time',
    
    # location variations
    'location': 'location',
    'geo_coordinates': 'location',
    'position': 'location',
    'cell_location': 'location',
    
    # event_type variations
    'event_type': 'event_type',
    'action': 'event_type',
    'event_name': 'event_type',
    'service_action': 'event_type',
    
    # is_fraud
    'is_fraud': 'is_fraud'
}

CDR_FIELD_MAPPING = {
    # caller_id variations
    'caller_id': 'caller_id',
    'calling_party': 'caller_id',
    'a_number': 'caller_id',
    'originating_number': 'caller_id',
    
    # callee_id variations
    'callee_id': 'callee_id',
    'called_party': 'callee_id',
    'b_number': 'callee_id',
    'terminating_number': 'callee_id',
    
    # call_start variations
    'call_start': 'call_start',
    'start_time': 'call_start',
    'setup_time': 'call_start',
    'connection_time': 'call_start',
    
    # call_duration variations
    'call_duration': 'call_duration',
    'duration': 'call_duration',
    'call_length': 'call_duration',
    'billable_seconds': 'call_duration',
    
    # call_type variations
    'call_type': 'call_type',
    'service_type': 'call_type',
    'call_category': 'call_type',
    'communication_type': 'call_type',
    
    # cell_id variations
    'cell_id': 'cell_id',
    'cell_identity': 'cell_id',
    'tower_id': 'cell_id',
    'bts_id': 'cell_id',
    
    # location variations
    'location': 'location',
    'geo_coordinates': 'location',
    'tower_location': 'location',
    'subscriber_location': 'location',
    
    # imei variations
    'imei': 'imei',
    'device_id': 'imei',
    'terminal_id': 'imei',
    'equipment_id': 'imei',
    
    # is_fraud
    'is_fraud': 'is_fraud'
}

# Master mapping dictionary
FIELD_MAPPINGS = {
    'ipdr': IPDR_FIELD_MAPPING,
    'edr': EDR_FIELD_MAPPING,
    'cdr': CDR_FIELD_MAPPING
}

# Functions to handle various timestamp formats
def normalize_timestamp(timestamp_value: str) -> Optional[str]:
    """
    Normalizes various timestamp formats to standard 'YYYY-MM-DD HH:MM:SS'
    """
    if timestamp_value is None:
        return None
        
    timestamp_str = str(timestamp_value).strip()
    
    # Try various formats
    formats_to_try = [
        '%Y-%m-%d %H:%M:%S',    # Standard format
        '%d/%m/%Y %H:%M:%S',    # DD/MM/YYYY
        '%m/%d/%Y %H:%M:%S',    # MM/DD/YYYY
        '%Y-%m-%dT%H:%M:%S.%fZ' # ISO format
    ]
    
    for fmt in formats_to_try:
        try:
            dt = datetime.datetime.strptime(timestamp_str, fmt)
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        except ValueError:
            pass
    
    # Try Unix timestamp
    try:
        dt = datetime.datetime.fromtimestamp(float(timestamp_str))
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    except (ValueError, OverflowError):
        pass
    
    # If we can't parse it, return as is
    return timestamp_str

def normalize_boolean(value: Any) -> Optional[bool]:
    """Normalize various boolean representations."""
    if value is None:
        return None
        
    if isinstance(value, bool):
        return value
        
    if isinstance(value, str):
        value = value.lower().strip()
        if value in ('true', 't', 'yes', 'y', '1'):
            return True
        if value in ('false', 'f', 'no', 'n', '0'):
            return False
            
    # Try numeric conversion
    try:
        return bool(int(value))
    except (ValueError, TypeError):
        return None

# File readers
def read_json_file(file_path: str) -> List[Dict]:
    """Read records from a JSON file."""
    with open(file_path, 'r') as f:
        return json.load(f)

def read_csv_file(file_path: str) -> List[Dict]:
    """Read records from a CSV file."""
    records = []
    with open(file_path, 'r', newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Convert empty strings to None
            cleaned_row = {k: (v if v != '' else None) for k, v in row.items()}
            records.append(cleaned_row)
    return records

def read_xml_file(file_path: str, record_tag: str) -> List[Dict]:
    """Read records from an XML file."""
    records = []
    tree = ET.parse(file_path)
    root = tree.getroot()
    
    for record_elem in root.findall(f'.//{record_tag}'):
        record = {}
        for child in record_elem:
            # Convert text 'None' to Python None
            value = child.text
            if value == 'None' or value is None:
                record[child.tag] = None
            # Convert boolean text to actual booleans
            elif value.lower() in ('true', 'false'):
                record[child.tag] = value.lower() == 'true'
            else:
                record[child.tag] = value
        records.append(record)
    
    return records

def read_gzip_json_file(file_path: str) -> List[Dict]:
    """Read records from a gzipped JSON file."""
    with gzip.open(file_path, 'rt') as f:
        return json.load(f)

def read_zip_csv_file(file_path: str) -> List[Dict]:
    """Read records from a zipped CSV file."""
    records = []
    with zipfile.ZipFile(file_path, 'r') as zip_ref:
        # Get the first file in the archive
        file_name = zip_ref.namelist()[0]
        with zip_ref.open(file_name) as csvfile:
            # Need to decode bytes to string for CSV reader
            lines = [line.decode('utf-8') for line in csvfile]
            reader = csv.DictReader(lines)
            for row in reader:
                # Convert empty strings to None
                cleaned_row = {k: (v if v != '' else None) for k, v in row.items()}
                records.append(cleaned_row)
    return records

# Main mapper function
def map_to_standard_schema(records: List[Dict], record_type: str) -> List[Dict]:
    """
    Map records with varying field names to a standardized schema
    """
    field_mapping = FIELD_MAPPINGS[record_type]
    target_schema = TARGET_SCHEMAS[record_type]
    standardized_records = []
    
    for record in records:
        # Create a new record with standardized field names
        std_record = {}
        
        # First pass - apply known mappings
        for source_field, value in record.items():
            if source_field in field_mapping:
                target_field = field_mapping[source_field]
                
                # Skip if we already have a value for this target field
                if target_field in std_record:
                    continue
                
                # Normalize timestamp fields
                if 'time' in target_field or 'timestamp' in target_field or 'start' in target_field:
                    std_record[target_field] = normalize_timestamp(value)
                # Normalize boolean fields
                elif 'vpn' in target_field or 'roaming' in target_field or 'fraud' in target_field:
                    std_record[target_field] = normalize_boolean(value)
                else:
                    std_record[target_field] = value
        
        # Check for missing fields
        missing_fields = [field for field in target_schema if field not in std_record]
        
        # If we have missing fields, try to infer mappings
        if missing_fields and len(missing_fields) < len(target_schema):
            # Print warning about missing fields
            missing_fields_str = ', '.join(missing_fields)
            print(f"Warning: Record missing fields: {missing_fields_str}")
            
            # Add placeholder nulls for missing fields
            for field in missing_fields:
                std_record[field] = None
        
        # Only add records that have at least some of the required fields
        if std_record:
            standardized_records.append(std_record)
    
    return standardized_records

def identify_record_type(file_path: str) -> str:
    """Identify record type from filename."""
    filename = os.path.basename(file_path).lower()
    if 'ipdr' in filename:
        return 'ipdr'
    elif 'edr' in filename:
        return 'edr'
    elif 'cdr' in filename:
        return 'cdr'
    else:
        raise ValueError(f"Unknown record type for file: {file_path}")

def detect_file_format(file_path: str) -> Tuple[str, dict]:
    """
    Detect the file format and return the appropriate reader function and parameters.
    """
    filename = file_path.lower()
    
    if filename.endswith('.json'):
        return ('json', {'file_path': file_path})
    elif filename.endswith('.csv'):
        return ('csv', {'file_path': file_path})
    elif filename.endswith('.xml'):
        record_type = identify_record_type(file_path)
        record_tag = f"{record_type}_record"
        return ('xml', {'file_path': file_path, 'record_tag': record_tag})
    elif filename.endswith('.json.gz'):
        return ('gzip_json', {'file_path': file_path})
    elif filename.endswith('.csv.zip'):
        return ('zip_csv', {'file_path': file_path})
    else:
        raise ValueError(f"Unsupported file format: {file_path}")

def read_records_from_file(file_path: str) -> List[Dict]:
    """
    Read records from a file based on its detected format.
    """
    format_type, params = detect_file_format(file_path)
    
    readers = {
        'json': read_json_file,
        'csv': read_csv_file,
        'xml': read_xml_file,
        'gzip_json': read_gzip_json_file,
        'zip_csv': read_zip_csv_file
    }
    
    if format_type not in readers:
        raise ValueError(f"Unsupported format: {format_type}")
    
    reader_func = readers[format_type]
    return reader_func(**params)

def process_directory(directory_path: str, output_dir: str = None):
    """
    Process all files in a directory, automatically mapping fields.
    """
    if output_dir is None:
        output_dir = os.path.join(directory_path, 'standardized')
    
    os.makedirs(output_dir, exist_ok=True)
    
    # Process each file
    for filename in os.listdir(directory_path):
        file_path = os.path.join(directory_path, filename)
        
        # Skip directories and non-data files
        if os.path.isdir(file_path) or not any(ext in filename.lower() for ext in ['.json', '.csv', '.xml', '.gz', '.zip']):
            continue
        
        try:
            print(f"Processing {filename}...")
            
            # Identify record type
            record_type = identify_record_type(file_path)
            
            # Read records
            records = read_records_from_file(file_path)
            print(f"  Read {len(records)} records")
            
            # Map to standard schema
            standardized_records = map_to_standard_schema(records, record_type)
            print(f"  Mapped to {len(standardized_records)} standardized records")
            
            # Get stats on mapped fields
            if standardized_records:
                field_coverage = {field: sum(1 for r in standardized_records if r.get(field) is not None) / len(standardized_records) * 100 
                                 for field in TARGET_SCHEMAS[record_type]}
                
                print("  Field mapping coverage:")
                for field, coverage in field_coverage.items():
                    print(f"    {field}: {coverage:.1f}%")
            
            # Save standardized records
            output_filename = f"standardized_{record_type}_{os.path.splitext(filename)[0]}.csv"
            output_path = os.path.join(output_dir, output_filename)
            
            df = pd.DataFrame(standardized_records)
            # Reorder columns to match target schema
            df = df.reindex(columns=TARGET_SCHEMAS[record_type])
            df.to_csv(output_path, index=False)
            
            print(f"  Saved to {output_path}")
            
        except Exception as e:
            print(f"Error processing {filename}: {str(e)}")

def process_specific_record_type(directory_path: str, record_type: str, output_dir: str = None):
    """
    Process only files of a specific record type (CDR, IPDR, EDR).
    """
    if output_dir is None:
        output_dir = os.path.join(directory_path, f'standardized_{record_type}')
    
    os.makedirs(output_dir, exist_ok=True)
    
    # Process each file
    for filename in os.listdir(directory_path):
        file_path = os.path.join(directory_path, filename)
        
        # Skip directories and files not matching the requested record type
        if os.path.isdir(file_path) or record_type not in filename.lower():
            continue
        
        try:
            print(f"Processing {filename}...")
            
            # Read records
            records = read_records_from_file(file_path)
            print(f"  Read {len(records)} records")
            
            # Map to standard schema
            standardized_records = map_to_standard_schema(records, record_type)
            print(f"  Mapped to {len(standardized_records)} standardized records")
            
            # Get stats on mapped fields
            if standardized_records:
                field_coverage = {field: sum(1 for r in standardized_records if r.get(field) is not None) / len(standardized_records) * 100 
                                 for field in TARGET_SCHEMAS[record_type]}
                
                print("  Field mapping coverage:")
                for field, coverage in field_coverage.items():
                    print(f"    {field}: {coverage:.1f}%")
            
            # Save standardized records
            output_filename = f"standardized_{os.path.splitext(filename)[0]}.csv"
            output_path = os.path.join(output_dir, output_filename)
            
            df = pd.DataFrame(standardized_records)
            # Reorder columns to match target schema
            df = df.reindex(columns=TARGET_SCHEMAS[record_type])
            df.to_csv(output_path, index=False)
            
            print(f"  Saved to {output_path}")
            
        except Exception as e:
            print(f"Error processing {filename}: {str(e)}")

def analyze_unknown_fields(directory_path: str):
    """
    Analyze files to detect unknown field names that aren't in our mapping dictionaries.
    This is useful for discovering new field variations.
    """
    unknown_fields = {
        'ipdr': set(),
        'edr': set(),
        'cdr': set()
    }
    
    # Process each file
    for filename in os.listdir(directory_path):
        file_path = os.path.join(directory_path, filename)
        
        # Skip directories and non-data files
        if os.path.isdir(file_path) or not any(ext in filename.lower() for ext in ['.json', '.csv', '.xml', '.gz', '.zip']):
            continue
        
        try:
            # Identify record type
            record_type = identify_record_type(file_path)
            
            # Read records
            records = read_records_from_file(file_path)
            
            # Check for unknown fields
            known_fields = set(FIELD_MAPPINGS[record_type].keys())
            for record in records:
                for field in record.keys():
                    if field not in known_fields:
                        unknown_fields[record_type].add(field)
            
        except Exception as e:
            print(f"Error analyzing {filename}: {str(e)}")
    
    # Report unknown fields
    print("\nUnknown Fields Analysis:")
    for record_type, fields in unknown_fields.items():
        if fields:
            print(f"\n{record_type.upper()} Unknown Fields:")
            for field in sorted(fields):
                print(f"  - '{field}'")
        else:
            print(f"\n{record_type.upper()}: No unknown fields found.")
    
    return unknown_fields

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Process telecom data with automatic field mapping')
    parser.add_argument('--dir', type=str, required=True, help='Directory containing data files')
    parser.add_argument('--output', type=str, default=None, help='Output directory for standardized files')
    parser.add_argument('--type', type=str, choices=['ipdr', 'edr', 'cdr', 'all'], default='all', 
                        help='Process only specific record type')
    parser.add_argument('--analyze', action='store_true', help='Analyze unknown fields')
    
    args = parser.parse_args()
    
    if args.analyze:
        analyze_unknown_fields(args.dir)
    elif args.type == 'all':
        process_directory(args.dir, args.output)
    else:
        process_specific_record_type(args.dir, args.type, args.output)