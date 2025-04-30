import os
import json
import csv
import gzip
import zipfile
import xml.etree.ElementTree as ET
import datetime
import pandas as pd
from typing import List, Dict, Any, Tuple, Optional

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

IPDR_FIELD_MAPPING = {
    'user_id': 'user_id',
    'subscriber_id': 'user_id',
    'customer_id': 'user_id',
    'msisdn': 'user_id',
    
    'timestamp': 'timestamp',
    'access_time': 'timestamp',
    'connection_time': 'timestamp',
    'event_timestamp': 'timestamp',

    'domain': 'domain',
    'destination_domain': 'domain',
    'host': 'domain',
    'url': 'domain',

    'ip_dst': 'ip_dst',
    'destination_ip': 'ip_dst',
    'remote_ip': 'ip_dst',
    'server_ip': 'ip_dst',

    'port': 'port',
    'dest_port': 'port',
    'server_port': 'port',
    'remote_port': 'port',

    'protocol': 'protocol',
    'service_protocol': 'protocol',
    'conn_protocol': 'protocol',
    'transport_protocol': 'protocol',

    'duration': 'duration',
    'session_duration': 'duration',
    'connection_time': 'duration',
    'time_spent': 'duration',

    'bytes_sent': 'bytes_sent',
    'upload_bytes': 'bytes_sent',
    'upstream_data': 'bytes_sent',
    'tx_bytes': 'bytes_sent',
    
    # bytes_received variations
    'bytes_received': 'bytes_received',
    'download_bytes': 'bytes_received',
    'downstream_data': 'bytes_received',
    'rx_bytes': 'bytes_received',

    'vpn_usage': 'vpn_usage',
    'vpn_detected': 'vpn_usage',
    'is_vpn': 'vpn_usage',
    'encrypted_tunnel': 'vpn_usage',

    'is_fraud': 'is_fraud'
}

EDR_FIELD_MAPPING = {
    'event_id': 'event_id',
    'session_id': 'event_id',
    'transaction_id': 'event_id',
    'tracking_id': 'event_id',

    'user_id': 'user_id',
    'subscriber_id': 'user_id',
    'msisdn': 'user_id',
    'phone_number': 'user_id',

    'device_model': 'device_model',
    'device_type': 'device_model',
    'handset': 'device_model',
    'terminal_model': 'device_model',

    'os_type': 'os_type',
    'os_version': 'os_type',
    'platform': 'os_type',
    'device_os': 'os_type',

    'roaming_status': 'roaming_status',
    'is_roaming': 'roaming_status',
    'roaming_active': 'roaming_status',
    'home_network': 'roaming_status',

    'network_type': 'network_type',
    'connection_type': 'network_type',
    'access_technology': 'network_type',
    'bearer_type': 'network_type',

    'event_time': 'event_time',
    'timestamp': 'event_time',
    'log_time': 'event_time',
    'recorded_at': 'event_time',
    
    'location': 'location',
    'geo_coordinates': 'location',
    'position': 'location',
    'cell_location': 'location',

    'event_type': 'event_type',
    'action': 'event_type',
    'event_name': 'event_type',
    'service_action': 'event_type',

    'is_fraud': 'is_fraud'
}

CDR_FIELD_MAPPING = {
    'caller_id': 'caller_id',
    'calling_party': 'caller_id',
    'a_number': 'caller_id',
    'originating_number': 'caller_id',
    
    'callee_id': 'callee_id',
    'called_party': 'callee_id',
    'b_number': 'callee_id',
    'terminating_number': 'callee_id',

    'call_start': 'call_start',
    'start_time': 'call_start',
    'setup_time': 'call_start',
    'connection_time': 'call_start',

    'call_duration': 'call_duration',
    'duration': 'call_duration',
    'call_length': 'call_duration',
    'billable_seconds': 'call_duration',

    'call_type': 'call_type',
    'service_type': 'call_type',
    'call_category': 'call_type',
    'communication_type': 'call_type',

    'cell_id': 'cell_id',
    'cell_identity': 'cell_id',
    'tower_id': 'cell_id',
    'bts_id': 'cell_id',

    'location': 'location',
    'geo_coordinates': 'location',
    'tower_location': 'location',
    'subscriber_location': 'location',

    'imei': 'imei',
    'device_id': 'imei',
    'terminal_id': 'imei',
    'equipment_id': 'imei',

    'is_fraud': 'is_fraud'
}

FIELD_MAPPINGS = {
    'ipdr': IPDR_FIELD_MAPPING,
    'edr': EDR_FIELD_MAPPING,
    'cdr': CDR_FIELD_MAPPING
}

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
    
    try:
        dt = datetime.datetime.fromtimestamp(float(timestamp_str))
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    except (ValueError, OverflowError):
        pass

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

    try:
        return bool(int(value))
    except (ValueError, TypeError):
        return None

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
            value = child.text
            if value == 'None' or value is None:
                record[child.tag] = None
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
        file_name = zip_ref.namelist()[0]
        with zip_ref.open(file_name) as csvfile:
            lines = [line.decode('utf-8') for line in csvfile]
            reader = csv.DictReader(lines)
            for row in reader:
                cleaned_row = {k: (v if v != '' else None) for k, v in row.items()}
                records.append(cleaned_row)
    return records

def map_to_standard_schema(records: List[Dict], record_type: str) -> List[Dict]:
    """
    Map records with varying field names to a standardized schema
    """
    field_mapping = FIELD_MAPPINGS[record_type]
    target_schema = TARGET_SCHEMAS[record_type]
    standardized_records = []
    
    for record in records:
        std_record = {}

        for source_field, value in record.items():
            if source_field in field_mapping:
                target_field = field_mapping[source_field]
                
                if target_field in std_record:
                    continue

                if 'time' in target_field or 'timestamp' in target_field or 'start' in target_field:
                    std_record[target_field] = normalize_timestamp(value)
                elif 'vpn' in target_field or 'roaming' in target_field or 'fraud' in target_field:
                    std_record[target_field] = normalize_boolean(value)
                else:
                    std_record[target_field] = value

        missing_fields = [field for field in target_schema if field not in std_record]

        if missing_fields and len(missing_fields) < len(target_schema):
            missing_fields_str = ', '.join(missing_fields)
            print(f"Warning: Record missing fields: {missing_fields_str}")

            for field in missing_fields:
                std_record[field] = None

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
        # Check parent directory name if not in filename
        parent_dir = os.path.basename(os.path.dirname(file_path)).lower()
        if 'ipdr' in parent_dir:
            return 'ipdr'
        elif 'edr' in parent_dir:
            return 'edr'
        elif 'cdr' in parent_dir:
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

def process_file(file_path: str, temp_dir: str) -> Tuple[str, str, str]:
    """
    Process a single file and return the record type and path to the standardized CSV.
    """
    try:
        record_type = identify_record_type(file_path)
        records = read_records_from_file(file_path)
        print(f"  Read {len(records)} records from {os.path.basename(file_path)}")

        standardized_records = map_to_standard_schema(records, record_type)
        print(f"  Mapped to {len(standardized_records)} standardized records")

        if not standardized_records:
            print(f"  No valid records found in {os.path.basename(file_path)}")
            return None, None, None

        # Create a unique filename for the standardized file
        file_basename = os.path.splitext(os.path.basename(file_path))[0]
        output_filename = f"std_{record_type}_{file_basename}.csv"
        output_path = os.path.join(temp_dir, output_filename)
        
        # Save to temporary CSV
        df = pd.DataFrame(standardized_records)
        df = df.reindex(columns=TARGET_SCHEMAS[record_type])
        df.to_csv(output_path, index=False)
        
        print(f"  Saved temporary standardized CSV: {output_filename}")
        
        return record_type, output_path, file_basename
        
    except Exception as e:
        print(f"Error processing {file_path}: {str(e)}")
        return None, None, None

def crawl_directories(base_path: str, final_output_dir: str):
    """
    Crawl through subdirectories, process all files, and merge them by record type.
    """
    # Create temporary directory for standardized files
    script_dir = os.path.dirname(os.path.abspath(__file__))
    temp_dir = os.path.join(script_dir, "temp_standardized")
    os.makedirs(temp_dir, exist_ok=True)
    
    # Create final output directory (one level above script location)
    parent_dir = os.path.dirname(script_dir)
    final_dir = os.path.join(parent_dir, final_output_dir)
    os.makedirs(final_dir, exist_ok=True)
    
    # Dictionary to store dataframes by record type
    merged_dfs = {
        'ipdr': [],
        'edr': [],
        'cdr': []
    }
    
    # Expected subdirectories to search in
    subdirs = ['ipdr_subdir', 'edr_subdir', 'cdr_subdir']
    
    # Path to generated_data is one level above script directory
    generated_data_path = os.path.join(parent_dir, 'generated_data')
    
    if not os.path.exists(generated_data_path):
        print(f"Error: Directory {generated_data_path} not found!")
        return
    
    print(f"Processing data from: {generated_data_path}")
    
    # Walk through the subdirectories in generated_data
    for subdir in subdirs:
        subdir_path = os.path.join(generated_data_path, subdir)
        
        if not os.path.exists(subdir_path):
            print(f"Warning: Subdirectory {subdir} not found in {generated_data_path}")
            continue
            
        print(f"\nProcessing subdirectory: {subdir}")
        
        # Process all files in the subdirectory
        for root, dirs, files in os.walk(subdir_path):
            for file in files:
                if any(file.endswith(ext) for ext in ['.json', '.csv', '.xml']) or \
                   any(file.endswith(ext) for ext in ['.json.gz', '.csv.zip']):
                    file_path = os.path.join(root, file)
                    print(f"\nProcessing file: {file_path}")
                    
                    record_type, temp_csv_path, _ = process_file(file_path, temp_dir)
                    
                    if record_type and temp_csv_path:
                        # Read the standardized CSV into a dataframe
                        try:
                            df = pd.read_csv(temp_csv_path)
                            if not df.empty:
                                merged_dfs[record_type].append(df)
                        except Exception as e:
                            print(f"Error reading temporary CSV {temp_csv_path}: {str(e)}")
    
    # Merge dataframes by record type and save final outputs
    for record_type, dfs in merged_dfs.items():
        if dfs:
            print(f"\nMerging {len(dfs)} files for {record_type} records...")
            merged_df = pd.concat(dfs, ignore_index=True)
            
            # Final output path
            final_path = os.path.join(final_dir, f"final_{record_type}.csv")
            
            # Ensure all columns from the target schema are present
            merged_df = merged_df.reindex(columns=TARGET_SCHEMAS[record_type])
            
            # Save the final merged file
            merged_df.to_csv(final_path, index=False)
            
            print(f"Saved final merged file: {final_path}")
            print(f"Total records: {len(merged_df)}")
            
            # Report field coverage
            field_coverage = {field: merged_df[field].notna().sum() / len(merged_df) * 100 
                             for field in TARGET_SCHEMAS[record_type]}
            
            print("Field coverage in merged file:")
            for field, coverage in field_coverage.items():
                print(f"  {field}: {coverage:.1f}%")
        else:
            print(f"\nNo valid files found for {record_type}")
    
    # Clean up temporary files
    print("\nCleaning up temporary files...")
    for file in os.listdir(temp_dir):
        os.remove(os.path.join(temp_dir, file))
    os.rmdir(temp_dir)
    print("Temporary files removed.")

if __name__ == "__main__":
    # Set directories
    final_output_dir = "final"
    
    # Start crawling directories and merging files
    print("Starting directory crawl and file processing...")
    crawl_directories(os.path.dirname(os.path.abspath(__file__)), final_output_dir)
    print("\nProcessing complete.")