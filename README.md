# Aries

## Data Ingestion & Transformation Pipeline for Fraud Detection

Aries is a data pipeline designed to feed structured data into ML models for fraud detection. The system handles both batch processing and real-time streaming data.

## Overview

- Processes multiple data formats (JSON, CSV, ZIP) into standardized structure
- Supports batch processing for historical analysis
- Enables real-time streaming for immediate fraud detection
- Uses Faker to generate synthetic test data for both modes
- Stores processed data in PostgreSQL (separate databases for IPDR, EDR and CDR)

## Technologies

- **Apache Kafka** (in Docker): Message broker for real-time data streaming
- **Apache Spark (PySpark)** (on host): Distributed data processing
- **PostgreSQL** (in Docker): Data storage
- **Python**: Primary programming language
- **Faker**: Synthetic data generation

## Data Flow

**Batch Processing:**
- Data sources are manually crawled and collected in various formats
- Manual column mapping transforms diverse formats into standardized CSV
- PySpark jobs process and transform the data
- Results stored in PostgreSQL databases
- ML models analyze the processed data

**Real-time Streaming:**
- Live data streams ingested through Kafka topics
- Dedicated consumers for IPDR, CDR, and EDR data types
- PySpark Streaming transforms data in real-time
- Processed events stored in PostgreSQL and fed to ML models
- Alerts generated for potential fraud cases

## Setup

```bash
# Clone the repository
git clone https://github.com/NaBa26/aries
cd aries

# Install dependencies
pip install -r requirements.txt

# Start Kafka and PostgreSQL using Docker
docker-compose up -d

# Configure database connections in config/db_config.yml
```

## Usage

**Start container:**
```bash
sudo docker-compose up -d
```
**Generate Test Data:**
```bash
python fake_data_generator.py
python file_crawler.py #to generate the final data
```

**Batch Processing:**
```bash
cd scripts
python producer.py
```

**Stream Processing:**
```bash
python live_processor.py
python live_ipdr_consumer.py #for ipdr
python live_cdr_consumer.py  #for cdr
python live_edr_consumer.py  #for edr
```

## Future Improvements

- Apache NiFi integration for automated data crawling
- ML-based approaches to automatically identify and map similar columns
- Apache Flink to replace custom stream consumers for better isolation and automation
- Enhanced monitoring and alerting capabilities
