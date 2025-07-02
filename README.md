# 🌦️ Weather Data Pipeline Project

This project implements a real-time weather data pipeline that collects weather data from the OpenWeatherMap API, processes it using Kafka and Spark, stores it in AWS S3, and ingests it into Snowflake using Snowpipe. The entire flow is optionally orchestrated with Apache Airflow.

## 📊 Architecture Overview

OpenWeatherMap API
↓
Kafka Producer → Kafka Topic
↓
Spark Consumer → CSV Output (partitioned by city)
↓
AWS S3 Bucket (organized by city)
↓
Snowpipe → Snowflake Table (weather_data)

## ⚙️ Technologies Used

- Python (API calls, Kafka producer/consumer)
- Apache Kafka (event streaming)
- Apache Spark (data transformation & batching)
- AWS S3 (cloud storage)
- Snowflake (data warehouse)
- Snowpipe (automated data loading)
- Apache Airflow (workflow orchestration - optional)

## 🗂️ Project Structure

├── producer.py # Kafka producer - fetches weather data
├── consumer.py # Kafka consumer - saves batch data to S3
├── spark_consumer.py # Spark job to read from Kafka and write to S3
├── airflow/
│ └── weather_data_pipeline.py # Optional Airflow DAG
├── snowflake_setup.sql # Snowflake table, stage, pipe setup
├── s3_client.py # Boto3 S3 client helper
├── .env.template # Environment variables template
├── sample_data/
│ └── example_weather.csv # Example CSV for reference
└── README.md

## 🚀 How to Run the Pipeline

1. **Set environment variables**
   - Copy `.env.template` to `.env` and fill in your keys

2. **Run Kafka Producer**
   ```bash
   python producer.py

3. **Run Spark Consumer to store in S3**
    ```bash
    spark-submit spark_consumer.py

4. **Run Kafka Batch Consumer (optional)**
    ```bash
    python consumer.py

5. **Load data into Snowflake**

    Run Snowflake queries from snowflake_setup.sql

    Manually trigger Snowpipe (if AUTO_INGEST = FALSE)

    ```sql
    ALTER PIPE weather_pipe REFRESH;

6. **View data**
    ```sql
    SELECT * FROM WEATHER_DB.RAW_DATA.WEATHER_DATA;