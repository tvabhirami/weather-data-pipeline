-- Create database and schema
CREATE DATABASE IF NOT EXISTS WEATHER_DB;
CREATE SCHEMA IF NOT EXISTS WEATHER_DB.RAW_DATA;
USE SCHEMA WEATHER_DB.RAW_DATA;

-- Create the target table
CREATE OR REPLACE TABLE weather_data (
    city STRING,
    temperature FLOAT,
    description STRING,
    timestamp STRING
);

-- Define file format
CREATE OR REPLACE FILE FORMAT weather_csv_format
TYPE = 'CSV'
SKIP_HEADER = 1
FIELD_OPTIONALLY_ENCLOSED_BY = '"';

-- Define external stage
CREATE OR REPLACE STAGE weather_stage
URL = 's3://my-weather-abhi/weather_data/spark_output/'
STORAGE_INTEGRATION = my_s3_int
FILE_FORMAT = weather_csv_format;

-- Define Snowpipe with city extracted from file path
CREATE OR REPLACE PIPE weather_pipe
AUTO_INGEST = FALSE
AS
COPY INTO weather_data
FROM (
  SELECT
    SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', 4), '=', 2) AS city,
    $1::FLOAT AS temperature,
    $2::STRING AS description,
    $3::STRING AS timestamp
  FROM @weather_stage
)
FILE_FORMAT = (FORMAT_NAME = weather_csv_format)
ON_ERROR = 'CONTINUE';
