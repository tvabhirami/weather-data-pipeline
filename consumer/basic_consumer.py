import json, os, csv, logging
from kafka import KafkaConsumer
from dotenv import load_dotenv
from datetime import datetime
from s3_client import get_s3_client
from collections import defaultdict

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# Load env
load_dotenv()

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "weather")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "weather-consumer-group")
S3_BUCKET = os.getenv("S3_BUCKET")
S3_KEY_PREFIX = os.getenv("S3_KEY_PREFIX", "weather_data/")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 5))

if not S3_BUCKET:
    raise ValueError("Missing S3 bucket")

s3 = get_s3_client()
city_batches = defaultdict(list)

def upload_to_s3(file_path, bucket, key):
    try:
        s3.upload_file(file_path, bucket, key)
        logging.info(f"Uploaded to s3://{bucket}/{key}")
    except Exception as e:
        logging.error(f"Upload error: {e}")

def main():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id=GROUP_ID,
        consumer_timeout_ms=5000  # Stop after 5 seconds of no new data
    )

    for message in consumer:
        data = message.value
        city = data.get("city")
        if not city:
            continue

        city_batches[city].append(data)

        if len(city_batches[city]) >= BATCH_SIZE:
            timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
            s3_key = f"{S3_KEY_PREFIX}{city}/weather_{timestamp}.csv"
            local_file = f"{city}_weather.csv"

            try:
                with open(local_file, "w", newline='') as f:
                    writer = csv.DictWriter(f, fieldnames=data.keys())
                    writer.writeheader()
                    writer.writerows(city_batches[city])
                upload_to_s3(local_file, S3_BUCKET, s3_key)
            except Exception as e:
                logging.error(f"Error processing batch for {city}: {e}")
            finally:
                city_batches[city].clear()
                if os.path.exists(local_file):
                    os.remove(local_file)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.info("Exiting consumer.")
