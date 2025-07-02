import json
import requests
from kafka import KafkaProducer
from dotenv import load_dotenv
import os
import logging
import time

# Load environment variables from .env
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

API_KEY = os.getenv("API_KEY")
CITIES = os.getenv("CITIES", "London").split(",")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "weather")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

if not API_KEY:
    logging.error("API_KEY is missing.")
    exit(1)

# Kafka producer setup
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def get_weather(city):
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"
    try:
        response = requests.get(url)
        data = response.json()

        if response.status_code != 200:
            logging.warning(f"{city} error: {data.get('message', 'Unknown')}")
            return None

        return {
            "city": data["name"],
            "temperature": data["main"]["temp"],
            "description": data["weather"][0]["description"],
            "timestamp": time.time()
        }

    except Exception as e:
        logging.error(f"Exception for {city}: {e}")
        return None

def main():
    for city in CITIES:
        data = get_weather(city)
        if data:
            producer.send(KAFKA_TOPIC, data)
            logging.info(f"Sent to Kafka: {data}")
    producer.flush()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.error(f"Producer failed: {e}")
