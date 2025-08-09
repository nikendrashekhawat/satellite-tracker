import requests
import json
import time
import os
from confluent_kafka import Producer

print("Producer script started")

# Load Kafka broker address
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = "satellite_position"

# Configure Confluent Kafka producer
conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'client.id': 'iss-producer'
}
producer = Producer(conf)

def delivery_report(err, msg):
    """Delivery callback called (from poll) for each produced message."""
    if err is not None:
        print(f"Delivery failed for record {msg.key()}: {err}")
    else:
        print(f"Record successfully produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def fetch_iss_location():
    url = "http://api.open-notify.org/iss-now.json"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        position = {
            "timestamp": data["timestamp"],
            "latitude": data["iss_position"]["latitude"],
            "longitude": data["iss_position"]["longitude"]
        }
        return position
    else:
        print(f"Failed to fetch ISS location. Status code: {response.status_code}")
        return None

if __name__ == "__main__":
    print("Hello Kafka here! This is my first confluent-kafka program...")
    print("Starting ISS Confluent Kafka producer...")
    while True:
        iss_position = fetch_iss_location()
        if iss_position:
            print("Sending data:", iss_position)
            producer.produce(
                TOPIC,
                key=str(iss_position["timestamp"]),  # Optional: use timestamp as key
                value=json.dumps(iss_position),
                callback=delivery_report
            )
            producer.poll(0)  # Trigger delivery callbacks
        time.sleep(10)  # Fetch every 10 seconds
