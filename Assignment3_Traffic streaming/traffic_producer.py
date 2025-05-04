import time
import random
import json
from kafka import KafkaProducer

TOPIC = "traffic_data"

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

def generate_traffic_event():
    sensors = ['S101', 'S102', 'S103', 'S104', 'S105']
    current_time = time.time()
    event = {
        "sensor_id": random.choice(sensors),
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(current_time)),
        "vehicle_count": random.randint(0, 50),
        "average_speed": round(random.uniform(20.0, 80.0), 1),
        "congestion_level": random.choice(["LOW", "MEDIUM", "HIGH"])
    }
    return event

def main():
    try:
        while True:
            event = generate_traffic_event()
            producer.send(TOPIC, event)
            print("Produced event:", event)
            time.sleep(1)
    except KeyboardInterrupt:
        print("Producer stopped by user.")
    finally:
        producer.close()

if __name__ == "__main__":
    main()
