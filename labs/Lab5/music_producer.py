import time
import random
import json
from kafka import KafkaProducer
from prometheus_client import Counter, start_http_server

start_http_server(8000)

song_plays = Counter(
    "song_plays_total", "Total number of song plays", ["song_id", "region"]
)

TOPIC = "music_events"
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

songs = [101, 202, 303, 404, 505
regions = ["US", "EU", "APAC"]

while True:
    song_id = random.choice(songs)
    region = random.choice(regions)
    event = {
        "song_id": song_id,
        "timestamp": time.time(),
        "region": region,
        "action": "play",
    }
    producer.send(TOPIC, event)
    song_plays.labels(song_id=song_id, region=region).inc()
    print(f"Sent event: {event}")
    time.sleep(random.uniform(0.5, 2.0))
