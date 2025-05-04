from kafka import KafkaConsumer
from prometheus_client import start_http_server, Gauge
import json

KAFKA_BROKER = "host.docker.internal:9092"
TOPICS = ["traffic_volume", "traffic_speed", "traffic_busiest", "traffic_congestion", "traffic_speed_drops"]

consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

traffic_volume_gauge = Gauge("traffic_volume", "Vehicle volume in last 5 min", ["sensor_id"])
traffic_speed_gauge = Gauge("traffic_speed", "Average speed in last 10 min", ["sensor_id"])
traffic_busiest_gauge = Gauge("traffic_busiest", "Busiest roads (30 min volume)", ["sensor_id"])
traffic_congestion_gauge = Gauge("traffic_congestion", "Congestion hotspots", ["sensor_id"])
traffic_speed_drops_gauge = Gauge("traffic_speed_drops", "Sudden speed drops", ["sensor_id"])

start_http_server(8000)

def process_message(topic, message):
    sensor_id = message.get("sensor_id")
    if not sensor_id:
        return

    if topic == "traffic_volume":
        traffic_volume_gauge.labels(sensor_id=sensor_id).set(message["volume_5min"])
    
    elif topic == "traffic_speed":
        traffic_speed_gauge.labels(sensor_id=sensor_id).set(message["avg_speed"])
    
    elif topic == "traffic_busiest":
        traffic_busiest_gauge.labels(sensor_id=sensor_id).set(message["volume_30min"])
    
    elif topic == "traffic_congestion":
        traffic_congestion_gauge.labels(sensor_id=sensor_id).set(1)
    
    elif topic == "traffic_speed_drops":
        traffic_speed_drops_gauge.labels(sensor_id=sensor_id).set(1)

for msg in consumer:
    try:
        process_message(msg.topic, msg.value)
    except Exception as e:
        print(f"Error processing message from {msg.topic}: {e}")