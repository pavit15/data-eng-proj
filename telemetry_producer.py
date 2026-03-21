import json
import time
import random
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode(),
)

TOPIC = "telemetry_stream"
DRIVERS = ["HAM","VER","LEC"]

def generate_event():
    driver = random.choice(DRIVERS)

    # 🔥 Inject anomaly
    if random.random() < 0.1:
        speed = random.uniform(50,500)
    else:
        speed = random.uniform(250,340)

    return {
        "driver_id": driver,
        "track": "Monza",
        "timestamp": int(time.time()*1000),
        "speed": speed,
        "rpm": random.randint(12000,15000),

        # 🔥 movement for heatmap
        "lat": 19.076 + random.uniform(-0.01,0.01),
        "lon": 72.877 + random.uniform(-0.01,0.01)
    }

print("Producer started")

while True:
    e = generate_event()
    producer.send(TOPIC,key=e["driver_id"],value=e)
    print(e)
    time.sleep(0.2)