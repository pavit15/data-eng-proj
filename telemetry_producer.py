import json
import time
import random
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode(),
    acks="all",
    retries=5,
)

TOPIC = "telemetry_stream"
DRIVERS = ["HAM","VER","LEC"]

def generate_event():

    driver = random.choice(DRIVERS)

    event = {
        "driver_id": driver,
        "track": "Monza",
        "timestamp": int(time.time()*1000),
        "speed": random.uniform(250,340),
        "rpm": random.randint(12000,15000),
        "lat": 19.076,
        "lon": 72.877
    }

    return event

print("Telemetry producer started")

while True:

    event = generate_event()

    producer.send(TOPIC,key=event["driver_id"],value=event)

    print(event)

    time.sleep(0.2)