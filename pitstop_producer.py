import json
import time
import random
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

TOPIC="pitstop_stream"
DRIVERS=["HAM","VER","LEC"]

while True:

    event={
        "driver_id":random.choice(DRIVERS),
        "timestamp":int(time.time()*1000),
        "pit_duration":random.uniform(2,4),
        "tire":"Soft"
    }

    producer.send(TOPIC,value=event)

    print(event)

    time.sleep(random.randint(10,20))