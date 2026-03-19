import json
import time
import random
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

TOPIC="weather_stream"

while True:

    weather = {
        "track":"Monza",
        "timestamp":int(time.time()*1000),
        "temperature":random.uniform(20,35),
        "rain_intensity":random.uniform(0,1)
    }

    producer.send(TOPIC,value=weather)

    print(weather)

    time.sleep(5)