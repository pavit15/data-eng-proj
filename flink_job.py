import json
import psycopg2
import time
import joblib
import os

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.functions import MapFunction, CoMapFunction
from pyflink.common.restart_strategy import RestartStrategies

# ---------------- ENV ----------------
env = StreamExecutionEnvironment.get_execution_environment()

# ✅ safer checkpointing
env.enable_checkpointing(15000)

# 🔥 DEBUG MODE (disable restart to see real errors)
env.set_restart_strategy(RestartStrategies.no_restart())

env.set_parallelism(1)

# ---------------- ML SETUP ----------------
MODEL_DIR = "/tmp"
ANOMALY_PATH = os.path.join(MODEL_DIR, "anomaly_model.pkl")
TIRE_PATH = os.path.join(MODEL_DIR, "tire_model.pkl")

if not os.path.exists(ANOMALY_PATH) or not os.path.exists(TIRE_PATH):
    from ml_models import train_models
    train_models()

iso = joblib.load(ANOMALY_PATH)
xgb = joblib.load(TIRE_PATH)

# ---------------- KAFKA ----------------
def create_consumer(topic):
    props = {
        "bootstrap.servers": "kafka:29092",
        "group.id": "flink_group_final",
        "auto.offset.reset": "earliest"   # ✅ FIXED
    }
    return FlinkKafkaConsumer(topic, SimpleStringSchema(), props)

def create_producer(topic):
    return FlinkKafkaProducer(
        topic=topic,
        serialization_schema=SimpleStringSchema(),
        producer_config={"bootstrap.servers": "kafka:29092"}
    )

# ---------------- SAFE PARSE ----------------
class SafeParse(MapFunction):
    def map(self, x):
        try:
            data = json.loads(x)

            if "driver_id" not in data or "speed" not in data:
                return {"_error": True, "raw": x}

            return data

        except Exception:
            return {"_error": True, "raw": x}

# ---------------- STREAMS ----------------
telemetry_raw = env.add_source(create_consumer("telemetry_stream"))
weather_raw = env.add_source(create_consumer("weather_stream"))

telemetry_stream = telemetry_raw.map(SafeParse())
weather_stream = weather_raw.map(SafeParse())

# ---------------- DLQ ----------------
dlq_stream = telemetry_stream.filter(lambda x: "_error" in x)
valid_stream = telemetry_stream.filter(lambda x: "_error" not in x)

dlq_stream.map(lambda x: json.dumps(x)).add_sink(create_producer("dlq_topic"))

# ---------------- JOIN ----------------
class WeatherJoin(CoMapFunction):
    def __init__(self):
        self.latest_weather = None

    def map1(self, telemetry):

        # ✅ SAFE DEFAULTS
        if self.latest_weather:
            telemetry["temperature"] = self.latest_weather.get("temperature", 25.0)
            telemetry["rain_intensity"] = self.latest_weather.get("rain_intensity", 0.0)
        else:
            telemetry["temperature"] = 25.0
            telemetry["rain_intensity"] = 0.0

        return telemetry

    def map2(self, weather):
        if "_error" not in weather:
            self.latest_weather = weather
        return None

joined = valid_stream.connect(weather_stream).map(WeatherJoin()) \
    .filter(lambda x: x is not None)

# ---------------- ROLLING AVG ----------------
class AvgSpeed(MapFunction):
    def __init__(self):
        self.stats = {}

    def map(self, v):
        d = v["driver_id"]

        if d not in self.stats:
            self.stats[d] = {"sum": 0, "count": 0}

        self.stats[d]["sum"] += v["speed"]
        self.stats[d]["count"] += 1

        v["rolling_avg_speed"] = self.stats[d]["sum"] / self.stats[d]["count"]
        return v

# ---------------- ML ----------------
class ML(MapFunction):
    def map(self, v):
        import numpy as np

        # ✅ SAFE FEATURE HANDLING
        speed = v.get("speed", 300)
        temperature = v.get("temperature", 25.0)
        rain = v.get("rain_intensity", 0.0)

        features = np.array([
            speed,
            13000,
            temperature,
            rain
        ]).reshape(1, -1)

        v["anomaly_score"] = float(iso.decision_function(features)[0])
        v["tire_health"] = float(xgb.predict(features)[0])

        return v

# ---------------- ALERT ----------------
class Alert(MapFunction):
    def map(self, v):
        if v["anomaly_score"] < -0.05:
            return json.dumps(v)
        return None

# ---------------- PIPELINE ----------------
stream = joined.map(AvgSpeed()).map(ML())

alerts = stream.map(Alert()).filter(lambda x: x is not None)
alerts.add_sink(create_producer("alerts_topic"))

# ---------------- DB ----------------
class DB(MapFunction):
    def open(self, ctx):
        self.conn = psycopg2.connect(
            host="postgres",
            database="telemetry",
            user="admin",
            password="admin"
        )

        # ✅ FIX: avoid commit blocking
        self.conn.autocommit = True
        self.cur = self.conn.cursor()

    def map(self, v):
        try:
            self.cur.execute("""
            INSERT INTO telemetry_processed
            (driver_id, speed, temperature, rain_intensity,
             rolling_avg_speed, lap, event_time,
             anomaly_score, tire_health, lat, lon)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                v["driver_id"],
                v["speed"],
                v["temperature"],
                v["rain_intensity"],
                v["rolling_avg_speed"],
                0,
                int(time.time() * 1000),
                v["anomaly_score"],
                v["tire_health"],
                v.get("lat"),
                v.get("lon")
            ))

        except Exception as e:
            print("🔥 DB ERROR:", e)
            raise e   # ✅ CRITICAL FIX

        return v

stream.map(DB()).print()

env.execute("FINAL PIPELINE WITH DLQ")