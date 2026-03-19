import json
import psycopg2
import time

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.datastream.functions import MapFunction, CoMapFunction, ProcessWindowFunction
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.common.time import Time


# ---------------- ENV ----------------
env = StreamExecutionEnvironment.get_execution_environment()
env.enable_checkpointing(5000)
env.set_parallelism(1)


# ---------------- KAFKA ----------------
def create_consumer(topic):
    props = {
        "bootstrap.servers": "kafka:29092",
        "group.id": "flink_group_final",
        "auto.offset.reset": "latest"
    }

    return FlinkKafkaConsumer(
        topics=topic,
        deserialization_schema=SimpleStringSchema(),
        properties=props
    )


# ---------------- PARSE ----------------
def parse(x):
    try:
        return json.loads(x)
    except Exception as e:
        print("Parse error:", e)
        return None


# ---------------- STREAMS ----------------
telemetry_stream = env.add_source(create_consumer("telemetry_stream")) \
    .map(parse) \
    .filter(lambda x: x is not None and "driver_id" in x)

weather_stream = env.add_source(create_consumer("weather_stream")) \
    .map(parse) \
    .filter(lambda x: x is not None)


# ---------------- JOIN ----------------
class WeatherJoin(CoMapFunction):

    def __init__(self):
        self.latest_weather = None

    def map1(self, telemetry):
        # only telemetry continues forward
        if self.latest_weather:
            telemetry["temperature"] = self.latest_weather.get("temperature")
            telemetry["rain_intensity"] = self.latest_weather.get("rain_intensity")
        return telemetry

    def map2(self, weather):
        # store weather but DO NOT emit it
        self.latest_weather = weather
        return None


joined_stream = telemetry_stream.connect(weather_stream).map(WeatherJoin()) \
    .filter(lambda x: x is not None and "driver_id" in x)


# ---------------- ROLLING AVG ----------------
class AvgSpeed(MapFunction):

    def __init__(self):
        self.stats = {}

    def map(self, value):
        if "driver_id" not in value:
            return None

        driver = value["driver_id"]
        speed = value["speed"]

        if driver not in self.stats:
            self.stats[driver] = {"total": 0, "count": 0}

        self.stats[driver]["total"] += speed
        self.stats[driver]["count"] += 1

        value["rolling_avg_speed"] = (
            self.stats[driver]["total"] /
            self.stats[driver]["count"]
        )

        return value


result_stream = joined_stream.map(AvgSpeed()) \
    .filter(lambda x: x is not None)


# ---------------- WINDOW ----------------
class WindowAvg(ProcessWindowFunction):

    def process(self, key, context, elements):
        elements = list(elements)

        total = sum(e["speed"] for e in elements)
        count = len(elements)

        avg = total / count if count > 0 else 0

        for e in elements:
            e["window_avg_speed"] = avg
            yield e


windowed_stream = result_stream \
    .key_by(lambda x: x["driver_id"]) \
    .window(TumblingProcessingTimeWindows.of(Time.seconds(5))) \
    .process(WindowAvg())


# ---------------- POSTGRES ----------------
class PostgresWriter(MapFunction):

    def open(self, runtime_context):
        self.conn = psycopg2.connect(
            host="postgres",
            database="telemetry",
            user="admin",
            password="admin"
        )
        self.cursor = self.conn.cursor()
        print("✅ Connected to Postgres")

    def map(self, value):
        try:
            self.cursor.execute(
                """
                INSERT INTO telemetry_processed
                (driver_id, speed, temperature, rain_intensity,
                 rolling_avg_speed, lap, event_time)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    value.get("driver_id"),
                    value.get("speed"),
                    value.get("temperature"),
                    value.get("rain_intensity"),
                    value.get("rolling_avg_speed"),
                    0,
                    int(time.time() * 1000)
                )
            )
            self.conn.commit()

        except Exception as e:
            print("❌ Insert error:", e)

        return value


# ---------------- EXECUTE ----------------
windowed_stream.map(PostgresWriter()).print()

env.execute("FINAL FIXED Pipeline")