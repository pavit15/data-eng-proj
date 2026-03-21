CREATE TABLE telemetry_processed (
    id SERIAL PRIMARY KEY,
    driver_id TEXT,
    speed FLOAT,
    temperature FLOAT,
    rain_intensity FLOAT,
    rolling_avg_speed FLOAT,
    lap INTEGER,
    event_time BIGINT,

    -- 🔥 NEW ML FIELDS
    anomaly_score FLOAT,
    tire_health FLOAT,

    -- 🔥 NEW GEO
    lat FLOAT,
    lon FLOAT
);

CREATE INDEX idx_driver ON telemetry_processed(driver_id);
CREATE INDEX idx_event_time ON telemetry_processed(event_time);