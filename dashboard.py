import streamlit as st
import psycopg2
import pandas as pd
import time

from ml_models import predict

st.set_page_config(page_title="Motorsports Dashboard", layout="wide")

st.title("🏎️ Real-Time Motorsports Dashboard")

# ---------------- DB CONNECTION ----------------
@st.cache_resource
def get_connection():
    return psycopg2.connect(
        host="postgres",
        database="telemetry",
        user="admin",
        password="admin"
    )

conn = get_connection()

# ---------------- FETCH DATA ----------------
def load_data():
    query = """
    SELECT *
    FROM telemetry_processed
    ORDER BY id DESC
    LIMIT 500
    """
    df = pd.read_sql(query, conn)

    # fallback ML
    if not df.empty:
        try:
            df = predict(df)
        except:
            pass

    return df

# ---------------- SIDEBAR ----------------
refresh = st.sidebar.slider("Refresh (seconds)", 1, 10, 2)

driver_filter = st.sidebar.multiselect(
    "Select Drivers",
    ["HAM", "VER", "LEC"],
    default=["HAM", "VER", "LEC"]
)

replay_index = st.sidebar.slider("Replay", 10, 500, 200)

placeholder = st.empty()

while True:
    df = load_data()

    if not df.empty:
        df = df[df["driver_id"].isin(driver_filter)]
        df = df.head(replay_index)

    with placeholder.container():

        # ---------------- TABLE ----------------
        st.subheader("📊 Latest Data")
        st.dataframe(df.head(20))

        # ---------------- HEATMAP ----------------
        st.subheader("🗺️ Track Heatmap")

        if not df.empty:
            if "lat" in df and "lon" in df:
                st.map(df[["lat", "lon"]])
            else:
                st.map(pd.DataFrame({
                    "lat": [19.076]*len(df),
                    "lon": [72.877]*len(df)
                }))

        # ---------------- SPEED ----------------
        st.subheader("📈 Speed by Driver")

        if not df.empty:
            chart_df = df.sort_values("id")

            st.line_chart(
                chart_df.pivot_table(
                    index="id",
                    columns="driver_id",
                    values="speed"
                )
            )

        # ---------------- ROLLING AVG ----------------
        st.subheader("⚡ Rolling Average Speed")

        if not df.empty:
            st.line_chart(
                chart_df.pivot_table(
                    index="id",
                    columns="driver_id",
                    values="rolling_avg_speed"
                )
            )

        # ---------------- ANOMALY ----------------
        st.subheader("🚨 Anomaly Detection")

        if not df.empty and "anomaly_score" in df:

            st.line_chart(
                df.pivot_table(
                    index="id",
                    columns="driver_id",
                    values="anomaly_score"
                )
            )

            anomalies = df[df["anomaly_score"] < -0.05]

            if not anomalies.empty:
                st.error("🚨 Anomaly detected!")
                st.dataframe(anomalies.head(10))
            else:
                st.write("No anomalies detected")

        # ---------------- TIRE HEALTH ----------------
        st.subheader("🛞 Tire Health Prediction")

        if not df.empty and "tire_health" in df:
            latest = df.iloc[0]

            st.metric(
                "Predicted Tire Wear",
                f"{latest['tire_health']:.2f}"
            )

        # ---------------- WEATHER ----------------
        st.subheader("🌧️ Weather")

        if not df.empty:
            latest = df.iloc[0]

            col1, col2 = st.columns(2)
            col1.metric("Temperature", f"{latest['temperature']:.2f} °C")
            col2.metric("Rain Intensity", f"{latest['rain_intensity']:.2f}")

    time.sleep(refresh)