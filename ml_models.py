import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
from xgboost import XGBRegressor
import joblib
import os

# ---------------- PATH ----------------
MODEL_DIR = "/tmp"

# ✅ Ensure directory exists
os.makedirs(MODEL_DIR, exist_ok=True)

ANOMALY_PATH = os.path.join(MODEL_DIR, "anomaly_model.pkl")
TIRE_PATH = os.path.join(MODEL_DIR, "tire_model.pkl")


# ---------------- TRAIN MODELS ----------------
def train_models():

    n = 1000

    data = pd.DataFrame({
        "speed": np.random.uniform(250, 340, n),
        "rpm": np.random.randint(12000, 15000, n),
        "temperature": np.random.uniform(20, 40, n),
        "rain_intensity": np.random.uniform(0, 1, n)
    })

    data["tire_wear"] = (
        0.3 * data["speed"] +
        0.2 * data["temperature"] +
        50 * data["rain_intensity"] +
        np.random.normal(0, 10, n)
    )

    iso = IsolationForest(contamination=0.05, random_state=42)
    iso.fit(data[["speed", "rpm", "temperature", "rain_intensity"]])

    xgb = XGBRegressor(n_estimators=50, max_depth=3)
    xgb.fit(
        data[["speed", "rpm", "temperature", "rain_intensity"]],
        data["tire_wear"]
    )

    joblib.dump(iso, ANOMALY_PATH)
    joblib.dump(xgb, TIRE_PATH)

    print("✅ Models saved in /tmp")


# ---------------- LOAD MODELS ----------------
def load_models():

    if not os.path.exists(ANOMALY_PATH) or not os.path.exists(TIRE_PATH):
        print("⚠️ Models not found. Training now...")
        train_models()

    iso = joblib.load(ANOMALY_PATH)
    xgb = joblib.load(TIRE_PATH)

    return iso, xgb


# ---------------- SAFE FEATURE BUILDER ----------------
def build_features(df):
    """
    Ensures:
    - All required columns exist
    - Correct order
    - No crashes
    """

    return pd.DataFrame({
        "speed": df.get("speed", 300),
        "rpm": 13000,
        "temperature": df.get("temperature", 25.0),
        "rain_intensity": df.get("rain_intensity", 0.0)
    })[["speed", "rpm", "temperature", "rain_intensity"]]


# ---------------- INFERENCE ----------------
def predict(df):

    iso, xgb = load_models()

    if df.empty:
        return df

    try:
        features = build_features(df)

        df["anomaly_score"] = iso.decision_function(features)
        df["tire_health"] = xgb.predict(features)

    except Exception as e:
        print("⚠️ Prediction error:", e)

        # fallback safe values
        df["anomaly_score"] = 0
        df["tire_health"] = 0

    return df


# ---------------- ENTRY ----------------
if __name__ == "__main__":
    train_models()