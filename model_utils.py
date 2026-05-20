#Este archivo carga el modelo y las features para consumer.py

import joblib
from config import MODEL_PATH, FEATURES_PATH


def load_model_and_features():
    if not MODEL_PATH.exists():
        raise FileNotFoundError(f"Model not found: {MODEL_PATH}")

    model = joblib.load(MODEL_PATH)

    if FEATURES_PATH.exists():
        features = joblib.load(FEATURES_PATH)
    else:
        features = [
            "gdp",
            "health",
            "social_support",
            "freedom",
            "corruption",
            "generosity",
        ]

    return model, features