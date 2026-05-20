#Este archivo guarda rutas y configuraciones generales para consumer.py

from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

DB_DIR = BASE_DIR / "data" / "streaming"
DB_DIR.mkdir(parents=True, exist_ok=True)

DB_PATH = DB_DIR / "streaming_happiness.db"

MODEL_PATH = BASE_DIR / "models" / "model.pkl"
FEATURES_PATH = BASE_DIR / "models" / "features.pkl"

TOPIC = "happiness-predictions"
BOOTSTRAP_SERVERS = "localhost:9092"
GROUP_ID = "happiness-inference-consumer-v4"
#GROUP_ID = "happiness-inference-consumer"