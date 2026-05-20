"""
Kafka Producer for the World Happiness Streaming ETL project.

This script reads raw CSV files from 2015 to 2019 and publishes each row
as a standardized JSON event into the Kafka topic happiness-predictions.
"""

from pathlib import Path
import json
import time
import pandas as pd
from kafka import KafkaProducer


BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"

TOPIC = "happiness-predictions"
BOOTSTRAP_SERVERS = "localhost:9092"
DELAY_SECONDS = 0.15


RAW_FILES = {
    2015: DATA_DIR / "2015.csv",
    2016: DATA_DIR / "2016.csv",
    2017: DATA_DIR / "2017.csv",
    2018: DATA_DIR / "2018.csv",
    2019: DATA_DIR / "2019.csv",
}


COLUMN_MAP = {
    2015: {
        "country": "Country",
        "gdp": "Economy (GDP per Capita)",
        "social_support": "Family",
        "health": "Health (Life Expectancy)",
        "freedom": "Freedom",
        "generosity": "Generosity",
        "corruption": "Trust (Government Corruption)",
        "actual_happiness_score": "Happiness Score",
    },
    2016: {
        "country": "Country",
        "gdp": "Economy (GDP per Capita)",
        "social_support": "Family",
        "health": "Health (Life Expectancy)",
        "freedom": "Freedom",
        "generosity": "Generosity",
        "corruption": "Trust (Government Corruption)",
        "actual_happiness_score": "Happiness Score",
    },
    2017: {
        "country": "Country",
        "gdp": "Economy..GDP.per.Capita.",
        "social_support": "Family",
        "health": "Health..Life.Expectancy.",
        "freedom": "Freedom",
        "generosity": "Generosity",
        "corruption": "Trust..Government.Corruption.",
        "actual_happiness_score": "Happiness.Score",
    },
    2018: {
        "country": "Country or region",
        "gdp": "GDP per capita",
        "social_support": "Social support",
        "health": "Healthy life expectancy",
        "freedom": "Freedom to make life choices",
        "generosity": "Generosity",
        "corruption": "Perceptions of corruption",
        "actual_happiness_score": "Score",
    },
    2019: {
        "country": "Country or region",
        "gdp": "GDP per capita",
        "social_support": "Social support",
        "health": "Healthy life expectancy",
        "freedom": "Freedom to make life choices",
        "generosity": "Generosity",
        "corruption": "Perceptions of corruption",
        "actual_happiness_score": "Score",
    },
}


def json_serializer(value: dict) -> bytes:
    return json.dumps(value, ensure_ascii=False).encode("utf-8")


def safe_float(value):
    if pd.isna(value):
        return None

    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def safe_country(value):
    if pd.isna(value):
        return None

    country = str(value).strip()

    if country == "":
        return None

    return country


def build_event(row, year: int, column_map: dict) -> dict:
    event = {
        "country": safe_country(row[column_map["country"]]),
        "year": int(year),
        "gdp": safe_float(row[column_map["gdp"]]),
        "social_support": safe_float(row[column_map["social_support"]]),
        "health": safe_float(row[column_map["health"]]),
        "freedom": safe_float(row[column_map["freedom"]]),
        "generosity": safe_float(row[column_map["generosity"]]),
        "corruption": safe_float(row[column_map["corruption"]]),
        "actual_happiness_score": safe_float(row[column_map["actual_happiness_score"]]),

        # Metadata opcional para trazabilidad
        "source_year": int(year),
    }

    return event


def publish_events() -> None:
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=json_serializer,
        key_serializer=lambda key: str(key).encode("utf-8"),
    )

    sent = 0

    for year, csv_path in RAW_FILES.items():
        print(f"Reading raw file: {csv_path}")

        df = pd.read_csv(csv_path)
        df.columns = df.columns.str.strip()

        column_map = COLUMN_MAP[year]

        for _, row in df.iterrows():
            event = build_event(row, year, column_map)

            key = f"{event['country']}-{event['year']}-{sent}"

            producer.send(TOPIC, key=key, value=event)

            sent += 1

            print(f"Sent event {sent}: {event}")

            time.sleep(DELAY_SECONDS)

    producer.flush()
    producer.close()

    print(f"Finished publishing {sent} events to topic '{TOPIC}'.")


if __name__ == "__main__":
    publish_events()