# Este archivo valida que el evento recibido tenga los campos correctos, tipos correctos y valores válidos para consumer.py

from typing import Any
import pandas as pd


REQUIRED_FIELDS = [
    "country",
    "year",
    "gdp",
    "social_support",
    "health",
    "freedom",
    "generosity",
    "corruption",
    "actual_happiness_score",
]


def parse_float(value: Any, field_name: str) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        raise ValueError(f"{field_name} must be numeric")

    return result


def parse_int(value: Any, field_name: str) -> int:
    try:
        result = int(value)
    except (TypeError, ValueError):
        raise ValueError(f"{field_name} must be integer")

    return result


def validate_event_schema(event: dict) -> None:
    missing_fields = [
        field for field in REQUIRED_FIELDS
        if field not in event or event[field] is None
    ]

    if missing_fields:
        raise KeyError(f"Missing required fields: {missing_fields}")


def clean_and_validate_event(event: dict) -> dict:
    validate_event_schema(event)

    country = str(event["country"]).strip()

    if country == "":
        raise ValueError("country cannot be empty")

    clean_event = {
        "country": country,
        "year": parse_int(event["year"], "year"),
        "gdp": parse_float(event["gdp"], "gdp"),
        "social_support": parse_float(event["social_support"], "social_support"),
        "health": parse_float(event["health"], "health"),
        "freedom": parse_float(event["freedom"], "freedom"),
        "generosity": parse_float(event["generosity"], "generosity"),
        "corruption": parse_float(event["corruption"], "corruption"),
        "actual_happiness_score": parse_float(
            event["actual_happiness_score"],
            "actual_happiness_score",
        ),
    }

    numeric_fields = [
        "gdp",
        "social_support",
        "health",
        "freedom",
        "generosity",
        "corruption",
        "actual_happiness_score",
    ]

    for field in numeric_fields:
        if clean_event[field] < 0:
            raise ValueError(f"{field} cannot be negative")

    if clean_event["actual_happiness_score"] > 10:
        raise ValueError("actual_happiness_score cannot be greater than 10")

    if clean_event["year"] < 2015 or clean_event["year"] > 2019:
        raise ValueError("year must be between 2015 and 2019")

    return clean_event


def build_feature_dataframe(clean_event: dict, features: list[str]) -> pd.DataFrame:
    missing_model_features = [
        feature for feature in features
        if feature not in clean_event
    ]

    if missing_model_features:
        raise KeyError(f"Missing model features: {missing_model_features}")

    return pd.DataFrame([clean_event])[features]