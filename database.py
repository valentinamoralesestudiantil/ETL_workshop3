from datetime import datetime
import json
import mysql.connector


MYSQL_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "valemoravale",
    "database": "happiness_dw",
    "port": 3306,
}


def get_connection():
    return mysql.connector.connect(**MYSQL_CONFIG)


def now_mysql() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def store_raw_event(conn, raw_payload: str) -> int:
    cursor = conn.cursor()

    cursor.execute(
        """
        INSERT INTO raw_happiness_events (
            event_json,
            processing_status,
            error_message,
            ingestion_time
        )
        VALUES (%s, %s, %s, %s)
        """,
        (
            raw_payload,
            "RECEIVED",
            None,
            now_mysql(),
        ),
    )

    conn.commit()
    return cursor.lastrowid


def update_raw_status(
    conn,
    raw_event_id: int,
    status: str,
    error_message: str | None = None,
) -> None:
    cursor = conn.cursor()

    cursor.execute(
        """
        UPDATE raw_happiness_events
        SET processing_status = %s,
            error_message = %s
        WHERE raw_event_id = %s
        """,
        (
            status,
            error_message,
            raw_event_id,
        ),
    )

    conn.commit()


def get_or_create_country(conn, country_name: str) -> int:
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT country_id
        FROM dim_country
        WHERE country_name = %s
        """,
        (country_name,),
    )

    row = cursor.fetchone()

    if row:
        return row[0]

    cursor.execute(
        """
        INSERT INTO dim_country (country_name)
        VALUES (%s)
        """,
        (country_name,),
    )

    conn.commit()
    return cursor.lastrowid


def get_or_create_date(conn, year: int) -> int:
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT date_id
        FROM dim_date
        WHERE year = %s
        """,
        (year,),
    )

    row = cursor.fetchone()

    if row:
        return row[0]

    cursor.execute(
        """
        INSERT INTO dim_date (year)
        VALUES (%s)
        """,
        (year,),
    )

    conn.commit()
    return cursor.lastrowid


def get_raw_ingestion_time(conn, raw_event_id: int) -> str:
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT ingestion_time
        FROM raw_happiness_events
        WHERE raw_event_id = %s
        """,
        (raw_event_id,),
    )

    row = cursor.fetchone()

    if row:
        return row[0]

    return now_mysql()


def sync_dim_raw_event(conn, raw_event_id: int) -> None:
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT processing_status, ingestion_time
        FROM raw_happiness_events
        WHERE raw_event_id = %s
        """,
        (raw_event_id,),
    )

    row = cursor.fetchone()

    if not row:
        return

    processing_status, ingestion_time = row

    cursor.execute(
        """
        INSERT INTO dim_raw_event (
            raw_event_id,
            processing_status,
            ingestion_time,
            event_source,
            topic_name
        )
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            processing_status = VALUES(processing_status),
            ingestion_time = VALUES(ingestion_time)
        """,
        (
            raw_event_id,
            processing_status,
            ingestion_time,
            "Kafka",
            "happiness-predictions",
        ),
    )

    conn.commit()


def store_prediction(
    conn,
    raw_event_id: int,
    clean_event: dict,
    predicted_score: float,
) -> None:
    actual_score = clean_event["actual_happiness_score"]
    prediction_error = abs(actual_score - predicted_score)

    country_id = get_or_create_country(conn, clean_event["country"])
    date_id = get_or_create_date(conn, clean_event["year"])
    event_timestamp = get_raw_ingestion_time(conn, raw_event_id)

    cursor = conn.cursor()

    cursor.execute(
        """
        INSERT INTO fact_predictions (
            raw_event_id,
            country_id,
            date_id,
            actual_score,
            predicted_score,
            prediction_error,
            event_timestamp,
            prediction_timestamp
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            prediction_id = prediction_id
        """,
        (
            raw_event_id,
            country_id,
            date_id,
            actual_score,
            predicted_score,
            prediction_error,
            event_timestamp,
            now_mysql(),
        ),
    )

    conn.commit()
    
def prediction_exists_by_country_year(conn, country_name: str, year: int) -> bool:
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT 1
        FROM fact_predictions fp
        JOIN dim_country dc ON fp.country_id = dc.country_id
        JOIN dim_date dd ON fp.date_id = dd.date_id
        WHERE dc.country_name = %s
          AND dd.year = %s
        LIMIT 1
        """,
        (country_name, year),
    )

    return cursor.fetchone() is not None

    conn.commit()