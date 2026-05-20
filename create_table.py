from pathlib import Path
import sqlite3

BASE_DIR = Path(__file__).resolve().parents[1]

DB_DIR = BASE_DIR / "data" / "streaming"
DB_DIR.mkdir(parents=True, exist_ok=True)

DB_PATH = DB_DIR / "streaming_happiness.db"


def create_tables():
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()

        cursor.execute("PRAGMA foreign_keys = ON;")

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS raw_happiness_events (
                raw_event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_json TEXT NOT NULL,
                processing_status TEXT NOT NULL,
                error_message TEXT,
                ingestion_time TEXT NOT NULL
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dim_country (
                country_id INTEGER PRIMARY KEY AUTOINCREMENT,
                country_name TEXT NOT NULL UNIQUE
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dim_date (
                date_id INTEGER PRIMARY KEY AUTOINCREMENT,
                year INTEGER NOT NULL UNIQUE
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dim_raw_event (
                raw_event_dim_id INTEGER PRIMARY KEY AUTOINCREMENT,
                raw_event_id INTEGER NOT NULL UNIQUE,
                processing_status TEXT NOT NULL,
                ingestion_time TEXT NOT NULL,
                event_source TEXT DEFAULT 'Kafka',
                topic_name TEXT DEFAULT 'happiness-predictions',
                FOREIGN KEY (raw_event_id) REFERENCES raw_happiness_events(raw_event_id)
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS fact_predictions (
                prediction_id INTEGER PRIMARY KEY AUTOINCREMENT,
                raw_event_id INTEGER NOT NULL,
                country_id INTEGER NOT NULL,
                date_id INTEGER NOT NULL,
                actual_score REAL NOT NULL,
                predicted_score REAL NOT NULL,
                prediction_error REAL NOT NULL,
                event_timestamp TEXT NOT NULL,
                prediction_timestamp TEXT NOT NULL,
                FOREIGN KEY (raw_event_id) REFERENCES raw_happiness_events(raw_event_id),
                FOREIGN KEY (country_id) REFERENCES dim_country(country_id),
                FOREIGN KEY (date_id) REFERENCES dim_date(date_id)
            )
        """)

        conn.commit()

    print("Database and tables created successfully.")
    print(f"Database path: {DB_PATH}")


if __name__ == "__main__":
    create_tables()