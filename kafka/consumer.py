from __future__ import annotations

from pathlib import Path
import sys
import json

from kafka import KafkaConsumer

BASE_DIR = Path(__file__).resolve().parents[1]
sys.path.append(str(BASE_DIR))

from config import TOPIC, BOOTSTRAP_SERVERS, GROUP_ID
from model_utils import load_model_and_features
from validation import clean_and_validate_event, build_feature_dataframe
from database import (
    get_connection,
    store_raw_event,
    sync_dim_raw_event,
    update_raw_status,
    store_prediction,
    prediction_exists_by_country_year,
)


def consume_events() -> None:
    model, features = load_model_and_features()

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )

    print(f"Consumer started. Reading from topic '{TOPIC}'. Press Ctrl+C to stop.")
    print(f"Model features order: {features}")

    processed = 0
    duplicates = 0
    invalid_schema = 0
    invalid_values = 0
    prediction_errors = 0

    conn = get_connection()

    try:
        for msg in consumer:
            raw_event_id = None

            try:
                # Mantener viva la conexión con MySQL
                conn.ping(reconnect=True, attempts=3, delay=2)

                raw_payload = msg.value.decode("utf-8")

                # 1. Guardar evento raw antes de validar
                raw_event_id = store_raw_event(conn, raw_payload)

                # 2. Convertir JSON
                try:
                    event = json.loads(raw_payload)
                except json.JSONDecodeError as exc:
                    invalid_schema += 1

                    update_raw_status(
                        conn,
                        raw_event_id,
                        "INVALID_SCHEMA",
                        f"Invalid JSON: {exc}",
                    )

                    sync_dim_raw_event(conn, raw_event_id)
                    consumer.commit()

                    print(f"Invalid JSON skipped: {exc}")
                    continue

                if not isinstance(event, dict):
                    invalid_schema += 1

                    update_raw_status(
                        conn,
                        raw_event_id,
                        "INVALID_SCHEMA",
                        "Event must be a JSON object",
                    )

                    sync_dim_raw_event(conn, raw_event_id)
                    consumer.commit()

                    print("Invalid event skipped: event is not a JSON object")
                    continue

                # 3. Validar y limpiar evento
                clean_event = clean_and_validate_event(event)

                # 4. Evitar duplicados por país y año
                if prediction_exists_by_country_year(
                    conn,
                    clean_event["country"],
                    clean_event["year"],
                ):
                    duplicates += 1

                    update_raw_status(
                        conn,
                        raw_event_id,
                        "DUPLICATE",
                        "Prediction already exists for this country and year",
                    )

                    sync_dim_raw_event(conn, raw_event_id)
                    consumer.commit()

                    print(
                        f"Duplicate skipped | "
                        f"Country: {clean_event['country']} | "
                        f"Year: {clean_event['year']}"
                    )

                    continue

                # 5. Asegurar orden de features
                X_event = build_feature_dataframe(clean_event, features)

                # 6. Generar predicción
                predicted_score = float(model.predict(X_event)[0])

                # 7. Guardar predicción
                store_prediction(
                    conn,
                    raw_event_id,
                    clean_event,
                    predicted_score,
                )

                # 8. Marcar evento raw como válido
                update_raw_status(conn, raw_event_id, "VALID")

                # 9. Actualizar dimensión raw
                sync_dim_raw_event(conn, raw_event_id)

                processed += 1

                # Confirmar mensaje procesado en Kafka
                consumer.commit()

                print(
                    f"Prediction stored | "
                    f"Country: {clean_event['country']} | "
                    f"Year: {clean_event['year']} | "
                    f"Actual: {clean_event['actual_happiness_score']:.3f} | "
                    f"Predicted: {predicted_score:.3f}"
                )

            except KeyError as exc:
                invalid_schema += 1

                if raw_event_id is not None:
                    update_raw_status(
                        conn,
                        raw_event_id,
                        "INVALID_SCHEMA",
                        str(exc),
                    )

                    sync_dim_raw_event(conn, raw_event_id)

                consumer.commit()

                print(f"Invalid schema skipped: {exc}")

            except ValueError as exc:
                invalid_values += 1

                if raw_event_id is not None:
                    update_raw_status(
                        conn,
                        raw_event_id,
                        "INVALID_VALUES",
                        str(exc),
                    )

                    sync_dim_raw_event(conn, raw_event_id)

                consumer.commit()

                print(f"Invalid values skipped: {exc}")

            except Exception as exc:
                prediction_errors += 1

                if raw_event_id is not None:
                    update_raw_status(
                        conn,
                        raw_event_id,
                        "PREDICTION_ERROR",
                        str(exc),
                    )

                    sync_dim_raw_event(conn, raw_event_id)

                    consumer.commit()

                print(f"Prediction error skipped: {exc}")

    except KeyboardInterrupt:
        print("Consumer stopped by user.")

    finally:
        consumer.close()
        #conn.close()

        print(
            f"Summary → processed: {processed}, "
            f"duplicates: {duplicates}, "
            f"invalid_schema: {invalid_schema}, "
            f"invalid_values: {invalid_values}, "
            f"prediction_errors: {prediction_errors}"
        )


if __name__ == "__main__":
    consume_events()