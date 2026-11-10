
"""
Database connection module.

Handles PostgreSQL connectivity for the Happiness Streaming ETL pipeline.
"""
import math
import os
from pathlib import Path

import psycopg2
from dotenv import load_dotenv


# ---------------------------------------------------------
# ENVIRONMENT CONFIGURATION
# ---------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]
ENV_FILE = PROJECT_ROOT / ".env"

load_dotenv(dotenv_path=ENV_FILE)


# ---------------------------------------------------------
# DATABASE CONFIGURATION
# ---------------------------------------------------------

def get_db_config() -> dict:
    """
    Load and validate PostgreSQL configuration.

    Returns:
        dict: PostgreSQL connection parameters.
    """

    required_variables = [
        "DB_HOST",
        "DB_PORT",
        "DB_NAME",
        "DB_USER",
        "DB_PASSWORD",
    ]

    missing_variables = [
        variable
        for variable in required_variables
        if not os.getenv(variable)
    ]

    if missing_variables:
        raise RuntimeError(
            "Missing database environment variables: "
            + ", ".join(missing_variables)
        )

    try:
        port = int(os.getenv("DB_PORT"))
    except ValueError as exc:
        raise ValueError(
            "DB_PORT must be a valid integer."
        ) from exc

    return {
        "host": os.getenv("DB_HOST"),
        "port": port,
        "dbname": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "connect_timeout": 5,
        "application_name": "happiness_streaming_etl",
    }


# ---------------------------------------------------------
# DATABASE CONNECTION
# ---------------------------------------------------------

def get_connection():
    """
    Establish a connection to PostgreSQL.

    Returns:
        psycopg2.extensions.connection:
            An active PostgreSQL connection.
    """

    config = get_db_config()

    connection = psycopg2.connect(**config)

    return connection


# ---------------------------------------------------------
# RAW EVENTS PERSISTENCE
# ---------------------------------------------------------

def insert_raw_event(
    connection,
    kafka_topic: str,
    kafka_partition: int,
    kafka_offset: int,
    kafka_key: bytes | str | None,
    raw_payload: bytes,
    event_id: str | None = None,
) -> tuple[int, bool]:
    """
    Persist a raw Kafka message in PostgreSQL.

    Kafka record identity is defined by:
        topic + partition + offset.

    If the same Kafka record is received again, the existing
    raw_event_id is returned instead of inserting a new row.

    Returns:
        tuple[int, bool]:
            - raw_event_id
            - True if inserted, False if already existed

    Transaction management is delegated to the caller.
    """

    if not kafka_topic:
        raise ValueError("kafka_topic cannot be empty.")

    if kafka_partition < 0:
        raise ValueError("kafka_partition cannot be negative.")

    if kafka_offset < 0:
        raise ValueError("kafka_offset cannot be negative.")

    if not isinstance(raw_payload, bytes):
        raise TypeError("raw_payload must be bytes.")

    if isinstance(kafka_key, bytes):
        kafka_key = kafka_key.decode("utf-8")

    query = """
        INSERT INTO public.raw_events (
            kafka_topic,
            kafka_partition,
            kafka_offset,
            kafka_key,
            raw_payload,
            event_id
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (
            kafka_topic,
            kafka_partition,
            kafka_offset
        )
        DO NOTHING
        RETURNING raw_event_id;
    """

    with connection.cursor() as cursor:

        cursor.execute(
            query,
            (
                kafka_topic,
                kafka_partition,
                kafka_offset,
                kafka_key,
                raw_payload,
                event_id,
            ),
        )

        result = cursor.fetchone()

        if result is not None:
            return result[0], True

        # The Kafka record already exists.
        cursor.execute(
            """
            SELECT raw_event_id
            FROM public.raw_events
            WHERE kafka_topic = %s
              AND kafka_partition = %s
              AND kafka_offset = %s;
            """,
            (
                kafka_topic,
                kafka_partition,
                kafka_offset,
            ),
        )

        existing_record = cursor.fetchone()

        if existing_record is None:
            raise RuntimeError(
                "Kafka record conflict detected, "
                "but the existing raw event could not be retrieved."
            )

        return existing_record[0], False


# ---------------------------------------------------------
# RAW EVENT STATUS MANAGEMENT
# ---------------------------------------------------------

VALID_RAW_EVENT_STATUSES = {
    "RECEIVED",
    "VALID",
    "INVALID_SCHEMA",
    "INVALID_VALUES",
    "PREDICTION_ERROR",
}


def update_raw_event_status(
    connection,
    raw_event_id: int,
    status: str,
    error_message: str | None = None,
) -> None:
    """
    Update the processing status of a raw Kafka event.

    Transaction management is delegated to the caller.
    """

    if status not in VALID_RAW_EVENT_STATUSES:
        raise ValueError(
            f"Invalid raw event status: {status}"
        )

    query = """
        UPDATE public.raw_events
        SET
            status = %s,
            error_message = %s
        WHERE raw_event_id = %s
        RETURNING raw_event_id;
    """

    with connection.cursor() as cursor:

        cursor.execute(
            query,
            (
                status,
                error_message,
                raw_event_id,
            ),
        )

        result = cursor.fetchone()

        if result is None:
            raise LookupError(
                f"Raw event {raw_event_id} does not exist."
            )


def mark_raw_event_duplicate(
    connection,
    raw_event_id: int,
) -> None:
    """
    Mark a raw Kafka event as a business-event duplicate.

    The processing status remains independent of this flag.
    Transaction management is delegated to the caller.
    """

    query = """
        UPDATE public.raw_events
        SET is_duplicate = TRUE
        WHERE raw_event_id = %s
        RETURNING raw_event_id;
    """

    with connection.cursor() as cursor:

        cursor.execute(query, (raw_event_id,))

        result = cursor.fetchone()

        if result is None:
            raise LookupError(
                f"Raw event {raw_event_id} does not exist."
            )

# ---------------------------------------------------------
# PROCESSED EVENTS MANAGEMENT
# ---------------------------------------------------------

def claim_event(
    connection,
    event_id: str,
    raw_event_id: int,
    model_version: str,
) -> bool:
    """
    Atomically claim a business event for processing.

    Returns:
        bool:
            True  -> event successfully claimed.
            False -> event_id already exists; raw event marked
                     as duplicate.

    Transaction management is delegated to the caller.
    """

    if not event_id or not event_id.strip():
        raise ValueError("event_id cannot be empty.")

    if not model_version or not model_version.strip():
        raise ValueError("model_version cannot be empty.")

    query = """
        INSERT INTO public.processed_events (
            event_id,
            raw_event_id,
            processing_status,
            model_version
        )
        VALUES (%s, %s, 'PROCESSING', %s)
        ON CONFLICT (event_id)
        DO NOTHING
        RETURNING event_id;
    """

    with connection.cursor() as cursor:

        cursor.execute(
            query,
            (
                event_id,
                raw_event_id,
                model_version,
            ),
        )

        result = cursor.fetchone()

        if result is not None:
            return True

    # Another raw event already claimed this business event.
    mark_raw_event_duplicate(connection, raw_event_id)

    return False


# ---------------------------------------------------------
# PREDICTIONS PERSISTENCE
# ---------------------------------------------------------

def insert_prediction(
    connection,
    event_id: str,
    predicted_score: float,
    actual_score: float,
    model_version: str,
) -> int:
    """
    Persist a model prediction and its actual value.

    The absolute error is calculated automatically by PostgreSQL.

    Transaction management is delegated to the caller.

    Returns:
        int: Generated prediction_id.
    """

    if not event_id:
        raise ValueError("event_id cannot be empty.")

    if not model_version or not model_version.strip():
        raise ValueError("model_version cannot be empty.")

    if not math.isfinite(predicted_score):
        raise ValueError("predicted_score must be finite.")

    if not math.isfinite(actual_score):
        raise ValueError("actual_score must be finite.")

    if not 0 <= predicted_score <= 10:
        raise ValueError("predicted_score must be between 0 and 10.")

    if not 0 <= actual_score <= 10:
        raise ValueError("actual_score must be between 0 and 10.")

    query = """
        INSERT INTO public.predictions (
            event_id,
            predicted_score,
            actual_score,
            model_version
        )
        VALUES (%s, %s, %s, %s)
        RETURNING prediction_id;
    """

    with connection.cursor() as cursor:

        cursor.execute(
            query,
            (
                event_id,
                predicted_score,
                actual_score,
                model_version,
            ),
        )

        result = cursor.fetchone()

        if result is None:
            raise RuntimeError(
                "Prediction insertion did not return an ID."
            )

        return result[0]


# ---------------------------------------------------------
# PROCESSED EVENT COMPLETION
# ---------------------------------------------------------

def complete_event_processing(
    connection,
    event_id: str,
    raw_event_id: int,
    status: str = "PREDICTED",
    error_message: str | None = None,
) -> None:
    """
    Complete the processing lifecycle of an event.

    Allowed final statuses:
        - PREDICTED
        - PREDICTION_ERROR

    Updates processed_events and raw_events within the
    caller's current transaction.

    Transaction management is delegated to the caller.
    """

    allowed_statuses = {
        "PREDICTED",
        "PREDICTION_ERROR",
    }

    if status not in allowed_statuses:
        raise ValueError(
            f"Invalid final processing status: {status}"
        )

    if status == "PREDICTED" and error_message is not None:
        raise ValueError(
            "A successfully predicted event cannot have an error message."
        )

    query = """
        UPDATE public.processed_events
        SET
            processing_status = %s,
            completed_at = NOW()
        WHERE event_id = %s
          AND raw_event_id = %s
          AND processing_status = 'PROCESSING'
        RETURNING event_id;
    """

    with connection.cursor() as cursor:

        cursor.execute(
            query,
            (
                status,
                event_id,
                raw_event_id,
            ),
        )

        result = cursor.fetchone()

        if result is None:
            raise LookupError(
                f"Event {event_id} was not found in PROCESSING status "
                "with the specified raw_event_id."
            )

    raw_status = (
        "VALID"
        if status == "PREDICTED"
        else "PREDICTION_ERROR"
    )

    update_raw_event_status(
        connection,
        raw_event_id,
        raw_status,
        error_message,
    )