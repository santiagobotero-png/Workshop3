
"""
Kafka consumer module.

Consumes Happiness events, validates their contract,
deduplicates business events, performs model inference,
and persists processing results in PostgreSQL.
"""

import logging
import time

from pathlib import Path

import joblib
import pandas as pd

from kafka import KafkaConsumer

from src.streaming.database import (
    get_connection,
    insert_raw_event,
    update_raw_event_status,
    mark_raw_event_duplicate,
    claim_event,
    insert_prediction,
    complete_event_processing,
)

from src.streaming.event_contract import (
    deserialize_event,
    validate_event,
    EventValidationError,
    MODEL_FEATURES,
    TARGET_FIELD,
)


# ============================================================
# 1. CONFIGURATION
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

MODEL_PATH = PROJECT_ROOT / "models" / "model.pkl"

KAFKA_BOOTSTRAP_SERVERS = ["localhost:9092"]

KAFKA_TOPIC = "happiness-events"

KAFKA_GROUP_ID = "happiness-ml-team"

MODEL_VERSION = "random_forest_v1"

MAX_PREDICTION_ATTEMPTS = 3

RETRY_DELAY_SECONDS = 1


# ============================================================
# 2. LOGGING CONFIGURATION
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format=(
        "%(asctime)s | %(levelname)s | "
        "%(name)s | %(message)s"
    ),
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger(__name__)


# ============================================================
# 3. MODEL LOADING
# ============================================================

def load_model():
    """
    Load the trained Random Forest pipeline.

    The model is loaded once when the consumer starts.
    """

    if not MODEL_PATH.exists():
        raise FileNotFoundError(
            f"Model artifact not found: {MODEL_PATH}"
        )

    model = joblib.load(MODEL_PATH)

    logger.info("Model loaded successfully.")
    logger.info("Model version: %s", MODEL_VERSION)

    return model


# ============================================================
# 4. KAFKA CONSUMER
# ============================================================

def create_consumer():
    """
    Create and configure the Kafka consumer.

    Automatic offset commits are disabled.
    Each message is processed sequentially.
    """

    consumer = KafkaConsumer(
        KAFKA_TOPIC,

        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,

        group_id=KAFKA_GROUP_ID,

        enable_auto_commit=False,

        auto_offset_reset="earliest",

        max_poll_records=1,

        value_deserializer=lambda value: value,

        key_deserializer=lambda key: key,
    )

    logger.info(
        "Kafka consumer connected to topic: %s",
        KAFKA_TOPIC,
    )

    logger.info("Consumer group: %s", KAFKA_GROUP_ID)

    return consumer


# ============================================================
# 5. RAW MESSAGE PERSISTENCE
# ============================================================

def persist_raw_message(connection, message):
    """
    Persist the original Kafka message before processing.

    The original payload is stored as bytes.

    Returns:
        tuple:
            raw_event_id
            inserted
            is_terminal
    """

    event = None
    event_id = None

    # Attempt to extract event_id without requiring
    # the event to pass the complete contract.
    try:
        event = deserialize_event(message.value)

        if isinstance(event.get("event_id"), str):
            candidate = event["event_id"].strip()

            if candidate:
                event_id = candidate

    except EventValidationError:
        pass

    try:
        raw_event_id, inserted = insert_raw_event(
            connection=connection,
            kafka_topic=message.topic,
            kafka_partition=message.partition,
            kafka_offset=message.offset,
            kafka_key=message.key,
            raw_payload=message.value,
            event_id=event_id,
        )

        connection.commit()

    except Exception:
        connection.rollback()
        raise

    # If the physical Kafka record already exists,
    # inspect its persisted processing state.
    if not inserted:

        try:
            with connection.cursor() as cursor:

                cursor.execute(
                    """
                    SELECT status, is_duplicate
                    FROM public.raw_events
                    WHERE raw_event_id = %s;
                    """,
                    (raw_event_id,),
                )

                result = cursor.fetchone()

            connection.commit()

        except Exception:
            connection.rollback()
            raise

        if result is None:
            raise RuntimeError(
                f"Raw event {raw_event_id} could not be retrieved."
            )

        status, is_duplicate = result

        terminal_statuses = {
            "VALID",
            "INVALID_SCHEMA",
            "INVALID_VALUES",
            "PREDICTION_ERROR",
        }

        is_terminal = (
            is_duplicate
            or status in terminal_statuses
        )

        if is_terminal:
            logger.info(
                "Kafka record already processed. "
                "raw_event_id=%s status=%s duplicate=%s",
                raw_event_id,
                status,
                is_duplicate,
            )

            return raw_event_id, False, True

    return raw_event_id, inserted, False


# ============================================================
# 6. MESSAGE VALIDATION
# ============================================================

def validate_message(raw_payload):
    """
    Deserialize and validate a Kafka message.

    Returns:
        dict: Validated event.
    """

    event = deserialize_event(raw_payload)

    result = validate_event(event)

    if not result["valid"]:

        raise EventValidationError(
            result["status"],
            result["errors"],
        )

    return event


# ============================================================
# 7. MODEL INFERENCE
# ============================================================

def predict_event(model, event):
    """
    Generate a happiness score prediction.

    Only MODEL_FEATURES are passed to the model.
    """

    features = pd.DataFrame(
        [
            {
                feature: event[feature]
                for feature in MODEL_FEATURES
            }
        ],
        columns=MODEL_FEATURES,
    )

    prediction = model.predict(features)

    predicted_score = float(prediction[0])

    return predicted_score


# ============================================================
# 8. RETRY MECHANISM
# ============================================================

def predict_with_retries(model, event):
    """
    Execute model inference with a maximum number
    of attempts.

    Returns:
        tuple:
            predicted_score
            error_message
    """

    last_error = None

    for attempt in range(1, MAX_PREDICTION_ATTEMPTS + 1):

        try:

            predicted_score = predict_event(model, event)

            logger.info(
                "Prediction successful on attempt %s.",
                attempt,
            )

            return predicted_score, None

        except Exception as exc:

            last_error = exc

            logger.warning(
                "Prediction attempt %s/%s failed: %s",
                attempt,
                MAX_PREDICTION_ATTEMPTS,
                exc,
            )

            if attempt < MAX_PREDICTION_ATTEMPTS:
                time.sleep(RETRY_DELAY_SECONDS)

    return None, str(last_error)


# ============================================================
# 9. INVALID MESSAGE HANDLING
# ============================================================

def handle_invalid_message(
    connection,
    raw_event_id,
    validation_error,
):
    """
    Persist the validation failure.

    The original payload remains available in raw_events.
    """

    try:

        update_raw_event_status(
            connection=connection,
            raw_event_id=raw_event_id,
            status=validation_error.status,
            error_message=str(validation_error),
        )

        connection.commit()

        logger.warning(
            "Invalid message persisted. "
            "raw_event_id=%s status=%s error=%s",
            raw_event_id,
            validation_error.status,
            validation_error,
        )

    except Exception:
        connection.rollback()
        raise


# ============================================================
# 10. EVENT PROCESSING
# ============================================================

def process_message(
    connection,
    model,
    message,
):
    """
    Process one Kafka message.

    Processing sequence:

        1. Persist raw message.
        2. Validate event contract.
        3. Claim business event.
        4. Execute model inference.
        5. Persist prediction or prediction error.
        6. Commit PostgreSQL transaction.

    Returns:
        bool:
            True if processing completed successfully.
    """

    logger.info(
        "Received Kafka message: topic=%s partition=%s offset=%s",
        message.topic,
        message.partition,
        message.offset,
    )

    # --------------------------------------------------------
    # Step 1: Persist original message
    # --------------------------------------------------------

    raw_event_id, inserted, is_terminal = persist_raw_message(
        connection,
        message,
    )

    if is_terminal:
        return True

    if inserted:
        logger.info(
            "Raw event persisted. raw_event_id=%s",
            raw_event_id,
        )

    # --------------------------------------------------------
    # Step 2: Deserialize and validate
    # --------------------------------------------------------

    try:

        event = validate_message(message.value)

    except EventValidationError as exc:

        handle_invalid_message(
            connection,
            raw_event_id,
            exc,
        )

        return True

    event_id = event["event_id"]

    logger.info(
        "Event contract validated. event_id=%s",
        event_id,
    )

    # --------------------------------------------------------
    # Step 3: Claim event and process transactionally
    # --------------------------------------------------------

    try:

        claimed = claim_event(
            connection=connection,
            event_id=event["event_id"],
            raw_event_id=raw_event_id,
            model_version=MODEL_VERSION,
        )

        if not claimed:

            update_raw_event_status(
                connection=connection,
                raw_event_id=raw_event_id,
                status="VALID",
            )

            connection.commit()

            logger.info(
                "Duplicate event skipped: %s",
                event["event_id"],
            )

            return True

        # ----------------------------------------------------
        # Step 5: Model inference with retries
        # ----------------------------------------------------

        predicted_score, prediction_error = predict_with_retries(
            model,
            event,
        )

        # ----------------------------------------------------
        # Step 6A: Prediction failure
        # ----------------------------------------------------

        if prediction_error is not None:

            complete_event_processing(
                connection=connection,
                event_id=event_id,
                raw_event_id=raw_event_id,
                status="PREDICTION_ERROR",
                error_message=prediction_error,
            )

            connection.commit()

            logger.error(
                "Prediction failed after all attempts. "
                "event_id=%s",
                event_id,
            )

            return True

        # ----------------------------------------------------
        # Step 6B: Persist successful prediction
        # ----------------------------------------------------

        actual_score = float(event[TARGET_FIELD])

        prediction_id = insert_prediction(
            connection=connection,
            event_id=event_id,
            predicted_score=predicted_score,
            actual_score=actual_score,
            model_version=MODEL_VERSION,
        )

        complete_event_processing(
            connection=connection,
            event_id=event_id,
            raw_event_id=raw_event_id,
            status="PREDICTED",
        )

        connection.commit()

        logger.info(
            "Event successfully processed. "
            "event_id=%s prediction_id=%s "
            "predicted_score=%.4f actual_score=%.4f",
            event_id,
            prediction_id,
            predicted_score,
            actual_score,
        )

        return True

    except Exception:

        connection.rollback()

        logger.exception(
            "Database or processing transaction failed. "
            "event_id=%s raw_event_id=%s",
            event_id,
            raw_event_id,
        )

        raise


# ============================================================
# 11. KAFKA OFFSET COMMIT
# ============================================================

def commit_message(consumer, message):
    """
    Commit the Kafka offset after PostgreSQL persistence.

    max_poll_records=1 ensures that the consumer processes
    one record at a time before committing.
    """

    consumer.commit()

    logger.info(
        "Kafka offset committed: partition=%s offset=%s",
        message.partition,
        message.offset,
    )


# ============================================================
# 12. CONSUMER EXECUTION
# ============================================================

def run_consumer():
    """
    Start the streaming consumer.

    The model is loaded once and the database connection
    is reused during message processing.
    """

    model = load_model()

    connection = get_connection()

    consumer = None

    try:

        consumer = create_consumer()

        logger.info("Streaming consumer started.")

        for message in consumer:

            # PostgreSQL must complete processing before
            # the corresponding Kafka offset is committed.
            process_message(
                connection=connection,
                model=model,
                message=message,
            )

            commit_message(
                consumer,
                message,
            )

    finally:

        if consumer is not None:
            consumer.close()

        if connection is not None:
            connection.close()

        logger.info("Streaming consumer stopped.")


# ============================================================
# 13. MAIN
# ============================================================

def main():

    try:
        run_consumer()

    except KeyboardInterrupt:

        logger.info(
            "Consumer interrupted by user."
        )


if __name__ == "__main__":
    main()
