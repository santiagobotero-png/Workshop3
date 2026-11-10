
import os
import re
import time
import unicodedata

import pandas as pd

from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import KafkaError

from .event_contract import (
    serialize_event,
    get_event_key,
    EventValidationError
)


# ============================================================
# 1. CONFIGURACIÓN
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

DATASET_PATH = Path(
    os.getenv(
        "HAPPINESS_DATASET_PATH",
        str(PROJECT_ROOT / "data/processed/world_happiness_2015_2019.csv")
    )
)

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092"
).split(",")

KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "happiness-events"
)

STREAM_DELAY_SECONDS = float(
    os.getenv("STREAM_DELAY_SECONDS", "0.5")
)

ACK_TIMEOUT_SECONDS = 30


# ============================================================
# 2. GENERACIÓN DEL IDENTIFICADOR
# ============================================================

def generate_event_id(year, country):
    """
    Genera un identificador determinista utilizando
    el año y el nombre normalizado del país.

    Ejemplo:
        HAPPINESS-2019-COLOMBIA
    """

    normalized = unicodedata.normalize("NFKD", country)

    normalized = normalized.encode(
        "ascii",
        "ignore"
    ).decode("ascii")

    normalized = re.sub(
        r"[^a-zA-Z0-9]+",
        "-",
        normalized
    ).strip("-").upper()

    if not normalized:
        raise ValueError(
            f"No se pudo generar un identificador para: {country}"
        )

    return f"HAPPINESS-{year}-{normalized}"


# ============================================================
# 3. CONSTRUCCIÓN DEL EVENTO
# ============================================================

def build_event(row):
    """
    Convierte una fila del dataset en un evento JSON
    compatible con event_contract.py.
    """

    country = str(row["country"])

    if pd.isna(row["country"]) or not country.strip():
        raise ValueError("El país no puede estar vacío.")

    if pd.isna(row["year"]):
        raise ValueError("El año no puede estar vacío.")

    year = int(row["year"])

    # Los valores numéricos se convierten a tipos nativos.
    # corruption admite None según el contrato.

    corruption = (
        None
        if pd.isna(row["corruption"])
        else float(row["corruption"])
    )

    event = {
        "event_id": generate_event_id(year, country),
        "country": country,
        "year": year,

        "gdp": float(row["gdp"]),
        "social_support": float(row["social_support"]),
        "health": float(row["health"]),
        "freedom": float(row["freedom"]),
        "corruption": corruption,
        "generosity": float(row["generosity"]),

        # Variable objetivo: se conserva para evaluación,
        # pero no se utilizará como predictor del modelo.
        "actual_happiness_score": float(
            row["happiness_score"]
        ),

        # Momento de generación/publicación del evento.
        "event_time": datetime.now(
            timezone.utc
        ).isoformat(
            timespec="seconds"
        ).replace("+00:00", "Z")
    }

    return event


# ============================================================
# 4. CREACIÓN DEL PRODUCTOR KAFKA
# ============================================================

def create_kafka_producer():
    """
    Configura y devuelve el productor Kafka.
    """

    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,

        # Garantía de confirmación del broker.
        acks="all",

        # Reintentos ante errores recuperables.
        retries=5,

        # Evita duplicados por reintentos dentro
        # de una misma sesión del productor.
        enable_idempotence=True,

        # Serialización gestionada por event_contract.py.
        # Kafka recibe directamente bytes UTF-8.
        request_timeout_ms=30000,
        delivery_timeout_ms=120000
    )


# ============================================================
# 5. PUBLICACIÓN DE EVENTOS
# ============================================================

def publish_event(producer, event):
    """
    Valida, serializa y publica un evento en Kafka.

    Espera la confirmación del broker antes de retornar.
    """

    # Obtener la clave Kafka a partir del país.
    key = get_event_key(event)

    # Validar y serializar el evento.
    value = serialize_event(event)

    # Publicar el mensaje.
    future = producer.send(
        KAFKA_TOPIC,
        key=key,
        value=value
    )

    # Esperar confirmación del broker.
    metadata = future.get(
        timeout=ACK_TIMEOUT_SECONDS
    )

    return metadata


# ============================================================
# 6. EJECUCIÓN DEL PRODUCTOR
# ============================================================

def run_producer():
    """
    Lee el dataset y publica sus registros en Kafka.

    Genera un resumen de ejecución con los resultados.
    """

    print("=" * 65)
    print("PRODUCTOR KAFKA - WORLD HAPPINESS")
    print("=" * 65)

    # --------------------------------------------------------
    # Validar configuración
    # --------------------------------------------------------

    if STREAM_DELAY_SECONDS < 0:
        raise ValueError(
            "STREAM_DELAY_SECONDS no puede ser negativo."
        )

    if not DATASET_PATH.exists():
        raise FileNotFoundError(
            f"No se encontró el dataset: {DATASET_PATH}"
        )

    # --------------------------------------------------------
    # Cargar dataset
    # --------------------------------------------------------

    df = pd.read_csv(DATASET_PATH)

    total_records = len(df)

    print(f"Dataset: {DATASET_PATH}")
    print(f"Registros encontrados: {total_records}")
    print(f"Broker: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Topic: {KAFKA_TOPIC}")
    print(f"Delay: {STREAM_DELAY_SECONDS} segundos")

    # --------------------------------------------------------
    # Inicializar contadores
    # --------------------------------------------------------

    published = 0
    invalid = 0
    kafka_errors = 0

    start_time = time.perf_counter()

    producer = None

    try:

        # Crear conexión con Kafka.
        producer = create_kafka_producer()

        print("\nIniciando publicación...\n")

        # ----------------------------------------------------
        # Procesamiento registro por registro
        # ----------------------------------------------------

        for index, (_, row) in enumerate(
            df.iterrows(),
            start=1
        ):

            # Construcción y validación del evento.
            try:

                event = build_event(row)

                metadata = publish_event(
                    producer,
                    event
                )

                published += 1

                # Pausa configurable entre publicaciones.
                if STREAM_DELAY_SECONDS > 0:
                    time.sleep(STREAM_DELAY_SECONDS)

            except (EventValidationError, ValueError,
                    TypeError, KeyError) as exc:

                invalid += 1

                print(
                    f"[INVALID] Registro {index}: {exc}"
                )

                continue

            except KafkaError as exc:

                kafka_errors += 1

                print(
                    f"[KAFKA ERROR] Registro {index}: {exc}"
                )

                # Si Kafka falla, detenemos el envío
                # para evitar continuar sin confirmaciones.
                break

            except Exception as exc:

                kafka_errors += 1

                print(
                    f"[ERROR] Registro {index}: {exc}"
                )

                break

        # ----------------------------------------------------
        # Vaciar mensajes pendientes
        # ----------------------------------------------------

        producer.flush(
            timeout=ACK_TIMEOUT_SECONDS
        )

    finally:

        if producer is not None:
            producer.close(
                timeout=ACK_TIMEOUT_SECONDS
            )

    # --------------------------------------------------------
    # Resumen de ejecución
    # --------------------------------------------------------

    elapsed_time = time.perf_counter() - start_time

    print("\n" + "=" * 65)
    print("RESUMEN DE PUBLICACIÓN")
    print("=" * 65)

    print(f"Registros procesados: {published + invalid + kafka_errors}")
    print(f"Eventos publicados:  {published}")
    print(f"Eventos inválidos:   {invalid}")
    print(f"Errores Kafka:       {kafka_errors}")
    print(f"Tiempo de ejecución: {elapsed_time:.2f} segundos")

    print("=" * 65)

    return {
        "total_records": total_records,
        "published": published,
        "invalid": invalid,
        "kafka_errors": kafka_errors,
        "elapsed_seconds": round(elapsed_time, 2)
    }


# ============================================================
# 7. PUNTO DE ENTRADA
# ============================================================

if __name__ == "__main__":
    run_producer()
