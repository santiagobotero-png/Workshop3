
import json
import math

from datetime import datetime


# ============================================================
# 1. DEFINICIÓN DEL CONTRATO
# ============================================================

REQUIRED_FIELDS = {
    "event_id",
    "country",
    "year",
    "gdp",
    "social_support",
    "health",
    "freedom",
    "corruption",
    "generosity",
    "actual_happiness_score",
    "event_time"
}

MODEL_FEATURES = [
    "gdp",
    "social_support",
    "health",
    "freedom",
    "corruption",
    "generosity"
]

NUMERIC_FIELDS = [
    "gdp",
    "social_support",
    "health",
    "freedom",
    "corruption",
    "generosity",
    "actual_happiness_score"
]

NULLABLE_FIELDS = {"corruption"}

ALLOWED_YEARS = {2015, 2016, 2017, 2018, 2019}

TARGET_FIELD = "actual_happiness_score"


# ============================================================
# 2. EXCEPCIÓN PERSONALIZADA
# ============================================================

class EventValidationError(ValueError):
    """Error producido por una validación o deserialización."""

    def __init__(self, status, errors):
        self.status = status
        self.errors = errors

        message = "; ".join(
            error["message"] for error in errors
        )

        super().__init__(message)


# ============================================================
# 3. FUNCIONES AUXILIARES
# ============================================================

def _add_error(errors, field, category, message):
    """Agrega un error estructurado al resultado."""

    errors.append({
        "field": field,
        "category": category,
        "message": message
    })


def _is_valid_utc_timestamp(value):
    """
    Comprueba que el timestamp sea ISO 8601
    y utilice UTC con el sufijo Z.
    """

    if not isinstance(value, str):
        return False

    if not value.endswith("Z"):
        return False

    try:
        parsed = datetime.fromisoformat(
            value[:-1] + "+00:00"
        )

        return (
            parsed.tzinfo is not None
            and parsed.utcoffset().total_seconds() == 0
        )

    except (ValueError, OverflowError):
        return False


# ============================================================
# 4. VALIDACIÓN DEL EVENTO
# ============================================================

def validate_event(event):
    """
    Valida la estructura y los valores de un evento.

    Retorna:
        {
            "valid": bool,
            "status": str,
            "errors": list
        }
    """

    errors = []

    # --------------------------------------------------------
    # Validación de estructura general
    # --------------------------------------------------------

    if not isinstance(event, dict):
        _add_error(
            errors,
            "event",
            "INVALID_SCHEMA",
            "El evento debe ser un objeto JSON."
        )

        return {
            "valid": False,
            "status": "INVALID_SCHEMA",
            "errors": errors
        }

    missing_fields = REQUIRED_FIELDS - set(event.keys())
    unexpected_fields = set(event.keys()) - REQUIRED_FIELDS

    for field in sorted(missing_fields):
        _add_error(
            errors,
            field,
            "INVALID_SCHEMA",
            f"Falta el campo obligatorio: {field}."
        )

    for field in sorted(unexpected_fields):
        _add_error(
            errors,
            field,
            "INVALID_SCHEMA",
            f"Campo no permitido: {field}."
        )

    # --------------------------------------------------------
    # Validación de event_id
    # --------------------------------------------------------

    if "event_id" in event:
        value = event["event_id"]

        if not isinstance(value, str):
            _add_error(
                errors,
                "event_id",
                "INVALID_SCHEMA",
                "event_id debe ser un string."
            )

        elif not value.strip():
            _add_error(
                errors,
                "event_id",
                "INVALID_VALUES",
                "event_id no puede estar vacío."
            )

    # --------------------------------------------------------
    # Validación de country
    # --------------------------------------------------------

    if "country" in event:
        value = event["country"]

        if not isinstance(value, str):
            _add_error(
                errors,
                "country",
                "INVALID_SCHEMA",
                "country debe ser un string."
            )

        elif not value.strip():
            _add_error(
                errors,
                "country",
                "INVALID_VALUES",
                "country no puede estar vacío."
            )

    # --------------------------------------------------------
    # Validación de year
    # --------------------------------------------------------

    if "year" in event:
        value = event["year"]

        if isinstance(value, bool) or not isinstance(value, int):
            _add_error(
                errors,
                "year",
                "INVALID_SCHEMA",
                "year debe ser un entero."
            )

        elif value not in ALLOWED_YEARS:
            _add_error(
                errors,
                "year",
                "INVALID_VALUES",
                "year debe estar entre 2015 y 2019."
            )

    # --------------------------------------------------------
    # Validación de variables numéricas
    # --------------------------------------------------------

    for field in NUMERIC_FIELDS:

        if field not in event:
            continue

        value = event[field]

        # Solo corruption admite null
        if value is None and field in NULLABLE_FIELDS:
            continue

        # Rechazar booleanos y tipos no numéricos
        if isinstance(value, bool) or not isinstance(
            value, (int, float)
        ):
            _add_error(
                errors,
                field,
                "INVALID_SCHEMA",
                f"{field} debe ser numérico."
            )

            continue

        # Rechazar NaN e infinitos
        try:
            is_finite = math.isfinite(value)
        except OverflowError:
            is_finite = False

        if not is_finite:
            _add_error(
                errors,
                field,
                "INVALID_VALUES",
                f"{field} debe ser un número finito."
            )

            continue

        # Los indicadores deben ser no negativos
        if field in MODEL_FEATURES and value < 0:
            _add_error(
                errors,
                field,
                "INVALID_VALUES",
                f"{field} no puede ser negativo."
            )

        # El score real debe estar entre 0 y 10
        if field == TARGET_FIELD and not 0 <= value <= 10:
            _add_error(
                errors,
                field,
                "INVALID_VALUES",
                "actual_happiness_score debe estar entre 0 y 10."
            )

    # --------------------------------------------------------
    # Validación de event_time
    # --------------------------------------------------------

    if "event_time" in event:
        if not _is_valid_utc_timestamp(event["event_time"]):
            _add_error(
                errors,
                "event_time",
                "INVALID_VALUES",
                "event_time debe ser ISO 8601 en UTC y terminar en Z."
            )

    # --------------------------------------------------------
    # Resultado final
    # --------------------------------------------------------

    if errors:
        # Si existe al menos un error estructural,
        # el estado general será INVALID_SCHEMA.
        status = (
            "INVALID_SCHEMA"
            if any(
                error["category"] == "INVALID_SCHEMA"
                for error in errors
            )
            else "INVALID_VALUES"
        )

        return {
            "valid": False,
            "status": status,
            "errors": errors
        }

    return {
        "valid": True,
        "status": "VALID",
        "errors": []
    }


# ============================================================
# 5. SERIALIZACIÓN JSON
# ============================================================

def serialize_event(event):
    """
    Valida y convierte un evento a JSON UTF-8.

    Retorna:
        bytes
    """

    result = validate_event(event)

    if not result["valid"]:
        raise EventValidationError(
            result["status"],
            result["errors"]
        )

    return json.dumps(
        event,
        ensure_ascii=False,
        allow_nan=False
    ).encode("utf-8")


# ============================================================
# 6. DESERIALIZACIÓN JSON
# ============================================================

def deserialize_event(message):
    """
    Convierte un mensaje JSON a un diccionario Python.

    La validación del contrato se realiza posteriormente
    mediante validate_event().
    """

    try:

        if isinstance(message, (bytes, bytearray)):
            message = message.decode("utf-8")

        if not isinstance(message, str):
            raise TypeError(
                "El mensaje debe ser string o bytes."
            )

        event = json.loads(message)

        if not isinstance(event, dict):
            raise ValueError(
                "El contenido JSON debe ser un objeto."
            )

        return event

    except (UnicodeDecodeError, json.JSONDecodeError,
            TypeError, ValueError) as exc:

        raise EventValidationError(
            "INVALID_SCHEMA",
            [{
                "field": "event",
                "category": "INVALID_SCHEMA",
                "message": f"Mensaje JSON inválido: {exc}"
            }]
        ) from exc


# ============================================================
# 7. OBTENCIÓN DE LA CLAVE KAFKA
# ============================================================

def get_event_key(event):
    """
    Obtiene country como clave Kafka codificada en UTF-8.
    """

    if not isinstance(event, dict):
        raise ValueError("El evento debe ser un diccionario.")

    result = validate_event(event)

    if not result["valid"]:
        raise EventValidationError(
            result["status"],
            result["errors"]
        )

    return event["country"].encode("utf-8")
