-- ============================================================
-- WORKSHOP 3 - HAPPINESS STREAMING ETL
-- PostgreSQL DDL
-- ============================================================

BEGIN;

-- ============================================================
-- 1. TABLA: raw_events
-- Conserva cada mensaje recibido desde Kafka.
-- ============================================================

CREATE TABLE IF NOT EXISTS public.raw_events (

    raw_event_id BIGINT GENERATED ALWAYS AS IDENTITY,

    kafka_topic VARCHAR(100) NOT NULL,
    kafka_partition INTEGER NOT NULL,
    kafka_offset BIGINT NOT NULL,

    kafka_key TEXT,

    raw_payload BYTEA NOT NULL,

    event_id VARCHAR(150),

    status VARCHAR(30) NOT NULL DEFAULT 'RECEIVED',

    is_duplicate BOOLEAN NOT NULL DEFAULT FALSE,

    error_message TEXT,

    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_raw_events
        PRIMARY KEY (raw_event_id),

    CONSTRAINT uq_raw_events_kafka_position
        UNIQUE (kafka_topic, kafka_partition, kafka_offset),

    CONSTRAINT ck_raw_events_partition
        CHECK (kafka_partition >= 0),

    CONSTRAINT ck_raw_events_offset
        CHECK (kafka_offset >= 0),

    CONSTRAINT ck_raw_events_status
        CHECK (
            status IN (
                'RECEIVED',
                'VALID',
                'INVALID_SCHEMA',
                'INVALID_VALUES',
                'PREDICTION_ERROR'
            )
        )

);


-- ============================================================
-- 2. TABLA: processed_events
-- Controla los eventos que ingresan al procesamiento ML.
-- event_id garantiza la deduplicación.
-- ============================================================

CREATE TABLE IF NOT EXISTS public.processed_events (

    event_id VARCHAR(150),

    raw_event_id BIGINT NOT NULL,

    processing_status VARCHAR(30) NOT NULL DEFAULT 'PROCESSING',

    model_version VARCHAR(100),

    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    completed_at TIMESTAMPTZ,

    CONSTRAINT pk_processed_events
        PRIMARY KEY (event_id),

    CONSTRAINT uq_processed_events_raw
        UNIQUE (raw_event_id),

    CONSTRAINT fk_processed_events_raw
        FOREIGN KEY (raw_event_id)
        REFERENCES public.raw_events (raw_event_id)
        ON DELETE RESTRICT,

    CONSTRAINT ck_processed_events_status
        CHECK (
            processing_status IN (
                'PROCESSING',
                'PREDICTED',
                'PREDICTION_ERROR'
            )
        ),

    CONSTRAINT ck_processed_events_timestamps
        CHECK (
            completed_at IS NULL
            OR completed_at >= processed_at
        )

);


-- ============================================================
-- 3. TABLA: predictions
-- Almacena las predicciones y los valores reales.
-- ============================================================

CREATE TABLE IF NOT EXISTS public.predictions (

    prediction_id BIGINT GENERATED ALWAYS AS IDENTITY,

    event_id VARCHAR(150) NOT NULL,

    predicted_score DOUBLE PRECISION NOT NULL,

    actual_score DOUBLE PRECISION NOT NULL,

    absolute_error DOUBLE PRECISION
        GENERATED ALWAYS AS (
            ABS(actual_score - predicted_score)
        ) STORED,

    model_version VARCHAR(100) NOT NULL,

    predicted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_predictions
        PRIMARY KEY (prediction_id),

    CONSTRAINT uq_predictions_event
        UNIQUE (event_id),

    CONSTRAINT fk_predictions_processed
        FOREIGN KEY (event_id)
        REFERENCES public.processed_events (event_id)
        ON DELETE RESTRICT,

    CONSTRAINT ck_predictions_predicted_score
        CHECK (
            predicted_score >= 0
            AND predicted_score <= 10
        ),

    CONSTRAINT ck_predictions_actual_score
        CHECK (
            actual_score >= 0
            AND actual_score <= 10
        ),

    CONSTRAINT ck_predictions_model_version
        CHECK (LENGTH(TRIM(model_version)) > 0)

);


-- ============================================================
-- 4. ÍNDICES ADICIONALES
-- ============================================================

CREATE INDEX IF NOT EXISTS idx_raw_events_event_id
    ON public.raw_events (event_id);

CREATE INDEX IF NOT EXISTS idx_raw_events_status
    ON public.raw_events (status);

CREATE INDEX IF NOT EXISTS idx_raw_events_received_at
    ON public.raw_events (received_at);

CREATE INDEX IF NOT EXISTS idx_raw_events_duplicates
    ON public.raw_events (is_duplicate)
    WHERE is_duplicate = TRUE;


CREATE INDEX IF NOT EXISTS idx_processed_events_status
    ON public.processed_events (processing_status);


CREATE INDEX IF NOT EXISTS idx_predictions_predicted_at
    ON public.predictions (predicted_at);

CREATE INDEX IF NOT EXISTS idx_predictions_model_version
    ON public.predictions (model_version);


-- ============================================================
-- 5. COMENTARIOS DE DOCUMENTACIÓN
-- ============================================================

COMMENT ON TABLE public.raw_events IS
    'Registro de todos los mensajes originales recibidos desde Kafka.';

COMMENT ON TABLE public.processed_events IS
    'Control de idempotencia y seguimiento del procesamiento de eventos.';

COMMENT ON TABLE public.predictions IS
    'Resultados de inferencia del modelo de felicidad.';

COMMENT ON COLUMN public.raw_events.raw_payload IS
    'Bytes originales del mensaje Kafka, sin reconstrucción del JSON.';

COMMENT ON COLUMN public.raw_events.is_duplicate IS
    'Indica si el event_id ya había sido procesado anteriormente.';

COMMENT ON COLUMN public.predictions.absolute_error IS
    'Error absoluto calculado automáticamente por PostgreSQL.';


COMMIT;