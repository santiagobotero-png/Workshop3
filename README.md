# WORKSHOP_3: Streaming ETL with Apache Kafka and Machine Learning

[![Python](https://img.shields.io/badge/Python-3.10%2B-blue.svg)](https://www.python.org/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-4.4.0-black.svg?logo=apachekafka)](https://kafka.apache.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-18-blue.svg?logo=postgresql)](https://www.postgresql.org/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED.svg?logo=docker)](https://www.docker.com/)
[![Scikit-Learn](https://img.shields.io/badge/Scikit--Learn-1.3%2B-F7931E.svg?logo=scikitlearn)](https://scikit-learn.org/)
[![Power BI](https://img.shields.io/badge/Power%20BI-Desktop-F2C811.svg?logo=powerbi)](https://powerbi.microsoft.com/)

## 1. Contexto General del Proyecto

**WORKSHOP_3** es un proyecto académico e industrial de ingeniería de datos avanzado que implementa un pipeline ETL híbrido (Batch & Real-time Streaming), combinando inferencia de **Machine Learning** en tiempo de procesamiento, **Apache Kafka**, persistencia operacional en **PostgreSQL**, modelado dimensional en esquema estrella y un dashboard interactivo en **Power BI**.

La solución toma como base histórica los datos del *World Happiness Report* correspondientes al quinquenio **2015–2019** (782 registros en total) para predecir el indicador objetivo `happiness_score` a partir de seis variables socioeconómicas principales.

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                                ARQUITECTURA DE LA SOLUCIÓN                              │
└─────────────────────────────────────────────────────────────────────────────────────────┘

 ┌───────────────────────────────────────────────────────────────────────────────────────┐
 │                                  1. PROCESAMIENTO BATCH                               │
 │                                                                                       │
 │ ┌───────────────┐     ┌──────────────────────┐     ┌────────────────────────────────┐ │
 │ │  Raw Data     │ ──> │ Cleaning &           │ ──> │ Model Training & Comparison    │ │
 │ │ (2015-2019)   │     │ Harmonization (EDA)  │     │ (Linear, Tree, Random Forest)  │ │
 │ └───────────────┘     └──────────────────────┘     └───────────────┬────────────────┘ │
 └────────────────────────────────────────────────────────────────────┼──────────────────┘
                                                                      │ Serialized Model
                                                                      ▼ (models/model.pkl)
 ┌───────────────────────────────────────────────────────────────────────────────────────┐
 │                                 2. PROCESAMIENTO STREAMING                            │
 │                                                                                       │
 │ ┌───────────────┐  Producer   ┌───────────────────┐  Consumer  ┌────────────────────┐ │
 │ │ Consolidated  │ ──────────> │ Apache Kafka      │ ─────────> │ Python ML Consumer │ │
 │ │ Dataset (CSV) │  acks=all   │ topic:            │  No Auto-  │ Contract Valid.    │ │
 │ └───────────────┘             │ happiness-events  │  Commit    │ Model Inference    │ │
 │                               └───────────────────┘            └───────────┬────────┘ │
 └────────────────────────────────────────────────────────────────────────────┼──────────┘
                                                                              │
 ┌────────────────────────────────────────────────────────────────────────────┼──────────┐
 │                                3. PERSISTENCIA Y ANALÍTICA                 │          │
 │                                                                            ▼          │
 │  ┌──────────────────────────────────────────────────────────────────────────────────┐ │
 │  │                              PostgreSQL (happiness_streaming)                    │ │
 │  │                                                                                  │ │
 │  │  [ Capa Operacional ]                                                            │ │
 │  │  ├── raw_events (BYTEA payload, deduplication control)                           │ │
 │  │  ├── processed_events (status, model version, timestamps)                         │ │
 │  │  └── predictions (predicted_score, actual_score, absolute_error)                 │ │
 │  │                                                                                  │ │
 │  │  [ Capa Analítica - Esquema Estrella ]                                           │ │
 │  │  ├── dim_country (164 países)                                                    │ │
 │  │  ├── dim_date (atributos temporales de emisión)                                  │ │
 │  │  └── fact_predictions (782 hechos con report_year histórico)                    │ │
 │  └──────────────────────────────────────────┬───────────────────────────────────────┘ │
 │                                             │ Direct Import Connection                │
 │                                             ▼                                         │
 │  ┌──────────────────────────────────────────────────────────────────────────────────┐ │
 │  │                         Power BI (Model Performance Overview)                    │ │
 │  └──────────────────────────────────────────────────────────────────────────────────┘ │
 └───────────────────────────────────────────────────────────────────────────────────────┘
```

### Relación entre Modalidades Batch y Streaming
- **Batch Processing:** Reservado para la recolección, limpieza, armonización de los 5 datasets heterogéneos, feature engineering y el entrenamiento/evaluación offline de modelos de regresión. Culmina con la exportación del artefacto de ML entrenado (`model.pkl`).
- **Streaming Processing:** Utilizado para la transportación desacoplada y asíncrona de eventos con Apache Kafka, la validación estricta de contratos de datos, la inferencia en tiempo real al consumir cada mensaje, el control operacional de idempotencia/deduplicación y la persistencia estructurada.

---

## 2. Fase A: Exploración, Limpieza y Preparación de Datos (EDA)

### Fuentes de Información de Origen
Ubicación de archivos originales: `data/raw/`
- `2015.csv`: 158 registros, 12 columnas.
- `2016.csv`: 157 registros, 13 columnas.
- `2017.csv`: 155 registros, 12 columnas.
- `2018.csv`: 156 registros, 9 columnas.
- `2019.csv`: 156 registros, 9 columnas.
- **Total acumulado:** 782 registros.

### Dataset Consolidado
Ubicación: `data/processed/world_happiness_2015_2019.csv` (782 registros, 10 columnas, 5 años).

### Esquema Canónico
| Columna | Tipo de Dato | Descripción |
| :--- | :--- | :--- |
| `year` | Integer | Año histórico de publicación del reporte |
| `country` | Text | Nombre normalizado del país |
| `happiness_rank` | Integer | Posición del país en el ranking anual (derivada) |
| `happiness_score` | Float | Puntuación objetivo de felicidad |
| `gdp` | Float | Producto Interno Bruto per cápita (normalizado) |
| `social_support` | Float | Apoyo social (unificación de Family / Social Support) |
| `health` | Float | Esperanza de vida saludable |
| `freedom` | Float | Libertad para tomar decisiones de vida |
| `corruption` | Float | Percepción de corrupción (1 nulo conocido en UAE 2018) |
| `generosity` | Float | Indicador de generosidad |

### Correlaciones Bivariadas Anuales (Aproximadas) con `happiness_score`
> *Nota: Estas métricas corresponden a asociaciones estadísticas bivariadas y no implican causalidad.*

| Indicador | 2015 | 2016 | 2017 | 2018 | 2019 |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **GDP** | 0.781 | 0.790 | 0.812 | 0.802 | 0.794 |
| **Health** | 0.724 | 0.765 | 0.782 | 0.776 | 0.780 |
| **Social Support** | 0.741 | 0.739 | 0.753 | 0.746 | 0.777 |
| **Freedom** | 0.568 | 0.567 | 0.570 | 0.544 | 0.567 |
| **Corruption** | 0.395 | 0.402 | 0.429 | 0.405 | 0.386 |
| **Generosity** | 0.180 | 0.157 | 0.155 | 0.136 | 0.076 |

### Decisiones Metodológicas de Limpieza y Armonización
1. **Homologación de nombres:** Mapeo de variables cambiantes (`Family` en 2015-2016 y `Social support` en 2017-2019 unificados en `social_support`).
2. **Normalización geográfica:** Estandarización de nombres para evitar colisiones o fragmentaciones (`Somaliland region` → `Somaliland Region`, `Macedonia` → `North Macedonia`, `Hong Kong S.A.R., China` → `Hong Kong`, `Taiwan Province of China` → `Taiwan`, `Trinidad & Tobago` → `Trinidad and Tobago`, `North Cyprus / Northern Cyprus` → `Northern Cyprus`, `Swaziland` → `Eswatini`).
3. **Preservación de atípicos e incompletos:** Se conservó el valor nulo de `corruption` para UAE (2018) y los 5 valores cero en `health` al no existir evidencia suficiente de error de digitalización.
4. **Sin imputación destructiva en raw:** No se eliminaron observaciones ni se aplicó imputación en la capa de datos canónica para preservar trazabilidad.
5. **Panel Longitudinal:** Se identificaron 141 países presentes ininterrumpidamente durante los 5 años ($141 \times 5 = 705$ observaciones) para análisis longitudinales específicos, manteniendo los 782 registros en la tabla procesada general.

### Variables Excluidas del Modelado Iniciar
- `happiness_rank`: Excluida por ser una variable estrictamente derivada de la variable objetivo (evitar Target Leakage).
- `Dystopia Residual`: Excluida debido a su relación mecánica con la construcción del indicador.
- `country`, `year`, `region`: Excluidas como predictores en el modelo inicial numérico para evitar sesgos o codificaciones categóricas prematuras.

---

## 3. Feature Engineering y Modelo Baseline

### Definición del Problema
Regresión supervisada multivariable:
- **Predictoras ($X$):** `gdp`, `social_support`, `health`, `freedom`, `corruption`, `generosity`.
- **Objetivo ($y$):** `happiness_score`.

### Estadísticas Descriptivas de las Predictoras
- **GDP:** Media = 0.916, Std = 0.407, Mediana = 0.982, Máx = 2.096
- **Social Support:** Media = 1.078, Std = 0.330, Mediana = 1.125, Máx = 1.644
- **Health:** Media = 0.612, Std = 0.248, Mediana = 0.647, Máx = 1.141
- **Freedom:** Media = 0.411, Std = 0.153, Mediana = 0.431, Máx = 0.724
- **Corruption:** Media = 0.125, Std = 0.106, Mediana = 0.091, Máx = 0.552 (781 válidos, 1 nulo)
- **Generosity:** Media = 0.219, Std = 0.122, Mediana = 0.202, Máx = 0.838
- **Happiness Score (Target):** Media = 5.379, Std = 1.127, Mín = 2.693, Mediana = 5.322, Máx = 7.769

*Correlación global GDP-Health: 0.784. Correlación global GDP-Happiness: 0.789.*

### Modelo Baseline (Linear Regression Pipeline)
- **Partición:** Split 80% entrenamiento (625 reg), 20% prueba (157 reg), `random_state=42`, `shuffle=True`. Redondeo a 3 decimales en memoria.
- **Pipeline:** `SimpleImputer(strategy='median')` → `StandardScaler()` → `LinearRegression()`.
- **Valores Aprendidos en Train:**
  - Medianas imputer: GDP (0.979), Social Support (1.125), Health (0.644), Freedom (0.433), Corruption (0.091), Generosity (0.204).
  - Medias scaler: GDP (0.911), Social Support (1.080), Health (0.609), Freedom (0.413), Corruption (0.127), Generosity (0.222).
  - Coeficientes estandarizados: GDP (0.438), Social Support (0.217), Health (0.256), Freedom (0.225), Corruption (0.128), Generosity (0.059). Intercepto: 5.377.

#### Métricas del Baseline (Conjunto de Prueba 20% - 157 reg)
- **MAE:** 0.441
- **MSE:** 0.329
- **RMSE:** 0.574
- **$R^2$:** 0.729
- **Análisis de Residuos ($Residual = Actual - Predicted$):** Media = -0.001, Std = 0.576, Min = -1.767, Mediana = 0.014, Max = 1.434.
- **Artefacto generado:** `models/happiness_regression_pipeline.joblib`.

---

## 4. Fase B: Comparación y Selección del Modelo Final

Se realizó un segundo experimento de evaluación comparativa utilizando una división limpia del **70% Entrenamiento (547 registros)** y **30% Prueba (235 registros)** con `random_state=42`.

### Resultados Comparativos de Modelos

| Modelo | MAE | MSE | RMSE | $R^2$ |
| :--- | :---: | :---: | :---: | :---: |
| **Linear Regression** | 0.449 | 0.341 | 0.584 | 0.727 |
| **Decision Tree Regressor** | 0.582 | 0.578 | 0.760 | 0.537 |
| **Random Forest Regressor** | **0.408** | **0.272** | **0.522** | **0.782** |

> **Aclaración Metodológica:** Las métricas del Baseline (80/20) y de esta etapa comparativa (70/30) provienen de particiones distintas, por lo cual no son comparables directamente entre sí.

### Selección y Configuración del Modelo Final
Se seleccionó **Random Forest Regressor** debido a su capacidad superior de generalización y menor tasa de error absoluto y cuadrático en el conjunto de test.

- **Hiperparámetros:** `n_estimators=200`, `random_state=42`, `n_jobs=-1`.
- **Preprocesamiento:** Imputación por mediana integrada en el pipeline.
- **Artefacto exportado:** `models/model.pkl` (~9.73 MB).
- **Validación de Serialización:** Carga verificada mediante `joblib.load()`, produciendo un 100% de coincidencia exacta ($Delta = 0.000000$) respecto al modelo en memoria.

---

## 5. Fase B: Arquitectura Apache Kafka y Streaming Pipeline

### Infraestructura Contenerizada (`docker-compose.yml`)
- **Kafka Broker:** Imagen `apache/kafka:4.4.0`, Contenedor: `happiness-streaming-broker`, Puerto Host: `9092`.
- **PostgreSQL DB:** Imagen `postgres:18`, Contenedor: `happiness-streaming-db`, Puerto Host: `5433` (interno `5432`). Base de datos: `happiness_streaming`.

### Topic Kafka Configuration
- **Nombre:** `happiness-events`
- **Particiones:** 3
- **Replication Factor:** 1
- **Message Key:** `country` (Asegura que todos los eventos de un mismo país arriben ordenados a la misma partición).
- **Distribución real observada en producción:**
  - Partición 0: 654 mensajes
  - Partición 1: 440 mensajes
  - Partición 2: 470 mensajes
  - **Total:** 1,564 mensajes (2 ejecuciones completas del productor).

### Productor Kafka (`src/streaming/producer.py`)
- Lee el dataset consolidado (`world_happiness_2015_2019.csv`).
- Genera eventos con `event_id` determinístico basado en el patrón: `HAPPINESS-{year}-{COUNTRY_UPPER}`.
- Incorpora timestamp UTC (`event_time`) con formato estricto ISO-8601 (terminación `Z`).
- Configuración de tolerancia a fallos: `acks=all`, `retries=5`, `enable_idempotence=True`.
- Delay de transmisión: `STREAM_DELAY_SECONDS=0.5`.
- **Métricas de producción:** 2 ejecuciones completas realizadas. $782 \times 2 = 1,564$ eventos transmitidos, 0 fallos de red, 0 mensajes rechazados por broker.

---

## 6. Contrato de Eventos (`src/streaming/event_contract.py`)

Para desacoplar el broker Kafka de la lógica de negocio del modelo, se definió un contrato estricto de validación previa a la inferencia.

### Estructura del Contrato
```python
REQUIRED_FIELDS = [
    "event_id", "country", "year", "gdp", "social_support", 
    "health", "freedom", "corruption", "generosity", 
    "actual_happiness_score", "event_time"
]

MODEL_FEATURES = ["gdp", "social_support", "health", "freedom", "corruption", "generosity"]
NUMERIC_FIELDS = ["gdp", "social_support", "health", "freedom", "corruption", "generosity", "actual_happiness_score"]
NULLABLE_FIELDS = ["corruption"]
ALLOWED_YEARS = [2015, 2016, 2017, 2018, 2019]
TARGET_FIELD = "actual_happiness_score"
```

### Reglas de Validación Aplicadas
1. Presencia obligatoria de todos los campos definidos en `REQUIRED_FIELDS`.
2. Rechazo de campos adicionales o no especificados (*strict schema*).
3. Validación de tipos numéricos (`float`/`int`). Se permite valor nulo explícito únicamente en `corruption`.
4. `year` debe pertenecer restrictivamente a `ALLOWED_YEARS`.
5. `event_time` debe ser una cadena ISO-8601 válida finalizada en `Z`.
6. `actual_happiness_score` se requiere exclusivamente para registro/evaluación offline y **jamás** se incluye como característica dentro de `MODEL_FEATURES`.

---

## 7. Consumidor Kafka e Inferencia en Tiempo Real

### Consumidor (`src/streaming/consumer.py`)
- **Group ID:** `happiness-ml-team`
- **Estrategia de Offsets:** `enable_auto_commit=False`, `auto_offset_reset='earliest'`, `max_poll_records=1`.
- Carga el artefacto `models/model.pkl` en memoria durante la inicialización.

```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                              FLUJO DE TRABAJO DEL CONSUMIDOR                           │
└────────────────────────────────────────────────────────────────────────────────────────┘

 [ Mensaje Kafka ] ──> 1. Persistencia Raw Inmediata (raw_events BYTEA)
                             │
                             ▼
                       2. Validación de Contrato
                             │
            ┌────────────────┴────────────────┐
            │ VÁLIDO                          │ INVÁLIDO
            ▼                                 ▼
      3. Chequeo Deduplicación (event_id)  Registrar Status INVALID_* en raw_events
            │
      ┌─────┴────────────────┐
      │ ÚNICO                │ DUPLICADO
      ▼                      ▼
  4. Inferencia ML      Marcar is_duplicate=TRUE en raw_events
     (Random Forest)         │
      │                      └─> Finalizar procesamiento del evento
      ▼
  5. Almacenar Predicción (predictions)
      │
      ▼
  6. Transacción Completada -> Manual Offset Commit
```

### Mecanismo de Deduplicación y Persistencia Raw
- **Persistencia Raw Inmediata:** El mensaje se guarda en la tabla `raw_events` antes de validar el contrato o invocar el modelo, garantizando auditoría total.
- **Control de Duplicados de Negocio:** Se evalúa la existencia del `event_id`. Si el evento ya fue procesado con éxito anteriormente, el payload secundario se conserva en `raw_events` asignando `is_duplicate = TRUE` y omitiendo la re-ejecución de la inferencia.
- **Garantías Transactional Commit:** El offset de Kafka solo se confirma (`commit()`) tras el éxito de la transacción SQL en PostgreSQL.

---

## 8. Fase C: Persistencia Operational y Modelo Analítico Dimensional

Base de Datos PostgreSQL: `happiness_streaming`.

### 8.1 Capa Operacional

#### Tabla `raw_events` (Conserva funcionalidad de `raw_happiness_events`)
- **Propósito:** Registro raw idempotente de auditoría para todos los mensajes recibidos del broker.
- **Campos:** `raw_event_id` (PK), `kafka_topic`, `kafka_partition`, `kafka_offset`, `kafka_key`, `raw_payload` (BYTEA), `event_id`, `status`, `is_duplicate`, `error_message`, `received_at`.
- **Restricción de Integridad:** `CONSTRAINT unique_kafka_message UNIQUE (kafka_topic, kafka_partition, kafka_offset)`.
- **Estados de Procesamiento (`status`):** `RECEIVED`, `VALID`, `INVALID_SCHEMA`, `INVALID_VALUES`, `PREDICTION_ERROR`.

#### Tabla `processed_events`
- **Propósito:** Estado de ciclo de vida para los eventos de negocio que avanzan a inferencia.
- **Campos:** `event_id` (PK), `raw_event_id` (FK a `raw_events`, Unique), `processing_status`, `model_version`, `processed_at`, `completed_at`.
- **Estados:** `PROCESSING`, `PREDICTED`, `PREDICTION_ERROR`.

#### Tabla `predictions`
- **Propósito:** Almacenar el resultado cuantitativo de la inferencia ML.
- **Campos:** `prediction_id` (PK), `event_id` (FK a `processed_events`, Unique), `predicted_score`, `actual_score`, `absolute_error` (calculado mediante `ABS(actual_score - predicted_score)`), `model_version`, `predicted_at`.

---

### 8.2 Capa Analítica — Modelo Dimensional en Esquema Estrella

```
           ┌───────────────────────────┐
           │        dim_country        │
           ├───────────────────────────┤
           │ PK  country_id            │
           │     country_name          │
           └─────────────┬─────────────┘
                         │ 1
                         │
                         │ *
┌────────────────────────┴────────────────────────┐          ┌───────────────────────────┐
│                 fact_predictions                │          │         dim_date          │
├─────────────────────────────────────────────────┤          ├───────────────────────────┤
│ PK      prediction_id                           │          │ PK  date_id (YYYYMMDD)    │
│ FK, UNQ raw_event_id ──> [raw_events]           │ *      1 │     full_date             │
│ FK      country_id   ───────────────────────────┼──────────┤     year                  │
│ FK      date_id      ───────────────────────────┘          │     quarter               │
│         event_time                              │          │     month                 │
│         actual_score                            │          │     month_name            │
│         predicted_score                         │          │     day                   │
│         prediction_error                        │          │     day_of_week           │
│         prediction_timestamp                    │          └───────────────────────────┘
│         report_year                             │
└─────────────────────────────────────────────────┘
```

#### Dimensión `dim_country`
- **Registros:** 164 países únicos cargados.
- **Atributos:** `country_id` (PK), `country_name` (Unique).

#### Dimensión `dim_date`
- **Propósito:** Representa la fecha de emisión/procesamiento del evento de streaming en el pipeline.
- **Atributos:** `date_id` (Integer YYYYMMDD, PK), `full_date`, `year`, `quarter`, `month`, `month_name`, `day`, `day_of_week` (ISO 8601).
- **Registro Cargar (Ejecución real):** 1 fila correspondiente a la fecha de emisión de la transmisión (`2026-11-07`, `date_id = 20261107`).

> **Distinción Temporal Crucial:** 
> - `dim_date` / `event_time`: Refiere al momento cronológico en que el evento de streaming fue publicado y procesado por la infraestructura.
> - `report_year`: Atributo contenido en `fact_predictions` que conserva el año histórico original del informe (2015–2019). **Todo análisis de evolución histórica debe realizarse obligatoriamente mediante `report_year`.**

#### Tabla de Hechos `fact_predictions`
- **Registros cargados:** 782 hechos analíticos.
- **Campos:** `prediction_id` (PK), `raw_event_id` (FK Unique a `raw_events`), `country_id` (FK), `date_id` (FK), `event_time`, `actual_score`, `predicted_score`, `prediction_error`, `prediction_timestamp`, `report_year`.

---

## 9. Validaciones de Integridad y Resultados Operacionales en PostgreSQL

### Resultados de la Ingesta Operacional
- **Mensajes totales leídos desde Kafka Broker:** 1,564
- **Registros insertados en `raw_events`:** 1,564
- **Eventos de negocio únicos procesados (`is_duplicate = FALSE`):** 782
- **Eventos duplicados detectados y marcados (`is_duplicate = TRUE`):** 782
- **Registros en `processed_events`:** 782
- **Registros en `predictions`:** 782

### Validaciones de Consistencia Dimensional
- **Registros en `fact_predictions`:** 782
- **Orfanos en `country_id` / `date_id` / `raw_event_id`:** 0
- **Inconsistencias en cálculo de error absoluto:** 0
- **Integridad Referencial Capa Raw-Analítica:** 100% verificada.

---

## 10. Resultados Agregados de Predicción

Sobre las 782 predicciones persistidas en el data warehouse operacional se obtuvieron las siguientes métricas agregadas:

- **Total de registros predichos:** 782
- **Cobertura geográfica:** 164 países
- **Promedio de Puntuación Real (`actual_score`):** 5.3790
- **Promedio de Puntuación Predicha (`predicted_score`):** 5.3801
- **MAE Descriptivo Global (Dataset Completo):** **0.2189**
- **Sesgo de Predicción Global (*Prediction Bias*):** +0.0011

> **Aclaración Metodológica Crítica:**
> El **MAE descriptivo de ~0.219** se calcula sobre la totalidad de los 782 registros almacenados, los cuales incluyen observaciones que participaron activamente en la fase de entrenamiento del modelo. Por ende, **no debe presentarse ni interpretarse como una métrica de generalización del modelo**.
> La métrica formal de evaluación sobre el conjunto de prueba independiente (235 registros, 30% holdout) para el modelo Random Forest es:
> - **MAE Test:** **0.408**
> - **MSE Test:** 0.272
> - **RMSE Test:** 0.522
> - **$R^2$ Test:** 0.782

---

## 11. Power BI — Integración y Dashboard Analítico

### Configuración de Conexión
- **Modo:** Direct Import
- **Fuente:** PostgreSQL Database (`localhost:5433`, DB: `happiness_streaming`)
- **Tablas importadas:** `public.fact_predictions`, `public.dim_country`, `public.dim_date`.

### Modelo de Datos (Esquema Estrella Activo)
- `dim_country[country_id]` `1` : `*` `fact_predictions[country_id]` (Filtro simple)
- `dim_date[date_id]` `1` : `*` `fact_predictions[date_id]` (Filtro simple)

### Medidas DAX Implementadas

```dax
Total Predictions = 
COUNTROWS('public fact_predictions')

Countries Analyzed = 
DISTINCTCOUNT('public fact_predictions'[country_id])

Average Actual Score = 
AVERAGE('public fact_predictions'[actual_score])

Average Predicted Score = 
AVERAGE('public fact_predictions'[predicted_score])

Average MAE = 
AVERAGE('public fact_predictions'[prediction_error])

Prediction Bias = 
AVERAGEX(
    'public fact_predictions',
    'public fact_predictions'[predicted_score] - 'public fact_predictions'[actual_score]
)

RMSE = 
SQRT(
    AVERAGEX(
        'public fact_predictions',
        POWER('public fact_predictions'[actual_score] - 'public fact_predictions'[predicted_score], 2)
    )
)

Error Bin = 
FLOOR(fact_predictions[prediction_error], 0.1)

Frequency = 
COUNTROWS(fact_predictions)
```

### Dashboard: "Model Performance Overview"
Visión de las 9 componentes/visualizaciones estructuradas:
1. **KPI Cards:** `Total Predictions` (782), `Countries Analyzed` (164), `Average MAE` (0.219), `Average Actual Score` (5.379).
2. **Actual vs Predicted Score (Scatter Plot):** Eje X = `actual_score`, Eje Y = `predicted_score`, Detalle = `prediction_id`, Tooltip = `prediction_error`. Muestra alineación estrecha a la diagonal ideal de $45^\circ$.
3. **Average MAE by Country (Horizontal Bar Chart):** Variación del error medio por país.
4. **Actual vs Predicted by Report Year (Line Chart):** Promedios anuales históricos alineados entre 5.35 y 5.41.
5. **Distribution of Prediction Errors (Column Chart):** Histograma de frecuencia mediante `Error Bin` (amplitud 0.1). Muestra alta concentración en el intervalo $[0.0 - 0.3]$.
6. **Average Prediction Error by Report Year:** Evolución del MAE entre 0.200 (2018) y 0.243 (2015).
7. **Top 10 Countries with Highest Prediction Error:** Identificación de países atípicos (Ej. Botswana MAE ~1.04, Tanzania MAE ~0.88).
8. **Prediction Bias by Report Year:** Muestra subestimación promedio en 2015 y 2016 (Sesgo -0.078 en 2016) y sobreestimación leve entre 2017 y 2019 (Sesgo +0.057 en 2017).
9. **Predictions by Report Year:** Volumen de predicciones distribuidas uniformemente por año histórico.

---

## 12. Estructura del Proyecto

```text
Workshop3/
├── data/
│   ├── raw/
│   │   ├── 2015.csv
│   │   ├── 2016.csv
│   │   ├── 2017.csv
│   │   ├── 2018.csv
│   │   └── 2019.csv
│   └── processed/
│       └── world_happiness_2015_2019.csv
├── models/
│   ├── happiness_regression_pipeline.joblib
│   └── model.pkl
├── src/
│   └── streaming/
│       ├── producer.py
│       ├── consumer.py
│       ├── event_contract.py
│       └── database.py
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

## 13. Configuración y Guía de Ejecución

### Requisitos Previos
- Python 3.10+
- Docker Desktop y Docker Compose
- Power BI Desktop

### 1. Entorno Python
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### 2. Despliegue de Infraestructura Contenerizada
```bash
docker compose up -d
docker compose ps
```

### 3. Configuración de Base de Datos
- **Host:** `127.0.0.1`
- **Port:** `5433`
- **Database:** `happiness_streaming`
- **User:** `happiness_user`
- *(Las credenciales de acceso deben ser configuradas mediante variables de entorno local o archivo `.env` no subido al repositorio).*

### 4. Ejecución del Consumer (ML Streaming Consumer)
En una terminal activa:
```bash
python src/streaming/consumer.py
```

### 5. Ejecución del Producer (Kafka Transmitter)
En una segunda terminal:
```bash
python src/streaming/producer.py
```

---

## 14. Compendio de Decisiones Técnicas y Justificaciones

1. **Preservación de CSVs Raw:** Garantiza auditabilidad y trazabilidad completa del origen sin corrupción de datos.
2. **Esquema Canónico:** Resuelve la heterogeneidad de nombres entre las 5 ediciones anuales del reporte.
3. **Mapeo `Family` → `social_support`:** Homologación conceptual justificada en documentación oficial del reporte, advirtiendo posibles cambios metodológicos menores.
4. **No Imputación en Capa Canónica:** Los nulos y ceros legítimos se conservan hasta la etapa de ML para evitar data leakage en la fuente.
5. **Exclusión de `happiness_rank` y `Dystopia Residual`:** Eliminación estricta de riesgo de Target Leakage.
6. **Mantenimiento de `generosity`:** Conservada por fundamentación teórica socioeconómica a pesar de su baja correlación bivariada.
7. **Imputación/Escalado dentro de Scikit-Learn Pipeline:** Evita de forma rigurosa la contaminación del conjunto de test (*Data Leakage*).
8. **Random Forest sobre Linear Regression:** Selección fundamentada empíricamente al reducir el MAE de 0.449 a 0.408 y elevar el $R^2$ a 0.782.
9. **Contrato Estricto de Eventos (`event_contract.py`):** Desacopla la infraestructura de mensajería del modelo, filtrando mensajes malformados.
10. **Persistencia Raw Inmediata (`raw_events`):** Mantiene la trazabilidad y los bytes originales de mensajes Kafka (`BYTEA`) incluso ante fallos de contrato o inferencia.
11. **Deduplicación por `event_id` en Consumidor:** Garantiza la idempotencia de negocio sin descartar la auditoría de mensajería redundante de Kafka.
12. **Deshabilitación de Auto-Commit en Kafka Consumer:** Garantiza procesamiento *At-Least-Once* coordinado transaccionalmente con PostgreSQL.
13. **Distinción entre `event_time` y `report_year`:** Previene la confusión entre el tiempo de transmisión de streaming y la dimensión temporal histórica de los datos.
14. **Modelo Dimensional Estrella para Power BI:** Optimiza la velocidad de consulta en modo Direct Import y simplifica el filtrado analítico.

---

## 15. Limitaciones Conocidas

- **Partición Aleatoria en ML:** Al dividir aleatoriamente registros, un mismo país puede estar presente en train y test (en distintos años), por lo que la métrica de evaluación no mide generalización pura hacia países totalmente no observados o predicciones hacia el futuro (*Time-Split*).
- **Inexistencia de Pruebas de Normalidad Formales:** Los diagnósticos de residuos se basaron en análisis visuales de dispersión e histogramas.
- **Métrica MAE del Dashboard:** El valor descriptivo de 0.219 no representa el error de prueba fuera de muestra (*out-of-sample*), el cual es formalmente 0.408.
- **Fecha Única en Streaming:** La dimensión `dim_date` contiene una única fecha derivada del momento de ejecución de las pruebas.
- **Multicolinealidad:** Presencia de alta correlación entre `gdp` y `health` (0.784) no tratada mediante reducción de dimensionalidad para priorizar la interpretabilidad de las variables.

---

## 16. Trabajo Futuro y Mejoras Propuestas

- **Validación Cruzada Temporal (*TimeSeriesSplit*):** Evaluar la capacidad de pronóstico entrenando con años $t$ y probando exclusivamente sobre el año $t+1$.
- **GroupKFold Cross-Validation:** Evaluar generalización aislando grupos de países completos en el conjunto de prueba.
- **MLflow Tracking & Registry:** Incorporar un servidor de MLflow para la gestión del ciclo de vida, versionado y gobernanza de artefactos `.pkl`.
- **Monitoreo de Data Drift y Concept Drift:** Implementar herramientas como Evidently AI o Great Expectations en el consumidor Kafka para detectar desviaciones en la distribución de las variables de entrada.
- **Orquestación en Kubernetes / Helm:** Escalar los consumidores Kafka dentro de un Consumer Group administrado horizontalmente con KEDA (*Kubernetes Event-driven Autoscaling*).
