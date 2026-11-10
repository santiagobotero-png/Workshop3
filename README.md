# WORKSHOP_3: Streaming ETL with Apache Kafka and Machine Learning

## 1. General Project Context

**WORKSHOP_3** is an advanced data engineering academic and industrial project that implements a hybrid ETL pipeline (Batch & Real-time Streaming), combining **Machine Learning** inference during stream processing, **Apache Kafka**, operational persistence in **PostgreSQL**, star-schema dimensional modeling, and an interactive dashboard in **Power BI**.

The solution uses historical data from the *World Happiness Report* for the 5-year period **2015–2019** (782 total records) to predict the target indicator `happiness_score` based on six main socioeconomic variables.

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                                  SOLUTION ARCHITECTURE                                  │
└─────────────────────────────────────────────────────────────────────────────────────────┘

 ┌───────────────────────────────────────────────────────────────────────────────────────┐
 │                                   1. BATCH PROCESSING                                 │
 │                                                                                       │
 │ ┌───────────────┐     ┌──────────────────────┐     ┌────────────────────────────────┐ │
 │ │  Raw Data     │ ──> │ Cleaning &           │ ──> │ Model Training & Comparison    │ │
 │ │ (2015-2019)   │     │ Harmonization (EDA)  │     │ (Linear, Tree, Random Forest)  │ │
 │ └───────────────┘     └──────────────────────┘     └───────────────┬────────────────┘ │
 └────────────────────────────────────────────────────────────────────┼──────────────────┘
                                                                      │ Serialized Model
                                                                      ▼ (models/model.pkl)
 ┌───────────────────────────────────────────────────────────────────────────────────────┐
 │                                2. STREAMING PROCESSING                                │
 │                                                                                       │
 │ ┌───────────────┐  Producer   ┌───────────────────┐  Consumer  ┌────────────────────┐ │
 │ │ Consolidated  │ ──────────> │ Apache Kafka      │ ─────────> │ Python ML Consumer │ │
 │ │ Dataset (CSV) │  acks=all   │ topic:            │  No Auto-  │ Contract Valid.    │ │
 │ └───────────────┘             │ happiness-events  │  Commit    │ Model Inference    │ │
 │                               └───────────────────┘            └───────────┬────────┘ │
 └────────────────────────────────────────────────────────────────────────────┼──────────┘
                                                                              │
 ┌────────────────────────────────────────────────────────────────────────────┼──────────┐
 │                                3. PERSISTENCE AND ANALYTICS                │          │
 │                                                                            ▼          │
 │  ┌──────────────────────────────────────────────────────────────────────────────────┐ │
 │  │                              PostgreSQL (happiness_streaming)                    │ │
 │  │                                                                                  │ │
 │  │  [ Operational Layer ]                                                           │ │
 │  │  ├── raw_events (BYTEA payload, deduplication control)                           │ │
 │  │  ├── processed_events (status, model version, timestamps)                         │ │
 │  │  └── predictions (predicted_score, actual_score, absolute_error)                 │ │
 │  │                                                                                  │ │
 │  │  [ Analytical Layer - Star Schema ]                                              │ │
 │  │  ├── dim_country (164 countries)                                                 │ │
 │  │  ├── dim_date (event emission date attributes)                                   │ │
 │  │  └── fact_predictions (782 facts with historical report_year)                    │ │
 │  └──────────────────────────────────────────┬───────────────────────────────────────┘ │
 │                                             │ Direct Import Connection                │
 │                                             ▼                                         │
 │  ┌──────────────────────────────────────────────────────────────────────────────────┐ │
 │  │                         Power BI (Model Performance Overview)                    │ │
 │  └──────────────────────────────────────────────────────────────────────────────────┘ │
 └───────────────────────────────────────────────────────────────────────────────────────┘
```

### Relationship Between Batch and Streaming Modalities

* **Batch Processing:** Dedicated to data collection, cleaning, and harmonization of the 5 heterogeneous datasets, feature engineering, and offline training/evaluation of regression models. It concludes with the export of the trained ML artifact (`model.pkl`).

* **Streaming Processing:** Dedicated to asynchronous, decoupled event transport via Apache Kafka, strict data contract validation, real-time inference upon message consumption, operational idempotency/deduplication control, and structured persistence.

---

## 2. Phase A: Exploratory Data Analysis, Cleaning, and Preparation (EDA)

### Source Datasets

Original files location: `data/raw/`

* `2015.csv`: 158 records, 12 columns.
* `2016.csv`: 157 records, 13 columns.
* `2017.csv`: 155 records, 12 columns.
* `2018.csv`: 156 records, 9 columns.
* `2019.csv`: 156 records, 9 columns.
* **Cumulative total:** 782 records.

### Consolidated Dataset

Location: `data/processed/world_happiness_2015_2019.csv` (782 records, 10 columns, 5 years).

### Canonical Schema

| Column | Data Type | Description |
| ----- | ----- | ----- |
| `year` | Integer | Historical publication year of the report |
| `country` | Text | Normalized country name |
| `happiness_rank` | Integer | Country rank in the annual report (derived) |
| `happiness_score` | Float | Target score for happiness |
| `gdp` | Float | Gross Domestic Product per capita (normalized) |
| `social_support` | Float | Social support (unification of Family / Social Support) |
| `health` | Float | Healthy life expectancy |
| `freedom` | Float | Freedom to make life choices |
| `corruption` | Float | Perceptions of corruption (1 known null in UAE 2018) |
| `generosity` | Float | Generosity indicator |

### Approximate Annual Bivariate Correlations with `happiness_score`

> *Note: These metrics correspond to bivariate statistical associations and do not imply causality.*

| Indicator | 2015 | 2016 | 2017 | 2018 | 2019 |
| ----- | ----- | ----- | ----- | ----- | ----- |
| **GDP** | 0.781 | 0.790 | 0.812 | 0.802 | 0.794 |
| **Health** | 0.724 | 0.765 | 0.782 | 0.776 | 0.780 |
| **Social Support** | 0.741 | 0.739 | 0.753 | 0.746 | 0.777 |
| **Freedom** | 0.568 | 0.567 | 0.570 | 0.544 | 0.567 |
| **Corruption** | 0.395 | 0.402 | 0.429 | 0.405 | 0.386 |
| **Generosity** | 0.180 | 0.157 | 0.155 | 0.136 | 0.076 |

### Methodological Cleaning and Harmonization Decisions

1. **Column Mapping:** Unification of evolving variables (`Family` in 2015–2016 and `Social support` in 2017–2019 mapped to `social_support`).
2. **Geographical Normalization:** Standardization of country names to prevent fragmentation (`Somaliland region` → `Somaliland Region`, `Macedonia` → `North Macedonia`, `Hong Kong S.A.R., China` → `Hong Kong`, `Taiwan Province of China` → `Taiwan`, `Trinidad & Tobago` → `Trinidad and Tobago`, `North Cyprus / Northern Cyprus` → `Northern Cyprus`, `Swaziland` → `Eswatini`).
3. **Preservation of Outliers and Missing Values:** Retained the single null value in `corruption` for UAE (2018) and the 5 zero values in `health`, as there was insufficient evidence of measurement error.
4. **No Destructive Imputation in Raw Data:** Neither rows were dropped nor imputation applied to the canonical layer to preserve full auditability.
5. **Longitudinal Panel:** Identified 141 countries present across all 5 years ($141 \times 5 = 705$ observations) for specific longitudinal analysis, while keeping 782 records in the general processed dataset.

### Variables Excluded from Modeling

* `happiness_rank`: Excluded because it is directly derived from the target variable (prevention of Target Leakage).
* `Dystopia Residual`: Excluded due to its mechanical relation to the construction of the target score.
* `country`, `year`, `region`: Excluded as predictors in the initial numeric baseline model to avoid premature bias or categorical encoding.

---

## 3. Feature Engineering and Baseline Model

### Problem Formulation

Supervised multivariable regression:

* **Predictors (**$X$**):** `gdp`, `social_support`, `health`, `freedom`, `corruption`, `generosity`.
* **Target (**$y$**):** `happiness_score`.

### Descriptive Statistics of Predictors

* **GDP:** Mean = 0.916, Std = 0.407, Median = 0.982, Max = 2.096
* **Social Support:** Mean = 1.078, Std = 0.330, Median = 1.125, Max = 1.644
* **Health:** Mean = 0.612, Std = 0.248, Median = 0.647, Max = 1.141
* **Freedom:** Mean = 0.411, Std = 0.153, Median = 0.431, Max = 0.724
* **Corruption:** Mean = 0.125, Std = 0.106, Median = 0.091, Max = 0.552 (781 valid, 1 null)
* **Generosity:** Mean = 0.219, Std = 0.122, Median = 0.202, Max = 0.838
* **Happiness Score (Target):** Mean = 5.379, Std = 1.127, Min = 2.693, Median = 5.322, Max = 7.769

*Global GDP-Health correlation: 0.784. Global GDP-Happiness correlation: 0.789.*

### Baseline Model (Linear Regression Pipeline)

* **Split:** 80% Training (625 records), 20% Testing (157 records), `random_state=42`, `shuffle=True`. In-memory rounding to 3 decimals.
* **Pipeline:** `SimpleImputer(strategy='median')` → `StandardScaler()` → `LinearRegression()`.
* **Learned Parameters on Train:**
  * Imputer Medians: GDP (0.979), Social Support (1.125), Health (0.644), Freedom (0.433), Corruption (0.091), Generosity (0.204).
  * Scaler Means: GDP (0.911), Social Support (1.080), Health (0.609), Freedom (0.413), Corruption (0.127), Generosity (0.222).
  * Standardized Coefficients: GDP (0.438), Social Support (0.217), Health (0.256), Freedom (0.225), Corruption (0.128), Generosity (0.059). Intercept: 5.377.

#### Baseline Metrics (Test Set 20% - 157 records)

* **MAE:** 0.441
* **MSE:** 0.329
* **RMSE:** 0.574
* $R^2$**:** 0.729
* **Residual Analysis (**$Residual = Actual - Predicted$**):** Mean = -0.001, Std = 0.576, Min = -1.767, Median = 0.014, Max = 1.434.
* **Exported Artifact:** `models/happiness_regression_pipeline.joblib`.

---

## 4. Phase B: Model Comparison and Final Selection

A second comparative evaluation experiment was performed using a clean **70% Training (547 records)** and **30% Test (235 records)** split with `random_state=42`.

### Model Evaluation Results

| Model | MAE | MSE | RMSE | $R^2$ |
| ----- | ----- | ----- | ----- | ----- |
| **Linear Regression** | 0.449 | 0.341 | 0.584 | 0.727 |
| **Decision Tree Regressor** | 0.582 | 0.578 | 0.760 | 0.537 |
| **Random Forest Regressor** | **0.408** | **0.272** | **0.522** | **0.782** |

> **Methodological Clarification:** The Baseline (80/20) and Comparative (70/30) metrics stem from different split ratios and cannot be directly compared against one another.

### Final Model Selection and Configuration

**Random Forest Regressor** was selected due to its superior generalization capabilities and lower test error rates.

* **Hyperparameters:** `n_estimators=200`, `random_state=42`, `n_jobs=-1`.
* **Preprocessing:** Integrated median imputation inside the pipeline.
* **Exported Artifact:** `models/model.pkl` (\~9.73 MB).
* **Serialization Validation:** Verified via `joblib.load()`, achieving exact match ($Delta = 0.000000$) against the in-memory predictions.

---

## 5. Phase B: Apache Kafka Architecture and Streaming Pipeline

### Containerized Infrastructure (`docker-compose.yml`)

* **Kafka Broker:** Image `apache/kafka:4.4.0`, Container: `happiness-streaming-broker`, Host Port: `9092`.
* **PostgreSQL DB:** Image `postgres:18`, Container: `happiness-streaming-db`, Host Port: `5433` (internal `5432`). Database: `happiness_streaming`.

### Kafka Topic Configuration

* **Name:** `happiness-events`
* **Partitions:** 3
* **Replication Factor:** 1
* **Message Key:** `country` (Ensures messages for the same country land in the same partition in order).
* **Observed Partition Distribution:**
  * Partition 0: 654 messages
  * Partition 1: 440 messages
  * Partition 2: 470 messages
  * **Total:** 1,564 messages (2 full producer runs).

### Kafka Producer (`src/streaming/producer.py`)

* Reads the consolidated dataset (`world_happiness_2015_2019.csv`).
* Builds deterministic events with pattern: `HAPPINESS-{year}-{COUNTRY_UPPER}`.
* Adds UTC timestamp (`event_time`) in strict ISO-8601 format (`Z` suffix).
* Fault tolerance config: `acks=all`, `retries=5`, `enable_idempotence=True`.
* Transmission delay: `STREAM_DELAY_SECONDS=0.5`.
* **Production Metrics:** 2 complete runs executed. $782 \times 2 = 1,564$ events transmitted, 0 network errors, 0 broker rejections.

---

## 6. Event Contract (`src/streaming/event_contract.py`)

To decouple the Kafka broker from model logic, a strict contract is enforced prior to model execution.

### Contract Schema

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

### Validation Rules

1. Mandatory presence of all `REQUIRED_FIELDS`.
2. Rejection of unexpected fields (strict schema enforcement).
3. Validation of numeric types (`float`/`int`). Null values allowed exclusively for `corruption`.
4. `year` must strictly belong to `ALLOWED_YEARS`.
5. `event_time` must be a valid ISO-8601 UTC string ending in `Z`.
6. `actual_happiness_score` is required solely for offline evaluation/logging and is **never** passed into `MODEL_FEATURES`.

---

## 7. Kafka Consumer and Real-time Inference

### Consumer (`src/streaming/consumer.py`)

* **Group ID:** `happiness-ml-team`
* **Offset Strategy:** `enable_auto_commit=False`, `auto_offset_reset='earliest'`, `max_poll_records=1`.
* Loads `models/model.pkl` once during initialization.

```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                               CONSUMER PROCESSING FLOW                                 │
└────────────────────────────────────────────────────────────────────────────────────────┘

 [ Kafka Message ] ──> 1. Immediate Raw Persistence (raw_events BYTEA)
                             │
                             ▼
                       2. Contract Validation
                             │
            ┌────────────────┴────────────────┐
            │ VALID                           │ INVALID
            ▼                                 ▼
      3. Deduplication Check (event_id)    Record INVALID_* Status in raw_events
            │
      ┌─────┴────────────────┐
      │ UNIQUE               │ DUPLICATE
      ▼                      ▼
  4. ML Inference       Mark is_duplicate=TRUE in raw_events
     (Random Forest)         │
      │                      └─> End Event Processing
      ▼
  5. Store Prediction (predictions)
      │
      ▼
  6. Transaction Complete -> Manual Offset Commit
```

### Raw Persistence and Deduplication Mechanism

* **Immediate Raw Persistence:** The message is saved to `raw_events` before contract validation or inference, ensuring full auditability.
* **Business Deduplication:** Evaluates `event_id`. If previously processed successfully, the payload is kept in `raw_events` with `is_duplicate = TRUE` while bypassing re-inference.
* **Transactional Offset Commit:** The Kafka offset is committed (`commit()`) only after the PostgreSQL transaction successfully succeeds.

---

## 8. Phase C: Operational Persistence and Analytical Dimensional Model

PostgreSQL Database: `happiness_streaming`.

### 8.1 Operational Layer

#### Table `raw_events` (Functional role of `raw_happiness_events`)

* **Purpose:** Idempotent raw audit table for all broker messages.
* **Fields:** `raw_event_id` (PK), `kafka_topic`, `kafka_partition`, `kafka_offset`, `kafka_key`, `raw_payload` (BYTEA), `event_id`, `status`, `is_duplicate`, `error_message`, `received_at`.
* **Integrity Constraint:** `CONSTRAINT unique_kafka_message UNIQUE (kafka_topic, kafka_partition, kafka_offset)`.
* **Processing Statuses:** `RECEIVED`, `VALID`, `INVALID_SCHEMA`, `INVALID_VALUES`, `PREDICTION_ERROR`.

#### Table `processed_events`

* **Purpose:** Lifecycle management of business events proceeding to inference.
* **Fields:** `event_id` (PK), `raw_event_id` (FK to `raw_events`, Unique), `processing_status`, `model_version`, `processed_at`, `completed_at`.
* **Statuses:** `PROCESSING`, `PREDICTED`, `PREDICTION_ERROR`.

#### Table `predictions`

* **Purpose:** Quantitative store of ML inference results.
* **Fields:** `prediction_id` (PK), `event_id` (FK to `processed_events`, Unique), `predicted_score`, `actual_score`, `absolute_error` (`ABS(actual_score - predicted_score)`), `model_version`, `predicted_at`.

### 8.2 Analytical Layer — Star Schema Model

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

#### Dimension `dim_country`

* **Records:** 164 unique countries loaded.
* **Attributes:** `country_id` (PK), `country_name` (Unique).

#### Dimension `dim_date`

* **Purpose:** Represents the streaming event emission timestamp.
* **Attributes:** `date_id` (Integer YYYYMMDD, PK), `full_date`, `year`, `quarter`, `month`, `month_name`, `day`, `day_of_week` (ISO 8601).
* **Loaded Records:** 1 row corresponding to the test run emission date (`2026-11-07`, `date_id = 20261107`).

> **Crucial Temporal Distinction:**
>
> * `dim_date` / `event_time`: Refers to the moment the streaming event was transmitted and processed by the system.
> * `report_year`: Attribute inside `fact_predictions` retaining the original historical report year (2015–2019). **Historical trend analysis must be conducted using `report_year`.**

#### Fact Table `fact_predictions`

* **Loaded Records:** 782 analytical facts.
* **Fields:** `prediction_id` (PK), `raw_event_id` (FK Unique to `raw_events`), `country_id` (FK), `date_id` (FK), `event_time`, `actual_score`, `predicted_score`, `prediction_error`, `prediction_timestamp`, `report_year`.

---

## 9. Integrity Validations and Operational Results in PostgreSQL

### Operational Ingestion Results

* **Total Kafka broker messages read:** 1,564
* **Inserted into `raw_events`:** 1,564
* **Unique business events processed (`is_duplicate = FALSE`):** 782
* **Duplicate events flagged (`is_duplicate = TRUE`):** 782
* **Records in `processed_events`:** 782
* **Records in `predictions`:** 782

### Dimensional Consistency Validations

* **Records in `fact_predictions`:** 782
* **Orphan records (`country_id` / `date_id` / `raw_event_id`):** 0
* **Absolute error calculation mismatches:** 0
* **Raw-to-Analytical Referencial Integrity:** 100% verified.

---

## 10. Aggregated Prediction Results

Across all 782 predictions persisted in PostgreSQL, the following metrics were calculated:

* **Total Predicted Records:** 782
* **Geographical Coverage:** 164 countries
* **Average Actual Score:** 5.3790
* **Average Predicted Score:** 5.3801
* **Global Descriptive MAE (Stored Dataset):** **0.2189**
* **Global Prediction Bias:** +0.0011

> **Critical Methodological Clarification:**
> The **descriptive MAE of \~0.219** is calculated over all 782 stored records, which include observations that were part of the training split. Therefore, **it must not be used as an out-of-sample generalization metric**.
> The official test set metric for the Random Forest model (235 records, 30% holdout) is:
>
> * **Test MAE:** **0.408**
> * **Test MSE:** 0.272
>
> * **Test RMSE:** 0.522
> * **Test** $R^2$**:** 0.782

---

## 11. Power BI — Analytics Integration & Dashboard

### Connection Setup

* **Mode:** Direct Import
* **Source:** PostgreSQL Database (`localhost:5433`, DB: `happiness_streaming`)
* **Tables:** `public.fact_predictions`, `public.dim_country`, `public.dim_date`.

### Data Model (Active Star Schema)

* `dim_country[country_id]` `1` : `*` `fact_predictions[country_id]` (Single filter direction)
* `dim_date[date_id]` `1` : `*` `fact_predictions[date_id]` (Single filter direction)

### DAX Measures

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

Structure of the 9 visualizations/components:

1. **KPI Cards:** `Total Predictions` (782), `Countries Analyzed` (164), `Average MAE` (0.219), `Average Actual Score` (5.379).
2. **Actual vs Predicted Score (Scatter Plot):** X-axis = `actual_score`, Y-axis = `predicted_score`, Detail = `prediction_id`, Tooltip = `prediction_error`. Shows tight alignment to the ideal $45^\circ$ line.
3. **Average MAE by Country (Horizontal Bar Chart):** Mean error variation per country.
4. **Actual vs Predicted by Report Year (Line Chart):** Annual historical averages tracking between 5.35 and 5.41.
5. **Distribution of Prediction Errors (Column Chart):** Frequency histogram built via `Error Bin` (0.1 step size). High concentration in $[0.0 - 0.3]$.
6. **Average Prediction Error by Report Year:** MAE trend ranging between 0.200 (2018) and 0.243 (2015).
7. **Top 10 Countries with Highest Prediction Error:** Highlights specific outliers (e.g., Botswana MAE \~1.04, Tanzania MAE \~0.88).
8. **Prediction Bias by Report Year:** Demonstrates slight underestimation in 2015–2016 (Bias -0.078 in 2016) and slight overestimation in 2017–2019 (Bias +0.057 in 2017).
9. **Predictions by Report Year:** Uniform historical record volume per year.

---

## 12. Repository Structure

```
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

## 13. Setup and Execution Guide

### Prerequisites

* Python 3.10+
* Docker Desktop & Docker Compose
* Power BI Desktop

### 1. Python Environment

```bash
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### 2. Infrastructure Deployment

```bash
docker compose up -d
docker compose ps
```

### 3. Database Configuration

* **Host:** `127.0.0.1`
* **Port:** `5433`
* **Database:** `happiness_streaming`
* **User:** `happiness_user`
* *(Credentials should be loaded via a local `.env` file).*

### 4. Run Consumer (ML Streaming Consumer)

In a dedicated terminal:

```bash
python src/streaming/consumer.py
```

### 5. Run Producer (Kafka Event Generator)

In a second terminal:

```bash
python src/streaming/producer.py
```

---

## 14. Justification of Technical Decisions

1. **Preservation of Raw CSVs:** Ensures total auditability and raw data integrity without altering sources.
2. **Canonical Schema:** Standardizes structural differences across the 5 annual dataset editions.
3. **`Family` → `social_support` Mapping:** Conceptually justified by official report notes while explicitly acknowledging minor methodology changes across years.
4. **Deferred Imputation:** Retains raw zeros and nulls in data files to prevent data leakage prior to the ML pipeline.
5. **Target Leakage Prevention:** Exclusion of `happiness_rank` and `Dystopia Residual`.
6. **Retention of `generosity`:** Maintained based on socioeconomic domain knowledge despite low bivariate correlation.
7. **Pipeline Preprocessing (Scikit-Learn):** Prevents test set contamination (*Data Leakage*) by embedding fit transformations strictly inside training.
8. **Random Forest Selection:** Empirical choice after reducing test MAE from 0.449 to 0.408 and improving $R^2$ to 0.782.
9. **Strict Event Contract (`event_contract.py`):** Decouples transport infrastructure from inference logic, rejecting malformed events.
10. **Immediate Raw Persistence (`raw_events`):** Stores raw message bytes (`BYTEA`) prior to validation/inference for audit safety.
11. **Business Deduplication by `event_id`:** Provides application idempotency while keeping raw messaging audit logs intact.
12. **Manual Offset Commit:** Guarantees *At-Least-Once* processing aligned transaccionality with PostgreSQL writes.
13. **`event_time` vs `report_year` Separation:** Avoids confusing real-time message metadata with historical data properties.
14. **Star Schema Model:** Optimizes Direct Import performance in Power BI and simplifies analytical slicing.

---

## 15. Known Limitations

* **Random Split Strategy:** Random sampling allows the same country to exist across both train and test splits (in different years), meaning test metrics do not measure true out-of-sample generalization to completely unseen countries or unseen future years (*Time-Split*).
* **Visual Residual Diagnostics:** Normality and homoscedasticity were verified visually rather than through formal statistical tests.
* **Dashboard MAE Metric:** The 0.219 descriptive MAE on the dashboard reflects the full persisted set and does not equal out-of-sample test MAE (0.408).
* **Single Streaming Date:** `dim_date` populated with 1 single date derived from the execution run.
* **Multicollinearity:** High correlation between `gdp` and `health` (0.784) was retained without dimensionality reduction to keep interpretability straightforward.

---

## 16. Future Improvements

* **Temporal Cross-Validation (*TimeSeriesSplit*):** Evaluate model capabilities by training on years $t$ and testing strictly on year $t+1$.
* **GroupKFold Cross-Validation:** Test generalization by completely withholding groups of countries during training.
* **MLflow Tracking & Registry:** Integrate an MLflow server to manage model versioning, metrics, and artifact lifecycle.
* **Data Drift Monitoring:** Implement Evidently AI or Great Expectations inside the Kafka consumer to track input distribution shifts.
* **Kubernetes / Helm Deployment:** Scale consumer groups horizontally on Kubernetes using KEDA (*Kubernetes Event-driven Autoscaling*).
