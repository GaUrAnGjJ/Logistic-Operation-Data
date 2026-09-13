# Logistics Data Engineering Platform — Implementation Plan

> **Project Type:** End-to-End Data Engineering Portfolio Project
> **Stack:** Python · PySpark · Docker · GCS · BigQuery · Power BI · SQL · Git/GitHub
> **Status:** Planning Phase
> **Data Profiling:** Complete — See DATA_DICTIONARY.md

---

## Table of Contents

1. [Project Architecture Overview](#1-project-architecture-overview)
2. [Repository Structure](#2-repository-structure)
3. [Technology Stack and Tooling](#3-technology-stack-and-tooling)
4. [Phase 0 — Environment Setup](#4-phase-0--environment-setup)
5. [Phase 1 — Data Understanding (Completed)](#5-phase-1--data-understanding-completed)
6. [Phase 2 — Batch Simulation and GCS Raw Zone](#6-phase-2--batch-simulation-and-gcs-raw-zone)
7. [Phase 3 — Bronze Layer](#7-phase-3--bronze-layer)
8. [Phase 4 — Data Quality Framework](#8-phase-4--data-quality-framework)
9. [Phase 5 — Silver Layer](#9-phase-5--silver-layer)
10. [Phase 6 — Quarantine and Rejection Log](#10-phase-6--quarantine-and-rejection-log)
11. [Phase 7 — Gold Staging Layer](#11-phase-7--gold-staging-layer)
12. [Phase 8 — Gold Validation and Safe Publishing](#12-phase-8--gold-validation-and-safe-publishing)
13. [Phase 9 — BigQuery Load and Optimization](#13-phase-9--bigquery-load-and-optimization)
14. [Phase 10 — Audit Framework](#14-phase-10--audit-framework)
15. [Phase 11 — Notification System](#15-phase-11--notification-system)
16. [Phase 12 — Power BI Dashboards](#16-phase-12--power-bi-dashboards)
17. [Phase 13 — Pipeline Automation](#17-phase-13--pipeline-automation)
18. [Phase 14 — Testing](#18-phase-14--testing)
19. [Phase 15 — Performance Validation](#19-phase-15--performance-validation)
20. [Milestone Summary and Timeline](#20-milestone-summary-and-timeline)
21. [Data Quality Rules Registry](#21-data-quality-rules-registry)
22. [Star Schema Design](#22-star-schema-design)
23. [BigQuery Partitioning and Clustering Strategy](#23-bigquery-partitioning-and-clustering-strategy)
24. [Key Design Decisions](#24-key-design-decisions)
25. [Resume and Portfolio Talking Points](#25-resume-and-portfolio-talking-points)

---

## 1. Project Architecture Overview

```
  CSV Files (14 datasets)
         |
         v
  +---------------------------------------------+
  |       GOOGLE CLOUD STORAGE                  |
  |  raw/ -> bronze/ -> silver/ -> gold_staging/|
  |                    quarantine/  manifests/  |
  +---------------------------------------------+
         |
         v
  +----------------------+
  |   PYSPARK (Docker)   |<---- DQ Framework
  +----------------------+
         |
    +----+-----+
    |          |
  VALID    INVALID
    |          |
    v          v
  Silver   Quarantine
    |          |
    v          v
  Gold     rejection_log
  Staging      |
    |          v
    v      pipeline_audit
  Validate
  +--+--+
PASS   FAIL
  |      |
  v      v
Publish  Skip (Gold UNCHANGED)
  |
  v
BigQuery
  |
  +--------------------+
  |                    |
  v                    v
Power BI          Power BI
Business          DQ Dashboard
Dashboard
```

---

## 2. Repository Structure

```
logistics-data-platform/
+-- README.md
+-- IMPLEMENTATION_PLAN.md
+-- DATA_DICTIONARY.md
+-- config/
|   +-- pipeline_config.yaml
|   +-- schema_definitions/
|   |   +-- bronze_schemas.py
|   |   +-- silver_schemas.py
|   |   +-- gold_schemas.py
|   +-- dq_rules/
|       +-- completeness_rules.yaml
|       +-- validity_rules.yaml
|       +-- range_rules.yaml
|       +-- uniqueness_rules.yaml
|       +-- referential_integrity_rules.yaml
+-- data/
|   +-- raw/                     # Original CSVs (immutable)
|   +-- batches/
|       +-- batch_001/
|       +-- batch_002/
|       +-- batch_003/
+-- src/
|   +-- ingestion/
|   |   +-- batch_splitter.py
|   |   +-- batch_controller.py
|   |   +-- gcs_uploader.py
|   |   +-- bronze_writer.py
|   +-- quality/
|   |   +-- dq_engine.py
|   |   +-- rules/
|   |   |   +-- completeness.py
|   |   |   +-- validity.py
|   |   |   +-- range_check.py
|   |   |   +-- uniqueness.py
|   |   |   +-- referential.py
|   |   +-- table_rules/
|   |       +-- loads_rules.py, trips_rules.py, ... (14 files)
|   +-- silver/
|   |   +-- transformations/
|   |   |   +-- common.py
|   |   |   +-- drivers_transform.py
|   |   |   +-- ... (14 transform files)
|   |   +-- silver_writer.py
|   +-- gold/
|   |   +-- dimensions/
|   |   |   +-- dim_date.py, dim_driver.py, dim_truck.py
|   |   |   +-- dim_trailer.py, dim_customer.py
|   |   |   +-- dim_facility.py, dim_route.py
|   |   +-- facts/
|   |   |   +-- fact_load.py, fact_trip.py
|   |   |   +-- fact_fuel_purchase.py, fact_maintenance.py
|   |   |   +-- fact_delivery_event.py, fact_safety_incident.py
|   |   +-- gold_validator.py
|   |   +-- gold_publisher.py
|   +-- bigquery/
|   |   +-- bq_loader.py
|   |   +-- bq_merge.py
|   +-- audit/
|   |   +-- audit_logger.py
|   |   +-- rejection_logger.py
|   +-- notification/
|   |   +-- notifier.py
|   +-- pipeline/
|       +-- pipeline_runner.py
|       +-- scheduler.py
+-- sql/bigquery/
|   +-- create_dimensions.sql, create_facts.sql
|   +-- create_audit_tables.sql, create_rejection_log.sql
|   +-- merge_fact_*.sql, reporting_views.sql
+-- tests/
|   +-- unit/        (test_dq_*.py, test_bronze_writer.py, ...)
|   +-- integration/ (test_e2e.py, test_idempotency.py, ...)
|   +-- fixtures/sample_data/
+-- notebooks/
|   +-- 01_data_profiling.ipynb
|   +-- 02_dq_exploration.ipynb
|   +-- 03_performance_validation.ipynb
+-- docker/
|   +-- Dockerfile, docker-compose.yml
+-- scripts/
|   +-- setup_gcs.sh, setup_bigquery.sh
|   +-- simulate_batch.sh, run_tests.sh
+-- powerbi/
|   +-- business_dashboard.pbix
|   +-- dq_dashboard.pbix
+-- docs/
|   +-- architecture_diagram.png, star_schema_erd.png
|   +-- performance_benchmarks.md, portfolio_summary.md
+-- requirements.txt
```

---

## 3. Technology Stack and Tooling

| Layer | Tool | Purpose |
|:---|:---|:---|
| Local Compute | Docker + bitnami/spark:3.5 | Run PySpark locally without Dataproc cost |
| Raw Storage | Google Cloud Storage (GCS) | Immutable raw zone; all pipeline zones |
| Processing | PySpark 3.x | Ingestion, transformation, DQ, Gold build |
| Data Warehouse | BigQuery | Published Gold layer, audit, rejection log |
| Data Catalog | DATA_DICTIONARY.md | Human-readable source of truth (completed) |
| Orchestration | Python schedule library | Batch detection and pipeline trigger |
| Testing | pytest + pyspark local mode | Unit and integration tests |
| Notifications | Slack Incoming Webhook | Pipeline alerts (no SMTP server needed) |
| BI | Power BI Desktop + BigQuery connector | Business and DQ dashboards |
| Version Control | Git + GitHub | Full project history, branching strategy |
| Config | PyYAML + python-dotenv | Externalize all paths and credentials |

---

## 4. Phase 0 — Environment Setup

### 4.1 Local Docker Spark

```yaml
# docker-compose.yml
services:
  spark-master:
    image: bitnami/spark:3.5
    environment:
      - SPARK_MODE=master
    ports:
      - "8080:8080"
      - "7077:7077"
  spark-worker:
    image: bitnami/spark:3.5
    environment:
      - SPARK_MODE=worker
      - SPARK_MASTER_URL=spark://spark-master:7077
```

### 4.2 GCS + BigQuery Setup

```bash
gsutil mb -l us-central1 gs://logistics-data-platform
for zone in raw bronze silver gold_staging quarantine manifests; do
  gsutil mkdir gs://logistics-data-platform/$zone/
done
bq mk --dataset --location=US logistics_gold
bq mk --dataset --location=US logistics_audit
```

### 4.3 pipeline_config.yaml

```yaml
gcs:
  bucket: gs://logistics-data-platform
  zones:
    raw: raw/
    bronze: bronze/
    silver: silver/
    gold_staging: gold_staging/
    quarantine: quarantine/
    manifests: manifests/
bigquery:
  project: your-gcp-project-id
  gold_dataset: logistics_gold
  audit_dataset: logistics_audit
batch:
  control_table: logistics_audit.batch_control
  max_rejection_rate_pct: 20.0
pipeline:
  spark_master: local[*]
```

### 4.4 Deliverables

- [ ] Docker Compose PySpark cluster running locally
- [ ] GCS bucket with all zones created
- [ ] BigQuery datasets created
- [ ] config/pipeline_config.yaml committed to Git
- [ ] .env template with GCP credentials (.env in .gitignore)
- [ ] requirements.txt pinned with all dependencies

---

## 5. Phase 1 — Data Understanding (Completed)

> **Status: DONE** — Full profiling in DATA_DICTIONARY.md

### Key Findings from Data Profiling

| Finding | Impact on Pipeline |
|:---|:---|
| 0 duplicate rows across all 14 tables | No deduplication at Bronze; still validate new batches |
| 0 orphan FK keys in historical data | Data is well-formed but FK rules must enforce on new batches |
| drivers.termination_date NULL 82.67% | Business-expected NULL — active drivers have no termination date |
| trips.driver_id / truck_id / trailer_id ~2% NULL | Valid unallocated runs; do NOT reject |
| fuel_purchases.truck_id / driver_id ~2% NULL | Card swipes without scan — valid NULLs |
| truck_utilization_metrics.utilization_rate can exceed 1.0 | Overtime operation; 436 rows > 1.0 — flag, do NOT reject |
| delivery_events timestamps have microsecond precision | Cast to TIMESTAMP(microsecond) in Silver |
| All dates use YYYY-MM-DD or YYYY-MM-DD HH:MM:SS | Consistent; easy to parse in PySpark |
| drivers.date_of_birth range: 1960-1995 | Valid human birth years |

---

## 6. Phase 2 — Batch Simulation and GCS Raw Zone

### 6.1 Batch Splitting Strategy

Split transaction datasets by year into 3 batches:

```
Batch 001 -> 2022-01-01 to 2022-12-31  (Year 1 historical)
Batch 002 -> 2023-01-01 to 2023-12-31  (Year 2 historical)
Batch 003 -> 2024-01-01 to 2024-12-31  (Year 3 historical)

Dimension tables (drivers, trucks, trailers, customers, facilities, routes):
  Batch 001 -> Full dataset (initial load)
  Batch 002 -> Simulated new/updated records
  Batch 003 -> Further simulated updates
```

### 6.2 Batch Control Table

```sql
CREATE TABLE logistics_audit.batch_control (
    batch_id          STRING NOT NULL,
    source_file       STRING NOT NULL,
    table_name        STRING NOT NULL,
    gcs_raw_path      STRING,
    arrival_time      TIMESTAMP,
    processing_start  TIMESTAMP,
    processing_end    TIMESTAMP,
    records_in_file   INT64,
    status            STRING,   -- PENDING|PROCESSING|COMPLETED|FAILED|SKIPPED
    error_message     STRING
);
```

> **Idempotency Rule:** Before processing any batch, check batch_control for status=COMPLETED.
> If found, SKIP without re-processing.

### 6.3 GCS Upload Structure

```
gs://logistics-data-platform/raw/
  loads/  trips/  fuel_purchases/  maintenance_records/
  delivery_events/  safety_incidents/
  drivers/  trucks/  trailers/  customers/  facilities/  routes/
  Each subfolder has: {table}_batch_001.csv, _batch_002.csv, _batch_003.csv
```

### 6.4 Batch Manifest JSON

```json
{
  "batch_id": "B_2022_001",
  "created_at": "2026-09-13T18:00:00Z",
  "tables": [
    { "table": "loads", "file": "loads_batch_001.csv", "row_count": 28470 },
    { "table": "trips", "file": "trips_batch_001.csv", "row_count": 28470 }
  ]
}
```

### 6.5 Deliverables

- [ ] src/ingestion/batch_splitter.py
- [ ] data/batches/batch_001/, batch_002/, batch_003/
- [ ] src/ingestion/gcs_uploader.py
- [ ] src/ingestion/batch_controller.py (idempotency guard)
- [ ] scripts/simulate_batch.sh (one-command batch trigger)

---

## 7. Phase 3 — Bronze Layer

### 7.1 Design Principles

- Read CSV from GCS raw zone using explicit PySpark schemas
- All columns ingested as StringType at Bronze (prevents data loss from type mismatch)
- Add 4 technical metadata columns to every record
- Write Parquet to GCS bronze zone, partitioned by batch_id
- Raise BatchLevelFailure if a required column is missing from the CSV

### 7.2 Technical Metadata Columns

| Column | Type | Description |
|:---|:---|:---|
| _batch_id | STRING | Batch identifier, e.g. B_2022_001 |
| _source_file | STRING | Full GCS path of the source CSV |
| _ingestion_timestamp | TIMESTAMP | UTC time of ingestion |
| _record_hash | STRING | SHA-256 of all field values concatenated |

### 7.3 Schema Mismatch = Batch-Level Failure

```python
missing_cols = set(expected_cols) - set(actual_csv_cols)
if missing_cols:
    raise BatchLevelFailure(
        f"Missing required columns: {missing_cols}. "
        f"Gold publication SKIPPED. Existing Gold UNCHANGED."
    )
```

When a BatchLevelFailure is raised:
- The entire batch is marked FAILED in batch_control
- Gold publication is SKIPPED
- Existing Gold tables are NOT touched
- A failure notification is sent immediately

### 7.4 Bronze Output Structure

```
gs://logistics-data-platform/bronze/
  loads/batch_id=B_2022_001/part-00000.parquet
  trips/batch_id=B_2022_001/part-00000.parquet
  ...
```

### 7.5 Deliverables

- [ ] config/schema_definitions/bronze_schemas.py (all 14 explicit schemas)
- [ ] src/ingestion/bronze_writer.py
- [ ] Bronze Parquet files readable by Spark with correct partition structure

---

## 8. Phase 4 — Data Quality Framework

### 8.1 Framework Architecture

```
Input DataFrame (Bronze)
      |
      v
+------------------------------------------+
|  DQ Engine                               |
|                                          |
|  1. Completeness Rules  (NOT NULL)       |
|  2. Validity Rules      (domain/format)  |
|  3. Range Rules         (numeric bounds) |
|  4. Uniqueness Rules    (business keys)  |
|  5. Referential Rules   (FK checks)      |
|                                          |
|  Each record tagged with:                |
|    _is_valid: BOOLEAN                    |
|    _dq_violations: ARRAY<STRING>         |
+------------------------------------------+
      |
  +---+----+
VALID   INVALID
  |         |
Silver   Quarantine + rejection_log
```

### 8.2 Core DQ Engine (src/quality/dq_engine.py)

```python
class DQEngine:
    def __init__(self, spark, table_name, batch_id):
        self.spark = spark
        self.table_name = table_name
        self.batch_id = batch_id
        self.rules = []

    def add_rule(self, rule):
        self.rules.append(rule)
        return self

    def evaluate(self, df):
        violation_cols = []
        for rule in self.rules:
            df, col_name = rule.apply(df)
            violation_cols.append(col_name)

        df = df.withColumn("_dq_violations",
                           array_compact(array(*violation_cols)))
        df = df.withColumn("_is_valid",
                           size("_dq_violations") == 0)

        valid_df   = df.filter("_is_valid = true")
        invalid_df = df.filter("_is_valid = false")

        return DQResult(valid_df, invalid_df)
```

### 8.3 Five Rule Types

**Completeness (NOT NULL):**
```python
when(col(column).isNull(), lit(f"{rule_id}: {column} must not be NULL")).otherwise(None)
```

**Validity (Domain/Enum):**
```python
when(~col(column).isin(allowed_values), lit(f"{rule_id}: invalid value")).otherwise(None)
```

**Range (Numeric Bounds):**
```python
when((col(column) < min_val) | (col(column) > max_val),
     lit(f"{rule_id}: out of range")).otherwise(None)
```

**Uniqueness:** Window function to detect business key duplicates within a batch.

**Referential Integrity:** Left-anti join against parent dimension to find orphan FK keys.

### 8.4 Loads Rule Configuration Example

```python
def build_loads_rules(customers_df, routes_df):
    return [
        NotNullRule("load_id",     "LOAD_PK_NOT_NULL"),
        NotNullRule("customer_id", "LOAD_CUSTOMER_NOT_NULL"),
        NotNullRule("route_id",    "LOAD_ROUTE_NOT_NULL"),
        NotNullRule("load_date",   "LOAD_DATE_NOT_NULL"),
        NotNullRule("revenue",     "LOAD_REVENUE_NOT_NULL"),
        DateFormatRule("load_date", "yyyy-MM-dd",           "LOAD_DATE_FORMAT"),
        RangeRule("weight_lbs",   min_val=1,                "LOAD_WEIGHT_POSITIVE"),
        RangeRule("pieces",       min_val=1,                "LOAD_PIECES_POSITIVE"),
        RangeRule("revenue",      min_val=0.0,              "LOAD_REVENUE_NON_NEG"),
        DomainRule("load_type",   ["Dry Van","Refrigerated"],"LOAD_TYPE_VALID"),
        DomainRule("booking_type",["Spot","Dedicated","Contract"],"LOAD_BOOKING_VALID"),
        ForeignKeyRule("customer_id", customers_df, "customer_id", "LOAD_CUSTOMER_FK"),
        ForeignKeyRule("route_id",    routes_df,    "route_id",    "LOAD_ROUTE_FK"),
    ]
```

### 8.5 Deliverables

- [ ] src/quality/dq_engine.py (core framework)
- [ ] src/quality/rules/ (5 rule type files)
- [ ] src/quality/table_rules/ (14 table-specific rule configuration files)
- [ ] Unit tests for every rule type in tests/unit/

---

## 9. Phase 5 — Silver Layer

### 9.1 Purpose

Silver is the trusted, cleansed, type-cast layer. Only records that pass ALL DQ checks reach Silver.

### 9.2 Common Transformations (All Tables)

- Trim leading/trailing whitespace from all STRING columns
- Cast date strings to DATE or TIMESTAMP using explicit format strings
- Cast numeric strings to INTEGER, FLOAT, or DECIMAL
- Carry forward pipeline metadata: _batch_id, _source_file
- Add _silver_timestamp (UTC)
- Do NOT replace NULLs with arbitrary default values — keep NULL where business-appropriate

### 9.3 Table-Specific Transformations

| Table | Key Transformations |
|:---|:---|
| drivers | hire_date/termination_date/date_of_birth to DATE; NULL termination_date preserved (active drivers) |
| trucks | acquisition_date to DATE; model_year to INTEGER; make to title case |
| trailers | acquisition_date, model_year cast; trailer_type standardized |
| customers | contract_start_date to DATE; customer_type/account_status standardized |
| facilities | latitude/longitude to DOUBLE; negative longitude valid (US geography) |
| routes | typical_distance_miles/typical_transit_days to INTEGER; rates to DECIMAL |
| loads | load_date to DATE; weight_lbs/pieces to INTEGER; revenue/surcharge/accessorial to DECIMAL |
| trips | dispatch_date to DATE; distance to INTEGER; nullable FKs stay NULL |
| fuel_purchases | purchase_date to TIMESTAMP; gallons/price/cost to DECIMAL; nullable FKs stay NULL |
| maintenance_records | maintenance_date to DATE; all cost columns to DECIMAL |
| delivery_events | scheduled/actual_datetime to TIMESTAMP(microsecond); on_time_flag to BOOLEAN |
| safety_incidents | incident_date to TIMESTAMP; damage/claim costs to DECIMAL; flags to BOOLEAN |
| driver_monthly_metrics | month to DATE; all metrics to DECIMAL or INTEGER |
| truck_utilization_metrics | month to DATE; utilization_rate >1.0 is VALID (overtime, keep it) |

### 9.4 Silver Output

```
gs://logistics-data-platform/silver/
  loads/batch_id=B_2022_001/part-00000.parquet
  trips/batch_id=B_2022_001/part-00000.parquet
  ...
```

### 9.5 Deliverables

- [ ] src/silver/transformations/common.py
- [ ] src/silver/transformations/{table}_transform.py (14 files)
- [ ] src/silver/silver_writer.py

---

## 10. Phase 6 — Quarantine and Rejection Log

### 10.1 Quarantine Zone (GCS)

```
gs://logistics-data-platform/quarantine/
  loads/batch_id=B_2022_001/part-00000.parquet
    Contains: all raw field values + _dq_violations array
```

### 10.2 Rejection Log Schema (BigQuery)

```sql
CREATE TABLE logistics_audit.rejection_log (
    rejection_id      STRING NOT NULL,
    batch_id          STRING NOT NULL,
    source_file       STRING NOT NULL,
    table_name        STRING NOT NULL,
    record_id         STRING,           -- PK value of the rejected record
    column_name       STRING,           -- Column that triggered the violation
    validation_rule   STRING NOT NULL,  -- e.g. LOAD_WEIGHT_POSITIVE
    rejection_reason  STRING NOT NULL,  -- Human-readable message
    raw_value         STRING,           -- The actual bad value from the record
    rejected_at       TIMESTAMP NOT NULL
);
```

### 10.3 Rejection Log Population (One Row Per Violation Per Record)

```python
rejection_rows = invalid_df.select(
    pk_col, "_source_file", "_batch_id",
    explode("_dq_violations").alias("violation")
).withColumn("rejection_id", expr("uuid()"))  .withColumn("table_name",   lit(table_name))  .withColumn("rejected_at",  current_timestamp())
```

Example: A record rejected for both LOAD_WEIGHT_POSITIVE and LOAD_CUSTOMER_FK
produces 2 rows in rejection_log, one per violation.

### 10.4 Deliverables

- [ ] src/audit/rejection_logger.py
- [ ] Quarantine Parquet written for every batch with invalid records
- [ ] rejection_log BigQuery table populated after each batch run

---

## 11. Phase 7 — Gold Staging Layer (Star Schema)

### 11.1 Why Staging First?

Never write directly to Power BI-facing Gold tables.
Build staging versions -> validate -> atomically publish.
Staging is a safe scratch pad; failed validations leave published Gold unchanged.

### 11.2 Dimension Table Design

| Dimension | Source | SCD Type | Key Notes |
|:---|:---|:---:|:---|
| dim_date | Generated programmatically | N/A | Date spine 2020-01-01 to 2025-12-31 |
| dim_driver | silver.drivers | Type 2 | Track employment_status changes over time |
| dim_truck | silver.trucks | Type 2 | Track status changes (Active/Maintenance/Inactive) |
| dim_trailer | silver.trailers | Type 1 | Stable; overwrite on change |
| dim_customer | silver.customers | Type 1 | Stable; overwrite on change |
| dim_facility | silver.facilities | Type 1 | Stable; overwrite on change |
| dim_route | silver.routes | Type 1 | Stable; overwrite on change |

SCD Type 2 columns added to dim_driver and dim_truck:
- driver_key (surrogate PK, INTEGER)
- valid_from (DATE)
- valid_to (DATE, 9999-12-31 for current records)
- is_current (BOOLEAN)

### 11.3 Fact Table Design

> CRITICAL: Keep fact tables SEPARATE. Never join fact_trip + fact_fuel_purchase
> into one table — doing so multiplies rows and inflates every measure.
> Example: 5 fuel purchases x 3 delivery events = 15 rows per trip (wrong).

| Fact Table | Grain | Key Dimensions | Key Measures |
|:---|:---|:---|:---|
| fact_load | Per load | date_key, customer_key, route_key | revenue, fuel_surcharge, weight_lbs, pieces |
| fact_trip | Per trip | date_key, driver_key, truck_key, trailer_key, load_key | distance_miles, duration_hours, fuel_gallons, avg_mpg, idle_hours |
| fact_fuel_purchase | Per fuel stop | date_key, trip_key, truck_key, driver_key | gallons, price_per_gallon, total_cost |
| fact_maintenance | Per maintenance event | date_key, truck_key | labor_cost, parts_cost, total_cost, downtime_hours |
| fact_delivery_event | Per pickup/delivery | date_key, load_key, trip_key, facility_key | detention_minutes, on_time_flag |
| fact_safety_incident | Per incident | date_key, trip_key, truck_key, driver_key | vehicle_damage_cost, cargo_damage_cost, claim_amount |

### 11.4 Gold Staging Output

```
gs://logistics-data-platform/gold_staging/
  batch_id=B_2022_001/
    dim_driver/, dim_truck/, dim_trailer/
    dim_customer/, dim_facility/, dim_route/, dim_date/
    fact_load/, fact_trip/, fact_fuel_purchase/
    fact_maintenance/, fact_delivery_event/, fact_safety_incident/
```

### 11.5 Deliverables

- [ ] src/gold/dimensions/dim_date.py (date spine generator)
- [ ] src/gold/dimensions/dim_*.py (6 dimension builders)
- [ ] src/gold/facts/fact_*.py (6 fact table builders)
- [ ] Gold Staging Parquet output in GCS

---

## 12. Phase 8 — Gold Validation and Safe Publishing

### 12.1 Pre-Publication Validation Checks

| Check | Failure Behavior |
|:---|:---|
| Row count sanity (within +/-30% of prior batch) | WARN only, do not block |
| Surrogate key uniqueness in all dimensions | BLOCK — do not publish |
| Primary key uniqueness in all facts | BLOCK |
| NULL surrogate FK keys in facts | BLOCK |
| Referential integrity (fact keys -> dim keys) | BLOCK |
| Revenue reconciliation (Gold vs Silver within 1%) | BLOCK |
| Batch rejection rate exceeds configured threshold | BLOCK |

### 12.2 Safe Publishing Logic

```python
class GoldPublisher:
    def publish(self, batch_id, staging_path, gold_path):
        # Step 1: Validate staging — before touching any published Gold
        result = GoldValidator().validate(staging_path)

        if not result.passed:
            # CRITICAL: Existing Gold is NOT touched
            self.audit_logger.log_skipped(batch_id, result.failures)
            self.notifier.send_failure(batch_id, result)
            return PublishStatus.SKIPPED

        # Step 2: Atomic MERGE into Gold (only reached if validation passed)
        self._merge_into_gold(staging_path, gold_path)

        # Step 3: Audit and notify success
        self.audit_logger.log_published(batch_id)
        self.notifier.send_success(batch_id)
        return PublishStatus.PUBLISHED
```

### 12.3 BigQuery MERGE Strategy (Idempotent)

```sql
MERGE logistics_gold.fact_load AS target
USING logistics_gold.fact_load_staging AS source
ON target.load_id = source.load_id
   AND target._batch_id = source._batch_id
WHEN MATCHED THEN
  UPDATE SET target.revenue = source.revenue, ...
WHEN NOT MATCHED THEN
  INSERT (load_id, date_key, customer_key, ...) VALUES (...)
```

Guarantees:
- Running the same batch twice inserts 0 duplicate rows
- Existing rows from previous batches are NOT deleted or altered
- Failed batches leave Gold entirely unchanged

### 12.4 Deliverables

- [ ] src/gold/gold_validator.py (all 7 validation checks)
- [ ] src/gold/gold_publisher.py (validate -> merge -> audit -> notify)
- [ ] sql/bigquery/merge_fact_*.sql (6 MERGE statements, one per fact table)
- [ ] Integration test: failed validation -> Gold row count unchanged

---

## 13. Phase 9 — BigQuery Load and Optimization

### 13.1 Table DDL Pattern (Partitioning + Clustering)

```sql
-- Example: fact_load
CREATE TABLE logistics_gold.fact_load
PARTITION BY DATE(load_date)
CLUSTER BY customer_key, route_key
(
    load_key              INT64 NOT NULL,
    load_id               STRING NOT NULL,
    date_key              DATE,
    load_date             DATE,
    customer_key          INT64,
    route_key             INT64,
    load_type             STRING,
    booking_type          STRING,
    weight_lbs            INT64,
    pieces                INT64,
    revenue               NUMERIC,
    fuel_surcharge        NUMERIC,
    accessorial_charges   NUMERIC,
    load_status           STRING,
    _batch_id             STRING,
    _ingestion_timestamp  TIMESTAMP
);
-- Apply same pattern to all 6 fact tables with appropriate partition/cluster columns
```

### 13.2 Reporting Views for Power BI

```sql
-- v_driver_performance
CREATE VIEW logistics_gold.v_driver_performance AS
SELECT
    d.driver_id, d.first_name, d.last_name, d.employment_status,
    COUNT(t.trip_key)             AS total_trips,
    SUM(t.actual_distance_miles)  AS total_miles,
    AVG(t.average_mpg)            AS avg_mpg,
    SUM(l.revenue)                AS total_revenue,
    AVG(CAST(de.on_time_flag AS INT64)) AS on_time_rate
FROM logistics_gold.fact_trip t
JOIN logistics_gold.dim_driver d ON t.driver_key = d.driver_key AND d.is_current = TRUE
JOIN logistics_gold.fact_load l ON t.load_key = l.load_key
JOIN logistics_gold.fact_delivery_event de ON t.trip_key = de.trip_key
GROUP BY 1, 2, 3, 4;
```

Other views to create:
- v_truck_utilization, v_route_profitability, v_facility_on_time
- v_monthly_fuel_cost, v_safety_summary, v_executive_kpis

### 13.3 Deliverables

- [ ] sql/bigquery/create_dimensions.sql
- [ ] sql/bigquery/create_facts.sql
- [ ] sql/bigquery/reporting_views.sql
- [ ] src/bigquery/bq_loader.py
- [ ] src/bigquery/bq_merge.py

---

## 14. Phase 10 — Audit Framework

### 14.1 pipeline_audit Table

```sql
CREATE TABLE logistics_audit.pipeline_audit (
    audit_id             STRING NOT NULL,
    batch_id             STRING NOT NULL,
    pipeline_name        STRING NOT NULL,
    source_file          STRING,
    table_name           STRING,
    start_time           TIMESTAMP,
    end_time             TIMESTAMP,
    duration_seconds     INT64,
    records_received     INT64,
    records_valid        INT64,
    records_rejected     INT64,
    rejection_rate_pct   FLOAT64,
    bronze_status        STRING,           -- SUCCESS | FAILED | SKIPPED
    silver_status        STRING,
    gold_staging_status  STRING,
    gold_publish_status  STRING,
    overall_status       STRING,           -- SUCCESS | PARTIAL | FAILED
    error_message        STRING,
    created_at           TIMESTAMP
);
```

### 14.2 Audit Entry Lifecycle

```
Batch Starts     -> INSERT row with overall_status=PROCESSING
Bronze Done      -> UPDATE bronze_status = SUCCESS or FAILED
Silver Done      -> UPDATE silver_status, records_valid, records_rejected
Gold Staging     -> UPDATE gold_staging_status
Gold Publish     -> UPDATE gold_publish_status, overall_status
Batch Ends       -> UPDATE end_time, duration_seconds
```

### 14.3 Deliverables

- [ ] src/audit/audit_logger.py (write and update pipeline_audit entries)
- [ ] pipeline_audit BigQuery table updated at each pipeline phase

---

## 15. Phase 11 — Notification System

### 15.1 Implementation (Slack Webhook)

```python
class PipelineNotifier:
    def __init__(self, webhook_url):
        self.webhook_url = webhook_url

    def send_success(self, batch_id, summary):
        msg = self._format_success(batch_id, summary)
        requests.post(self.webhook_url, json={"text": msg})

    def send_failure(self, batch_id, details):
        msg = self._format_failure(batch_id, details)
        requests.post(self.webhook_url, json={"text": msg})
```

### 15.2 Notification Templates

**Success with record rejections:**
```
Logistics Pipeline Alert - Batch B_2022_001

Records received:  28,470
Records accepted:  28,320   (99.47%)
Records rejected:     150   (0.53%)

Top rejection reasons:
  LOAD_WEIGHT_POSITIVE    80 records
  LOAD_CUSTOMER_FK        40 records
  LOAD_ROUTE_FK           30 records

Gold publication:  SUCCESS
```

**Batch-level failure (schema/structural):**
```
ALERT: Logistics Pipeline FAILED - Batch B_2022_002

Failure type: Schema validation failure
Details:      Missing required column 'route_id' in loads_batch_002.csv

Gold publication:  SKIPPED
Existing Gold:     UNCHANGED
```

### 15.3 Deliverables

- [ ] src/notification/notifier.py
- [ ] SLACK_WEBHOOK_URL configured in .env
- [ ] Notifications triggered on both success-with-rejections and batch failures

---

## 16. Phase 12 — Power BI Dashboards

### 16.1 Connection Rule

Power BI connects ONLY to:
- Published Gold tables (dim_*, fact_*)
- Reporting views (v_driver_performance, v_truck_utilization, etc.)
- Audit tables (pipeline_audit, rejection_log) — DQ dashboard only

NEVER connect to: raw, bronze, silver, or gold_staging layers.

### 16.2 Business Dashboard (5 Pages)

**Page 1 — Executive Overview**
- KPI cards: Total Revenue, Total Loads, Total Trips, On-Time %, Fuel Cost, Maintenance Cost
- Revenue trend by month (line chart)
- Revenue by booking type (donut chart)
- Top 10 customers by revenue (bar chart)

**Page 2 — Delivery Performance**
- On-Time vs Late count (clustered bar)
- On-time rate by facility (heat table)
- Average detention minutes by facility
- Route performance table: avg distance, on-time %, avg revenue

**Page 3 — Fleet Performance**
- Truck utilization rate by truck (bar)
- Total miles by truck (sorted bar)
- Average MPG trend (line)
- Maintenance cost vs downtime scatter plot
- Fuel cost per mile trend

**Page 4 — Driver Performance**
- Driver scorecard table: trips, miles, revenue, MPG, on-time %, incidents
- Top/bottom 10 drivers by on-time %
- Safety incidents summary per driver

**Page 5 — Customer and Route Analytics**
- Revenue by customer (bar)
- Loads by route (table)
- Revenue per mile by route
- Customer type breakdown (donut)

### 16.3 DQ Dashboard (3 Pages)

Data source: pipeline_audit + rejection_log

**Page 1 — Pipeline Health**
- KPI cards: Total Batches, Successful, Failed
- Processing duration trend
- Records processed per batch (bar)
- Pipeline execution status donut

**Page 2 — Data Quality Monitoring**
- Total rejected records (card)
- Rejection rate trend (line)
- Rejections by table (stacked bar)
- Rejections by rule (treemap)
- Top rejection reasons table

**Page 3 — Batch Drill-Down**
- Slicers: Batch -> Table -> Validation Rule
- Detail table: record_id, column_name, validation_rule, rejection_reason, raw_value

### 16.4 Deliverables

- [ ] powerbi/business_dashboard.pbix
- [ ] powerbi/dq_dashboard.pbix
- [ ] Screenshots and documentation in docs/

---

## 17. Phase 13 — Pipeline Automation

### 17.1 Orchestration Approach

Python schedule library watching GCS manifests/ every 10 minutes.
Simple, zero additional infrastructure, appropriate for portfolio scope.

```python
# src/pipeline/scheduler.py
import schedule, time

def detect_and_run():
    pending_batches = batch_controller.get_pending_batches()
    for batch in pending_batches:
        pipeline_runner.run(batch)

schedule.every(10).minutes.do(detect_and_run)

while True:
    schedule.run_pending()
    time.sleep(60)
```

### 17.2 Full Pipeline Runner

```python
# src/pipeline/pipeline_runner.py
def run(batch):
    audit = AuditLogger(batch.batch_id)
    audit.start()

    try:
        # 1. Idempotency guard
        if batch_controller.is_already_processed(batch.batch_id):
            audit.skip("Already processed")
            return

        # 2. Bronze ingestion
        bronze_df = BronzeWriter(spark).write(batch)

        # 3. DQ evaluation
        dq_engine = build_dq_engine(batch.table_name)
        result = dq_engine.evaluate(bronze_df)

        # 4. Handle batch-level failure
        if result.is_batch_failure:
            audit.fail("Batch-level failure: " + result.error)
            notifier.send_failure(batch.batch_id, result)
            return  # Gold UNCHANGED

        # 5. Write Silver and Quarantine
        SilverWriter(spark).write(result.valid_df, batch)
        QuarantineWriter(spark).write(result.invalid_df, batch)
        RejectionLogger().log(result.invalid_df, batch)

        # 6. Build Gold Staging
        GoldBuilder(spark).build_staging(batch)

        # 7. Validate and Publish
        publish_status = GoldPublisher().publish(batch.batch_id)

        # 8. Audit and Notify
        audit.complete(result, publish_status)
        notifier.send(batch.batch_id, result, publish_status)

    except Exception as e:
        audit.error(str(e))
        raise
```

### 17.3 Deliverables

- [ ] src/pipeline/pipeline_runner.py
- [ ] src/pipeline/scheduler.py
- [ ] scripts/simulate_batch.sh (upload batch to GCS, trigger pipeline)
- [ ] End-to-end demo walkthrough in docs/portfolio_summary.md

---

## 18. Phase 14 — Testing

### 18.1 Unit Tests

| Test File | What It Tests |
|:---|:---|
| test_dq_completeness.py | NOT NULL rule rejects records with NULL required fields |
| test_dq_validity.py | Domain rule rejects invalid enum values |
| test_dq_range.py | Range rule rejects negative weights and revenues |
| test_dq_referential.py | FK rule rejects orphan customer_id and route_id |
| test_bronze_writer.py | Schema mismatch raises BatchLevelFailure |
| test_silver_transforms.py | Date casting, type casting, NULL preservation |
| test_batch_controller.py | Duplicate batch_id returns SKIPPED status |

### 18.2 Integration Tests

| Test | Scenario | Expected Result |
|:---|:---|:---|
| test_idempotency.py | Run same batch twice | Second run inserts 0 new rows |
| test_duplicate_batch.py | Submit known batch_id | Status=SKIPPED, Gold unchanged |
| test_gold_unchanged_on_failure.py | Batch fails Gold validation | Gold row count identical before/after |
| test_pipeline_e2e.py | Full batch: raw CSV to BigQuery | All stages succeed, audit shows SUCCESS |
| test_record_level_failure.py | Valid + invalid records in same batch | Valid->Silver, Invalid->Quarantine |
| test_batch_level_failure.py | CSV missing required column | Pipeline FAILED, Gold unchanged |

### 18.3 Running Tests

```bash
# Unit tests (no GCS or BQ required, use local Spark)
pytest tests/unit/ -v

# Integration tests
pytest tests/integration/ -v -m integration

# Coverage report
pytest --cov=src --cov-report=html
```

### 18.4 Deliverables

- [ ] All unit tests passing
- [ ] All integration tests passing
- [ ] Test coverage report generated
- [ ] scripts/run_tests.sh

---

## 19. Phase 15 — Performance Validation

### 19.1 Benchmark Queries (Before vs After Partitioning/Clustering)

**Query 1: Revenue by customer for Q1 2023**
```sql
SELECT customer_id, SUM(revenue) AS total_revenue
FROM logistics_gold.fact_load
WHERE load_date BETWEEN '2023-01-01' AND '2023-03-31'
GROUP BY customer_id ORDER BY total_revenue DESC;
```

**Query 2: Driver performance for specific truck**
```sql
SELECT driver_key, COUNT(*) AS trips, SUM(actual_distance_miles) AS miles
FROM logistics_gold.fact_trip
WHERE dispatch_date BETWEEN '2023-01-01' AND '2023-12-31'
  AND truck_key = 42
GROUP BY driver_key;
```

**Query 3: Monthly fuel cost trend**
```sql
SELECT DATE_TRUNC(purchase_date, MONTH) AS month, SUM(total_cost) AS fuel_cost
FROM logistics_gold.fact_fuel_purchase
WHERE purchase_date BETWEEN '2022-01-01' AND '2024-12-31'
GROUP BY month ORDER BY month;
```

### 19.2 Benchmark Results Template

| Query | Without Optimization | With Optimization | Improvement |
|:---|:---:|:---:|:---:|
| Q1 Revenue by customer (Q1 2023) | TBD MB | TBD MB | TBD% |
| Q2 Driver trip analysis (2023) | TBD MB | TBD MB | TBD% |
| Q3 Monthly fuel cost trend | TBD MB | TBD MB | TBD% |

> Measure actual bytes scanned in BigQuery job details. Document in docs/performance_benchmarks.md.

### 19.3 Aggregate Validation vs Source Metrics

```
# Compare Gold-derived driver trips vs driver_monthly_metrics source
gold_trips = spark.sql(
    "SELECT driver_id, DATE_TRUNC(month, dispatch_date) AS month, COUNT(*) AS gold_trips "
    "FROM silver.trips GROUP BY driver_id, month"
)
source_trips = spark.read.csv("data/raw/driver_monthly_metrics.csv")
    .select("driver_id", "month", "trips_completed")
discrepancies = gold_trips.join(source_trips, ["driver_id", "month"])
    .filter(col("gold_trips") != col("trips_completed"))
# Investigate all discrepancies
```

### 19.4 Deliverables

- [ ] notebooks/03_performance_validation.ipynb with actual before/after measurements
- [ ] docs/performance_benchmarks.md with documented results
- [ ] Aggregate comparison: Gold vs driver_monthly_metrics and truck_utilization_metrics

---

## 20. Milestone Summary and Timeline

| Phase | Key Deliverable | Effort |
|:---|:---|:---:|
| Phase 0 | Docker, GCS, BQ environment ready | 0.5 days |
| Phase 1 | DATA_DICTIONARY.md | Done |
| Phase 2 | Batch CSVs + GCS upload scripts | 0.5 days |
| Phase 3 | Bronze Parquet in GCS | 1 day |
| Phase 4 | Reusable DQ engine + all 14 table rule files | 2 days |
| Phase 5 | Cleansed Silver Parquet | 2 days |
| Phase 6 | Quarantine + rejection_log in BigQuery | 0.5 days |
| Phase 7 | Star Schema in Gold Staging | 2 days |
| Phase 8 | Gold Validator + Publisher | 1 day |
| Phase 9 | Partitioned/clustered Gold in BigQuery | 1 day |
| Phase 10 | pipeline_audit framework | 0.5 days |
| Phase 11 | Slack notifications | 0.5 days |
| Phase 12 | Power BI Business + DQ dashboards | 2 days |
| Phase 13 | Automated pipeline runner + scheduler | 1 day |
| Phase 14 | Unit + integration test suite | 2 days |
| Phase 15 | Performance benchmarks + aggregate validation | 0.5 days |
| Final | README, docs, portfolio polish | 1 day |
| **Total** | | **~18 days** |

---

## 21. Data Quality Rules Registry

### drivers
| Rule ID | Column | Rule Type | Rule |
|:---|:---|:---|:---|
| DRV_PK_NOTNULL | driver_id | Completeness | NOT NULL |
| DRV_HIRE_DATE | hire_date | Validity | Valid date YYYY-MM-DD |
| DRV_TERM_DATE | termination_date | Validity | Valid date if not NULL |
| DRV_DOB | date_of_birth | Validity | Valid date; DOB < hire_date |
| DRV_STATUS | employment_status | Validity | [Active, Terminated] |
| DRV_CDL | cdl_class | Validity | [A, B] |
| DRV_EXP | years_experience | Range | >= 0 |

### trucks
| Rule ID | Column | Rule Type | Rule |
|:---|:---|:---|:---|
| TRK_PK | truck_id | Completeness | NOT NULL |
| TRK_STATUS | status | Validity | [Active, Maintenance, Inactive] |
| TRK_FUEL | fuel_type | Validity | [Diesel] |
| TRK_TANK | tank_capacity_gallons | Range | > 0 |
| TRK_ACQ | acquisition_date | Validity | Valid date YYYY-MM-DD |
| TRK_YEAR | model_year | Range | 1990 to 2030 |

### customers
| Rule ID | Column | Rule Type | Rule |
|:---|:---|:---|:---|
| CUST_PK | customer_id | Completeness | NOT NULL |
| CUST_NAME | customer_name | Completeness | NOT NULL |
| CUST_TYPE | customer_type | Validity | [Dedicated, Contract, Spot] |
| CUST_STATUS | account_status | Validity | [Active, Inactive] |
| CUST_DATE | contract_start_date | Validity | Valid date |

### routes
| Rule ID | Column | Rule Type | Rule |
|:---|:---|:---|:---|
| RT_PK | route_id | Completeness | NOT NULL |
| RT_DIST | typical_distance_miles | Range | > 0 |
| RT_RATE | base_rate_per_mile | Range | > 0 |
| RT_DAYS | typical_transit_days | Range | >= 1 |

### facilities
| Rule ID | Column | Rule Type | Rule |
|:---|:---|:---|:---|
| FAC_PK | facility_id | Completeness | NOT NULL |
| FAC_TYPE | facility_type | Validity | [Cross-Dock, Distribution Center, Terminal, Warehouse] |
| FAC_LAT | latitude | Range | -90 to 90 |
| FAC_LON | longitude | Range | -180 to 180 (negative is valid for US) |

### loads
| Rule ID | Column | Rule Type | Rule |
|:---|:---|:---|:---|
| LD_PK | load_id | Completeness | NOT NULL |
| LD_CUST_NN | customer_id | Completeness | NOT NULL |
| LD_ROUTE_NN | route_id | Completeness | NOT NULL |
| LD_DATE_NN | load_date | Completeness | NOT NULL |
| LD_REV_NN | revenue | Completeness | NOT NULL |
| LD_DATE | load_date | Validity | Valid date YYYY-MM-DD |
| LD_TYPE | load_type | Validity | [Dry Van, Refrigerated] |
| LD_BOOK | booking_type | Validity | [Spot, Dedicated, Contract] |
| LD_WGT | weight_lbs | Range | > 0 |
| LD_PCS | pieces | Range | >= 1 |
| LD_REV | revenue | Range | >= 0 |
| LD_CUST_FK | customer_id | Referential | -> customers.customer_id |
| LD_ROUTE_FK | route_id | Referential | -> routes.route_id |

### trips
| Rule ID | Column | Rule Type | Rule |
|:---|:---|:---|:---|
| TR_PK | trip_id | Completeness | NOT NULL |
| TR_LOAD_NN | load_id | Completeness | NOT NULL |
| TR_DATE | dispatch_date | Validity | Valid date |
| TR_DIST | actual_distance_miles | Range | > 0 |
| TR_DUR | actual_duration_hours | Range | > 0 |
| TR_FUEL | fuel_gallons_used | Range | > 0 |
| TR_MPG | average_mpg | Range | > 0 and <= 30 (reasonable diesel MPG) |
| TR_IDLE | idle_time_hours | Range | >= 0 |
| TR_LOAD_FK | load_id | Referential | -> loads.load_id |
| TR_DRV_FK | driver_id | Referential | If not NULL -> drivers.driver_id |
| TR_TRK_FK | truck_id | Referential | If not NULL -> trucks.truck_id |
| TR_TRL_FK | trailer_id | Referential | If not NULL -> trailers.trailer_id |

### fuel_purchases
| Rule ID | Column | Rule Type | Rule |
|:---|:---|:---|:---|
| FP_PK | fuel_purchase_id | Completeness | NOT NULL |
| FP_TRIP_NN | trip_id | Completeness | NOT NULL |
| FP_DATE | purchase_date | Validity | Valid timestamp |
| FP_GAL | gallons | Range | > 0 |
| FP_PRICE | price_per_gallon | Range | > 0 |
| FP_COST | total_cost | Range | > 0 |
| FP_TRIP_FK | trip_id | Referential | -> trips.trip_id |
| FP_TRK_FK | truck_id | Referential | If not NULL -> trucks.truck_id |
| FP_DRV_FK | driver_id | Referential | If not NULL -> drivers.driver_id |

### delivery_events
| Rule ID | Column | Rule Type | Rule |
|:---|:---|:---|:---|
| DE_PK | event_id | Completeness | NOT NULL |
| DE_LOAD_NN | load_id | Completeness | NOT NULL |
| DE_TRIP_NN | trip_id | Completeness | NOT NULL |
| DE_FAC_NN | facility_id | Completeness | NOT NULL |
| DE_TYPE | event_type | Validity | [Pickup, Delivery] |
| DE_SCHED | scheduled_datetime | Validity | Valid timestamp |
| DE_ACTUAL | actual_datetime | Validity | Valid timestamp |
| DE_DET | detention_minutes | Range | >= 0 |
| DE_LOAD_FK | load_id | Referential | -> loads.load_id |
| DE_TRIP_FK | trip_id | Referential | -> trips.trip_id |
| DE_FAC_FK | facility_id | Referential | -> facilities.facility_id |

### safety_incidents
| Rule ID | Column | Rule Type | Rule |
|:---|:---|:---|:---|
| SI_PK | incident_id | Completeness | NOT NULL |
| SI_TRIP_NN | trip_id | Completeness | NOT NULL |
| SI_DATE | incident_date | Validity | Valid timestamp |
| SI_TYPE | incident_type | Validity | [Accident, Moving Violation, DOT Violation, Equipment Damage, Customer Complaint] |
| SI_DMG | vehicle_damage_cost | Range | >= 0 |
| SI_CDMG | cargo_damage_cost | Range | >= 0 |
| SI_CLM | claim_amount | Range | >= 0 |
| SI_TRIP_FK | trip_id | Referential | -> trips.trip_id |
| SI_TRK_FK | truck_id | Referential | If not NULL -> trucks.truck_id |
| SI_DRV_FK | driver_id | Referential | If not NULL -> drivers.driver_id |

### maintenance_records
| Rule ID | Column | Rule Type | Rule |
|:---|:---|:---|:---|
| MR_PK | maintenance_id | Completeness | NOT NULL |
| MR_TRUCK_NN | truck_id | Completeness | NOT NULL |
| MR_DATE | maintenance_date | Validity | Valid date |
| MR_TYPE | maintenance_type | Validity | [Inspection, Tire, Preventive, Repair, Transmission, Brake, Engine] |
| MR_ODO | odometer_reading | Range | > 0 |
| MR_LABOR | labor_hours | Range | >= 0 |
| MR_LCOST | labor_cost | Range | >= 0 |
| MR_PCOST | parts_cost | Range | >= 0 |
| MR_TCOST | total_cost | Range | >= 0 |
| MR_DOWN | downtime_hours | Range | >= 0 |
| MR_TRK_FK | truck_id | Referential | -> trucks.truck_id |

---

## 22. Star Schema Design

```
                         dim_date
                         --------
                         date_key (PK)
                         full_date
                         year / month / quarter
                         day_of_week / is_weekend
                              |
         +--------------------+--------------------+
         |                    |                    |
    fact_load           fact_trip        fact_fuel_purchase
    ---------           ---------        ------------------
    load_key(PK)        trip_key(PK)     fp_key(PK)
    date_key(FK)        date_key(FK)     date_key(FK)
    customer_key(FK)    load_key(FK)     trip_key(FK)
    route_key(FK)       driver_key(FK)   truck_key(FK)
    load_id             truck_key(FK)    driver_key(FK)
    load_type           trailer_key(FK)  gallons
    weight_lbs          dispatch_date    price_per_gallon
    pieces              distance_miles   total_cost
    revenue             duration_hours
    fuel_surcharge      fuel_gallons
    booking_type        avg_mpg
                        idle_hours
         |                    |                    |
         +--------------------+--------------------+
                              |
         +--------------------+--------------------+
         |                    |                    |
  fact_delivery_event  fact_maintenance  fact_safety_incident
  -------------------  ----------------  --------------------
  event_key(PK)        maint_key(PK)     incident_key(PK)
  date_key(FK)         date_key(FK)      date_key(FK)
  load_key(FK)         truck_key(FK)     trip_key(FK)
  trip_key(FK)         maintenance_date  truck_key(FK)
  facility_key(FK)     maintenance_type  driver_key(FK)
  event_type           odometer_reading  incident_type
  scheduled_datetime   labor_cost        at_fault_flag
  actual_datetime      parts_cost        injury_flag
  detention_minutes    total_cost        vehicle_damage_cost
  on_time_flag         downtime_hours    cargo_damage_cost
                                         claim_amount
                                         preventable_flag

Dimension Tables:
  dim_driver   - driver_key(PK), driver_id, first_name, last_name,
                 employment_status, cdl_class, years_experience,
                 is_current, valid_from, valid_to  [SCD Type 2]
  dim_truck    - truck_key(PK), truck_id, make, model_year, status,
                 fuel_type, tank_capacity_gallons,
                 is_current, valid_from, valid_to  [SCD Type 2]
  dim_trailer  - trailer_key(PK), trailer_id, trailer_type, length_feet,
                 model_year, status  [SCD Type 1]
  dim_customer - customer_key(PK), customer_id, customer_name,
                 customer_type, account_status  [SCD Type 1]
  dim_facility - facility_key(PK), facility_id, facility_name,
                 facility_type, city, state, latitude, longitude  [SCD Type 1]
  dim_route    - route_key(PK), route_id, origin_city, origin_state,
                 destination_city, destination_state,
                 typical_distance_miles, base_rate_per_mile  [SCD Type 1]
```

---

## 23. BigQuery Partitioning and Clustering Strategy

| Table | Partition Column | Cluster Columns | Rationale |
|:---|:---|:---|:---|
| fact_load | load_date | customer_key, route_key | Date-range queries common; filter by customer/route |
| fact_trip | dispatch_date | driver_key, truck_key | Fleet/driver analysis always date-bounded |
| fact_fuel_purchase | purchase_date | truck_key, driver_key | Fuel analysis by date range and vehicle |
| fact_maintenance | maintenance_date | truck_key | Maintenance history always per truck, date-bounded |
| fact_delivery_event | event_date | facility_key, event_type | Facility performance filters by date and event type |
| fact_safety_incident | incident_date | driver_key, truck_key | Safety analysis by period, driver, and vehicle |
| dim_* (all dims) | None | None | Dimensions are small; partitioning adds overhead |
| pipeline_audit | start_time | batch_id | Audit queries filter by date; batch lookup |
| rejection_log | rejected_at | table_name, validation_rule | DQ dashboard filters by date, table, rule |

---

## 24. Key Design Decisions

| Decision | Choice | Rationale |
|:---|:---|:---|
| Bronze type strategy | All StringType at Bronze | Prevents data loss from type mismatch; safe casting in Silver |
| SCD Type 2 scope | Only dim_driver and dim_truck | Status changes are analytically meaningful; other dims are stable |
| Nullable FKs | Allow NULL in trips/fuel_purchases/safety_incidents | Unassigned records are valid business events; rejecting loses real data |
| utilization_rate > 1.0 | Flag, do NOT reject | Overtime operation is valid; filtering it corrupts utilization metrics |
| Gold publish strategy | MERGE not TRUNCATE+INSERT | Guarantees idempotency; protects existing Gold from partial writes |
| Separate fact tables | Never join facts into one denormalized table | Avoids fanout row multiplication that inflates all aggregate measures |
| Orchestration tool | Python schedule library | Simple, zero extra infrastructure, sufficient for portfolio scope |
| Notification channel | Slack Incoming Webhook | Real-time alerts, no SMTP server needed |
| Aggregate table usage | Validation/reconciliation only | Use driver_monthly_metrics to validate Gold-derived metrics, not as primary source |
| Performance proof | Actual measurement required | Never claim improvement without documented before/after bytes-scanned evidence |

---

## 25. Resume and Portfolio Talking Points

1. **Medallion Architecture (Bronze -> Silver -> Gold)**
   Designed and implemented a 3-layer lakehouse with explicit data contracts between layers,
   processing 544K+ records across 14 logistics tables.

2. **Reusable Data Quality Framework**
   Built a config-driven, extensible DQ engine supporting 5 rule types
   (completeness, validity, range, uniqueness, referential integrity) across all 14 datasets,
   eliminating duplicate validation logic.

3. **Safe Atomic Gold Publishing**
   Implemented a pre-publication validation gate guaranteeing Power BI never consumes partial or
   failed data. Existing Gold remains immutable until a new batch passes all validation checks.

4. **Incremental and Idempotent Pipeline**
   Split 3 years of historical data into logical batches and implemented batch_control tracking
   to ensure the same batch is never processed twice regardless of how many times the pipeline runs.

5. **Record-Level vs Batch-Level Failure Handling**
   Distinguished between individual record rejections (DQ failures; valid records still proceed)
   and structural batch failures (missing column; entire batch halted, Gold unchanged).

6. **Star Schema Design**
   Designed a 7-dimension, 6-fact star schema with clear per-entity grain definitions,
   preventing measure multiplication by maintaining separate fact tables.

7. **BigQuery Performance Optimization**
   Applied date-based partitioning and multi-column clustering on all 6 fact tables,
   with documented before/after bytes-scanned benchmarks demonstrating measurable improvement.

8. **Audit and Observability**
   Built a full pipeline audit trail (pipeline_audit, rejection_log) queryable via a live
   Power BI DQ dashboard, enabling engineering teams to trace every record through the pipeline.

9. **End-to-End Automation**
   Wired the complete pipeline from GCS file detection through BigQuery publication and
   Slack notification without any manual intervention.

10. **SCD Type 2 Implementation**
    Selectively applied Slowly Changing Dimensions on dim_driver and dim_truck with documented
    business justification for tracking employment/equipment status changes over time.

---

*Last Updated: September 2026*
*Repository: github.com/yourusername/logistics-data-platform*
