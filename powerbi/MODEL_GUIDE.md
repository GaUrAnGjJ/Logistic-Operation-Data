# 📐 Power BI Star Schema Data Modeling Guide
**Project:** Logistics Operations & Analytics Platform  
**Target Warehouse:** Google BigQuery (`logistic-data-508513.logistics_gold` & `logistics_audit`)

---

## 1. Overview & Architecture Strategy

This guide provides the complete blueprint for establishing the analytical data model in **Power BI Desktop**, connecting directly to Google BigQuery.

The platform follows a **Kimball Dimensional Modeling** methodology:
- **7 Dimension Tables** (Conformed dimensions providing business context)
- **5 Fact Tables** (Transactional grains partitioned by date and clustered in BigQuery)
- **2 Aggregated Summary Marts** (Pre-computed monthly performance metrics)
- **Semantic Views** (Optional pre-joined views for DirectQuery reporting)

```
       ┌─────────────────┐       ┌─────────────────┐       ┌─────────────────┐
       │  dim_customer   │       │   dim_driver    │       │    dim_truck    │
       └────────┬────────┘       └────────┬────────┘       └────────┬────────┘
                │ 1                       │ 1                       │ 1
                │                         │                         │
                │ *                       │ *                       │ *
       ┌────────▼─────────────────────────▼─────────────────────────▼────────┐
       │                             fact_trips                              │
       └────────▲─────────────────────────▲─────────────────────────▲────────┘
                │ *                       │ *                       │ *
                │                         │                         │
                │ 1                       │ 1                       │ 1
       ┌────────┴────────┐       ┌────────┴────────┐       ┌────────┴────────┐
       │   dim_trailer   │       │    dim_route    │       │    dim_date     │
       └─────────────────┘       └─────────────────┘       └─────────────────┘
```

---

## 2. BigQuery Connection Setup

### A. Connector Selection
In Power BI Desktop:
1. Click **Get Data** $\rightarrow$ **More...** $\rightarrow$ Search **Google BigQuery**.
2. Click **Connect**.
3. In the authentication prompt, select **Organizational account** or **Service Account / GCP Credentials**.
4. Navigate to Project: `logistic-data-508513`.

### B. Storage Mode Recommendations

| Mode | Recommendation | Rationale |
|---|---|---|
| **Import Mode (Recommended for < 5M rows)** | ⭐ **Primary Choice** | Lightning-fast DAX calculation, full support for time-intelligence functions (`SAMEPERIODLASTYEAR`, `DATESYTD`), offline availability, and high interactive responsiveness. Total dataset is ~550k rows, which consumes < 50 MB RAM in Power BI VertiPaq. |
| **DirectQuery Mode** | *Alternative for real-time* | Offloads queries directly to BigQuery. Useful if live streaming / sub-minute freshness is enabled in the future. Requires semantic SQL views (`v_bi_*`) to prevent complex join generation. |
| **Composite / Dual Mode** | *Hybrid* | Dimensions in **Dual / Import**, heavy historical facts in **DirectQuery**. Best suited if fact table exceeds 100M rows. |

---

## 3. Star Schema Relationship Specifications

All dimensional relationships must follow **1-to-Many (`1:*`)** cardinality with **Single Cross-Filter Direction** (`Dimension filters Fact`). Avoid bidirectional filtering to maintain query efficiency and prevent circular relationship ambiguity.

### A. Operational Fact: `fact_trips`

| Dimension Table | Dimension Key (PK) | Fact Table | Fact Foreign Key (FK) | Cardinality | Filter Direction | Active? |
|---|---|---|---|---|---|---|
| `dim_date` | `full_date` | `fact_trips` | `dispatch_date` | 1 to Many (`1:*`) | Single | **Active** |
| `dim_date` | `full_date` | `fact_trips` | `load_date` | 1 to Many (`1:*`) | Single | Inactive (Use `USERELATIONSHIP`) |
| `dim_customer` | `customer_id` | `fact_trips` | `customer_id` | 1 to Many (`1:*`) | Single | **Active** |
| `dim_driver` | `driver_id` | `fact_trips` | `driver_id` | 1 to Many (`1:*`) | Single | **Active** (See SCD2 section) |
| `dim_truck` | `truck_id` | `fact_trips` | `truck_id` | 1 to Many (`1:*`) | Single | **Active** |
| `dim_trailer` | `trailer_id` | `fact_trips` | `trailer_id` | 1 to Many (`1:*`) | Single | **Active** |
| `dim_route` | `route_id` | `fact_trips` | `route_id` | 1 to Many (`1:*`) | Single | **Active** |

### B. Milestone Fact: `fact_delivery_events`

| Dimension Table | Dimension Key (PK) | Fact Table | Fact Foreign Key (FK) | Cardinality | Filter Direction | Active? |
|---|---|---|---|---|---|---|
| `dim_date` | `full_date` | `fact_delivery_events` | `event_date` | 1 to Many (`1:*`) | Single | **Active** |
| `dim_facility` | `facility_id` | `fact_delivery_events` | `facility_id` | 1 to Many (`1:*`) | Single | **Active** |

### C. Expense Fact: `fact_fuel_purchases`

| Dimension Table | Dimension Key (PK) | Fact Table | Fact Foreign Key (FK) | Cardinality | Filter Direction | Active? |
|---|---|---|---|---|---|---|
| `dim_date` | `full_date` | `fact_fuel_purchases` | `purchase_date` | 1 to Many (`1:*`) | Single | **Active** |
| `dim_truck` | `truck_id` | `fact_fuel_purchases` | `truck_id` | 1 to Many (`1:*`) | Single | **Active** |
| `dim_driver` | `driver_id` | `fact_fuel_purchases` | `driver_id` | 1 to Many (`1:*`) | Single | **Active** |

### D. Fleet Servicing Fact: `fact_maintenance`

| Dimension Table | Dimension Key (PK) | Fact Table | Fact Foreign Key (FK) | Cardinality | Filter Direction | Active? |
|---|---|---|---|---|---|---|
| `dim_date` | `full_date` | `fact_maintenance` | `maintenance_date` | 1 to Many (`1:*`) | Single | **Active** |
| `dim_truck` | `truck_id` | `fact_maintenance` | `truck_id` | 1 to Many (`1:*`) | Single | **Active** |

### E. Risk & Claims Fact: `fact_safety_incidents`

| Dimension Table | Dimension Key (PK) | Fact Table | Fact Foreign Key (FK) | Cardinality | Filter Direction | Active? |
|---|---|---|---|---|---|---|
| `dim_date` | `full_date` | `fact_safety_incidents` | `incident_date` | 1 to Many (`1:*`) | Single | **Active** |
| `dim_driver` | `driver_id` | `fact_safety_incidents` | `driver_id` | 1 to Many (`1:*`) | Single | **Active** |
| `dim_truck` | `truck_id` | `fact_safety_incidents` | `truck_id` | 1 to Many (`1:*`) | Single | **Active** |

---

## 4. Handling Slowly Changing Dimensions (SCD Type 2)

In `dim_driver`, driver records track historical changes (such as employment status transitions from *Active* to *Terminated* across Batches 001, 002, and 003).

### Recommended Approaches in Power BI:

#### Pattern 1: Current State Reporting (Default for Executive Dashboards)
If reporting on the **current fleet status** (e.g. active drivers today):
1. Create a calculated table or filter in Power Query:
   ```powerquery
   Table.SelectRows(dim_driver, each [is_current] = true)
   ```
2. Or apply a visual/page-level filter:
   `dim_driver[is_current] is TRUE`
3. Join `dim_driver[driver_id]` directly to `fact_trips[driver_id]`. This maintains standard 1-to-many relationship simplicity.

#### Pattern 2: Point-in-Time Historical Analysis (Using Surrogate Keys)
To accurately reflect the driver's attributes *at the exact time the trip was dispatched*:
* The transaction is joined on the surrogate key `driver_sk`, or evaluated via DAX:
```dax
Driver Name at Event = 
CALCULATE(
    SELECTEDVALUE(dim_driver[full_name]),
    FILTER(
        dim_driver,
        dim_driver[driver_id] = SELECTEDVALUE(fact_trips[driver_id]) &&
        fact_trips[dispatch_date] >= dim_driver[valid_from] &&
        (fact_trips[dispatch_date] <= dim_driver[valid_to] || ISBLANK(dim_driver[valid_to]))
    )
)
```

---

## 5. Role-Playing Date Dimensions

A single `dim_date` table serves multiple date contexts across the data model:

```
                  ┌──────────────────────┐
                  │       dim_date       │
                  └──────────┬───────────┘
                             │
            ┌────────────────┼────────────────┐
     Active │                │ Inactive       │ Inactive
 (dispatch_date)       (load_date)      (actual_datetime)
            │                │                │
            ▼                ▼                ▼
       fact_trips       fact_trips   fact_delivery_events
```

### DAX Implementation for Role-Playing Dates:
When a measure needs to analyze performance by `load_date` instead of `dispatch_date`:
```dax
Revenue by Load Date = 
CALCULATE(
    [Total Revenue],
    USERELATIONSHIP(fact_trips[load_date], dim_date[full_date])
)
```

---

## 6. Data Quality & Audit Sub-Model

For **Dashboard 2 (Pipeline Health & Governance)**, keep the audit tables in an isolated model group to prevent cross-filtering interference with operational business facts:

```
┌────────────────────────┐      ┌────────────────────────┐
│     pipeline_audit     │      │     batch_control      │
└───────────┬────────────┘      └───────────┬────────────┘
            │                               │
            │ batch_id                      │ batch_id
            ▼                               ▼
┌────────────────────────┐      ┌────────────────────────┐
│  data_quality_metrics  │      │     rejection_log      │
└────────────────────────┘      └────────────────────────┘
```

* **Shared Key:** `batch_id`
* **Purpose:** Allows executive stakeholders and data engineers to click on a batch (e.g. `batch_002`) and simultaneously see:
  1. Pipeline stage run times (Bronze $\rightarrow$ Silver $\rightarrow$ Gold)
  2. Data quality pass score (99.92%)
  3. Drill-through inspection of quarantined rows with rejection reasons.

---

## 7. Power BI Formatting & Column Standards

1. **Mark as Date Table**:
   * Right-click `dim_date` $\rightarrow$ **Mark as date table** $\rightarrow$ Select `full_date`.
2. **Sort by Column Configuration**:
   * `dim_date[month_name]` $\rightarrow$ Sort by `dim_date[month]`
   * `dim_date[day_name]` $\rightarrow$ Sort by `dim_date[day_of_week]`
3. **Data Categories**:
   * `dim_facility[latitude]` $\rightarrow$ Data Category: **Latitude**
   * `dim_facility[longitude]` $\rightarrow$ Data Category: **Longitude**
   * `dim_facility[city]` $\rightarrow$ Data Category: **City**
   * `dim_facility[state]` $\rightarrow$ Data Category: **State or Province**
4. **Key Columns to Hide from Report View**:
   * Hide foreign keys in Fact tables (`customer_id`, `truck_id`, etc.) to force users to slice by Dimension attributes, preserving dimensional integrity.

