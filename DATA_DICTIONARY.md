# Logistics Data Dictionary & Comprehensive CSV Profiling

This document provides a data profiling reference for all 14 CSV dataset tables within the Logistics data repository. Each table section details:
- **Row counts & column counts**
- **Table-level duplicate row statistics**
- **Actual column names**
- **Actual inferred data types** (e.g., `INTEGER`, `FLOAT / DECIMAL`, `DATE`, `TIMESTAMP`, `BOOLEAN`, `VARCHAR / ENUM`)
- **Null percentage & counts**
- **Duplicate percentage & counts** (computed across non-null values)
- **Unique value counts**
- **Date formats** (standardized pattern detected)
- **Invalid values & anomaly detection**
- **Foreign-key relationships & referential integrity status**

---

## Executive Summary of Dataset

| Table Name | CSV File | Row Count | Column Count | Duplicate Rows | Null Columns (>0%) | Primary Key(s) | Foreign Keys Count |
| :--- | :--- | :---: | :---: | :---: | :---: | :--- | :---: |
| **customers** | `customers.csv` | 200 | 8 | 0 (0.00%) | 0 (0.00%) | `customer_id` | 0 |
| **delivery_events** | `delivery_events.csv` | 170,820 | 11 | 0 (0.00%) | 0 (0.00%) | `event_id` | 3 |
| **driver_monthly_metrics** | `driver_monthly_metrics.csv` | 4,464 | 9 | 0 (0.00%) | 0 (0.00%) | `driver_id, month` | 1 |
| **drivers** | `drivers.csv` | 150 | 12 | 0 (0.00%) | 1 column(s) | `driver_id` | 0 |
| **facilities** | `facilities.csv` | 50 | 9 | 0 (0.00%) | 0 (0.00%) | `facility_id` | 0 |
| **fuel_purchases** | `fuel_purchases.csv` | 196,442 | 11 | 0 (0.00%) | 2 column(s) | `fuel_purchase_id` | 3 |
| **loads** | `loads.csv` | 85,410 | 12 | 0 (0.00%) | 0 (0.00%) | `load_id` | 2 |
| **maintenance_records** | `maintenance_records.csv` | 2,920 | 12 | 0 (0.00%) | 0 (0.00%) | `maintenance_id` | 1 |
| **routes** | `routes.csv` | 58 | 9 | 0 (0.00%) | 0 (0.00%) | `route_id` | 0 |
| **safety_incidents** | `safety_incidents.csv` | 170 | 15 | 0 (0.00%) | 2 column(s) | `incident_id` | 3 |
| **trailers** | `trailers.csv` | 180 | 9 | 0 (0.00%) | 0 (0.00%) | `trailer_id` | 0 |
| **trips** | `trips.csv` | 85,410 | 12 | 0 (0.00%) | 3 column(s) | `trip_id` | 4 |
| **truck_utilization_metrics** | `truck_utilization_metrics.csv` | 3,312 | 10 | 0 (0.00%) | 0 (0.00%) | `truck_id, month` | 1 |
| **trucks** | `trucks.csv` | 120 | 11 | 0 (0.00%) | 0 (0.00%) | `truck_id` | 0 |

---

## Table: `customers`
*Stores customer profiles, account status, credit terms, and annual revenue potential.*

- **Source File:** `data/customers.csv`
- **Total Row Count:** **200** rows
- **Total Column Count:** **8** columns
- **Duplicate Rows:** **0** (0.00%)
- **Primary Key:** `customer_id` (100% unique, 0 duplicate keys)
- **Foreign Key Relationships:** None (Root / Master Dimension Table)

### Column Specifications & Data Profiling

| Actual Column Name | Actual Data Type | Null % (Count) | Duplicate % (Count) | Unique Values | Date Format | Invalid Values | Foreign-Key Relationship |
| :--- | :--- | :---: | :---: | :---: | :---: | :--- | :--- |
| `customer_id` | `VARCHAR / STRING` | 0.00% (0) | 0.00% (0) | 200 | N/A | None (0 invalid / 100% valid) | **PRIMARY KEY** |
| `customer_name` | `VARCHAR / STRING` | 0.00% (0) | 46.50% (93) | 107 | N/A | None (0 invalid / 100% valid) | None |
| `customer_type` | `VARCHAR (ENUM - 3 values)` | 0.00% (0) | 98.50% (197) | 3 | N/A | None (0 invalid / 100% valid) | None |
| `credit_terms_days` | `INTEGER` | 0.00% (0) | 98.00% (196) | 4 | N/A | None (0 invalid / 100% valid) | None |
| `primary_freight_type` | `VARCHAR (ENUM - 6 values)` | 0.00% (0) | 97.00% (194) | 6 | N/A | None (0 invalid / 100% valid) | None |
| `account_status` | `VARCHAR (ENUM - 2 values)` | 0.00% (0) | 99.00% (198) | 2 | N/A | None (0 invalid / 100% valid) | None |
| `contract_start_date` | `DATE` | 0.00% (0) | 13.50% (27) | 173 | `YYYY-MM-DD` | None (0 invalid / 100% valid) | None |
| `annual_revenue_potential` | `INTEGER` | 0.00% (0) | 0.00% (0) | 200 | N/A | None (0 invalid / 100% valid) | None |

#### Observations & Domain Integrity Insights
- **Completeness:** 100% complete dataset. Zero null or missing values across all columns.
- **Temporal Scope:** `contract_start_date`: 2020-01-07 00:00:00 to 2022-01-01 00:00:00
- **Categorical Enums:** `customer_type` (['Dedicated', 'Contract', 'Spot']); `credit_terms_days` ([60, 30, 15, 45]); `primary_freight_type` (['General', 'Retail', 'Consumer Goods', 'Food/Beverage', 'Automotive', 'Electronics']); `account_status` (['Inactive', 'Active'])

---

## Table: `delivery_events`
*Granular log of pickup and delivery milestones, tracking scheduled vs. actual timestamps, detention minutes, and on-time statuses.*

- **Source File:** `data/delivery_events.csv`
- **Total Row Count:** **170,820** rows
- **Total Column Count:** **11** columns
- **Duplicate Rows:** **0** (0.00%)
- **Primary Key:** `event_id` (100% unique, 0 duplicate keys)
- **Foreign Key Relationships:** `load_id` &rarr; `loads(load_id)`, `trip_id` &rarr; `trips(trip_id)`, `facility_id` &rarr; `facilities(facility_id)` *(100% referential integrity verified, 0 orphan keys)*

### Column Specifications & Data Profiling

| Actual Column Name | Actual Data Type | Null % (Count) | Duplicate % (Count) | Unique Values | Date Format | Invalid Values | Foreign-Key Relationship |
| :--- | :--- | :---: | :---: | :---: | :---: | :--- | :--- |
| `event_id` | `VARCHAR / STRING` | 0.00% (0) | 0.00% (0) | 170,820 | N/A | None (0 invalid / 100% valid) | **PRIMARY KEY** |
| `load_id` | `VARCHAR / STRING` | 0.00% (0) | 50.00% (85,410) | 85,410 | N/A | None (0 invalid / 100% valid) | **FK** &rarr; `loads(load_id)` |
| `trip_id` | `VARCHAR / STRING` | 0.00% (0) | 50.00% (85,410) | 85,410 | N/A | None (0 invalid / 100% valid) | **FK** &rarr; `trips(trip_id)` |
| `event_type` | `VARCHAR (ENUM - 2 values)` | 0.00% (0) | 100.00% (170,818) | 2 | N/A | None (0 invalid / 100% valid) | None |
| `facility_id` | `VARCHAR / STRING` | 0.00% (0) | 99.97% (170,770) | 50 | N/A | None (0 invalid / 100% valid) | **FK** &rarr; `facilities(facility_id)` |
| `scheduled_datetime` | `TIMESTAMP (microsecond)` | 0.00% (0) | 41.68% (71,196) | 99,624 | `YYYY-MM-DD HH:MM:SS.ffffff` | None (0 invalid / 100% valid) | None |
| `actual_datetime` | `TIMESTAMP (microsecond)` | 0.00% (0) | 0.00% (0) | 170,820 | `YYYY-MM-DD HH:MM:SS.ffffff` | None (0 invalid / 100% valid) | None |
| `detention_minutes` | `INTEGER` | 0.00% (0) | 99.86% (170,580) | 240 | N/A | None (0 invalid / 100% valid) | None |
| `on_time_flag` | `BOOLEAN` | 0.00% (0) | 100.00% (170,818) | 2 | N/A | None (0 invalid / 100% valid) | None |
| `location_city` | `VARCHAR / STRING` | 0.00% (0) | 99.99% (170,800) | 20 | N/A | None (0 invalid / 100% valid) | None |
| `location_state` | `VARCHAR / STRING` | 0.00% (0) | 99.99% (170,801) | 19 | N/A | None (0 invalid / 100% valid) | None |

#### Observations & Domain Integrity Insights
- **Completeness:** 100% complete dataset. Zero null or missing values across all columns.
- **Temporal Scope:** `scheduled_datetime`: 2022-01-01 07:00:00 to 2025-01-02 19:21:42.629833; `actual_datetime`: 2022-01-01 06:13:28.892373 to 2025-01-03 00:29:47.403350
- **Categorical Enums:** `event_type` (['Pickup', 'Delivery'])
- **Referential Integrity:** All foreign key constraints verified with 0 orphan keys (100% matched against parent primary keys).

---

## Table: `driver_monthly_metrics`
*Aggregated monthly operational performance per driver including mileage, revenue, MPG, fuel volume, and on-time delivery rates.*

- **Source File:** `data/driver_monthly_metrics.csv`
- **Total Row Count:** **4,464** rows
- **Total Column Count:** **9** columns
- **Duplicate Rows:** **0** (0.00%)
- **Primary Key:** `driver_id, month` (100% unique, 0 duplicate keys)
- **Foreign Key Relationships:** `driver_id` &rarr; `drivers(driver_id)` *(100% referential integrity verified, 0 orphan keys)*

### Column Specifications & Data Profiling

| Actual Column Name | Actual Data Type | Null % (Count) | Duplicate % (Count) | Unique Values | Date Format | Invalid Values | Foreign-Key Relationship |
| :--- | :--- | :---: | :---: | :---: | :---: | :--- | :--- |
| `driver_id` | `VARCHAR / STRING` | 0.00% (0) | 97.22% (4,340) | 124 | N/A | None (0 invalid / 100% valid) | **COMPOSITE PK** |
| `month` | `DATE` | 0.00% (0) | 99.19% (4,428) | 36 | `YYYY-MM-DD` | None (0 invalid / 100% valid) | **COMPOSITE PK** |
| `trips_completed` | `INTEGER` | 0.00% (0) | 99.28% (4,432) | 32 | N/A | None (0 invalid / 100% valid) | None |
| `total_miles` | `INTEGER` | 0.00% (0) | 8.15% (364) | 4,100 | N/A | None (0 invalid / 100% valid) | None |
| `total_revenue` | `FLOAT / DECIMAL` | 0.00% (0) | 0.02% (1) | 4,463 | N/A | None (0 invalid / 100% valid) | None |
| `average_mpg` | `FLOAT / DECIMAL` | 0.00% (0) | 97.83% (4,367) | 97 | N/A | None (0 invalid / 100% valid) | None |
| `total_fuel_gallons` | `FLOAT / DECIMAL` | 0.00% (0) | 5.89% (263) | 4,201 | N/A | None (0 invalid / 100% valid) | None |
| `on_time_delivery_rate` | `FLOAT / DECIMAL` | 0.00% (0) | 95.81% (4,277) | 187 | N/A | None (0 invalid / 100% valid) | None |
| `average_idle_hours` | `FLOAT / DECIMAL` | 0.00% (0) | 98.86% (4,413) | 51 | N/A | None (0 invalid / 100% valid) | None |

#### Observations & Domain Integrity Insights
- **Completeness:** 100% complete dataset. Zero null or missing values across all columns.
- **Temporal Scope:** `month`: 2022-01-01 00:00:00 to 2024-12-01 00:00:00
- **Referential Integrity:** All foreign key constraints verified with 0 orphan keys (100% matched against parent primary keys).

---

## Table: `drivers`
*Fleet driver demographics, employment lifecycle (hire/termination), CDL licensing, home terminal, and years of experience.*

- **Source File:** `data/drivers.csv`
- **Total Row Count:** **150** rows
- **Total Column Count:** **12** columns
- **Duplicate Rows:** **0** (0.00%)
- **Primary Key:** `driver_id` (100% unique, 0 duplicate keys)
- **Foreign Key Relationships:** None (Root / Master Dimension Table)

### Column Specifications & Data Profiling

| Actual Column Name | Actual Data Type | Null % (Count) | Duplicate % (Count) | Unique Values | Date Format | Invalid Values | Foreign-Key Relationship |
| :--- | :--- | :---: | :---: | :---: | :---: | :--- | :--- |
| `driver_id` | `VARCHAR / STRING` | 0.00% (0) | 0.00% (0) | 150 | N/A | None (0 invalid / 100% valid) | **PRIMARY KEY** |
| `first_name` | `VARCHAR / STRING` | 0.00% (0) | 86.67% (130) | 20 | N/A | None (0 invalid / 100% valid) | None |
| `last_name` | `VARCHAR / STRING` | 0.00% (0) | 86.67% (130) | 20 | N/A | None (0 invalid / 100% valid) | None |
| `hire_date` | `DATE` | 0.00% (0) | 2.00% (3) | 147 | `YYYY-MM-DD` | None (0 invalid / 100% valid) | None |
| `termination_date` | `DATE` | 82.67% (124) | 0.00% (0) | 26 | `YYYY-MM-DD` | None (0 invalid / 100% valid) | None |
| `license_number` | `VARCHAR / STRING` | 0.00% (0) | 0.00% (0) | 150 | N/A | None (0 invalid / 100% valid) | None |
| `license_state` | `VARCHAR / STRING` | 0.00% (0) | 84.67% (127) | 23 | N/A | None (0 invalid / 100% valid) | None |
| `date_of_birth` | `DATE` | 0.00% (0) | 1.33% (2) | 148 | `YYYY-MM-DD` | None (0 invalid / 100% valid) | None |
| `home_terminal` | `VARCHAR / STRING` | 0.00% (0) | 83.33% (125) | 25 | N/A | None (0 invalid / 100% valid) | None |
| `employment_status` | `VARCHAR (ENUM - 2 values)` | 0.00% (0) | 98.67% (148) | 2 | N/A | None (0 invalid / 100% valid) | None |
| `cdl_class` | `VARCHAR (ENUM - 1 values)` | 0.00% (0) | 99.33% (149) | 1 | N/A | None (0 invalid / 100% valid) | None |
| `years_experience` | `INTEGER` | 0.00% (0) | 84.00% (126) | 24 | N/A | None (0 invalid / 100% valid) | None |

#### Observations & Domain Integrity Insights
- **Null `termination_date` (82.67%):** Expected business condition representing 124 currently active drivers (only 26 drivers have termination dates recorded).
- **Temporal Scope:** `hire_date`: 2012-01-28 00:00:00 to 2021-12-06 00:00:00; `termination_date`: 2013-01-08 00:00:00 to 2022-05-05 00:00:00; `date_of_birth`: 1960-05-24 00:00:00 to 1995-07-03 00:00:00
- **Categorical Enums:** `employment_status` (['Active', 'Terminated']); `cdl_class` (['A'])

---

## Table: `facilities`
*Physical distribution centers, cross-docks, terminals, and warehouses with geolocation, dock capacity, and operating hours.*

- **Source File:** `data/facilities.csv`
- **Total Row Count:** **50** rows
- **Total Column Count:** **9** columns
- **Duplicate Rows:** **0** (0.00%)
- **Primary Key:** `facility_id` (100% unique, 0 duplicate keys)
- **Foreign Key Relationships:** None (Root / Master Dimension Table)

### Column Specifications & Data Profiling

| Actual Column Name | Actual Data Type | Null % (Count) | Duplicate % (Count) | Unique Values | Date Format | Invalid Values | Foreign-Key Relationship |
| :--- | :--- | :---: | :---: | :---: | :---: | :--- | :--- |
| `facility_id` | `VARCHAR / STRING` | 0.00% (0) | 0.00% (0) | 50 | N/A | None (0 invalid / 100% valid) | **PRIMARY KEY** |
| `facility_name` | `VARCHAR / STRING` | 0.00% (0) | 30.00% (15) | 35 | N/A | None (0 invalid / 100% valid) | None |
| `facility_type` | `VARCHAR (ENUM - 4 values)` | 0.00% (0) | 92.00% (46) | 4 | N/A | None (0 invalid / 100% valid) | None |
| `city` | `VARCHAR / STRING` | 0.00% (0) | 58.00% (29) | 21 | N/A | None (0 invalid / 100% valid) | None |
| `state` | `VARCHAR / STRING` | 0.00% (0) | 60.00% (30) | 20 | N/A | None (0 invalid / 100% valid) | None |
| `latitude` | `FLOAT / DECIMAL` | 0.00% (0) | 58.00% (29) | 21 | N/A | None (0 invalid / 100% valid) | None |
| `longitude` | `FLOAT / DECIMAL` | 0.00% (0) | 58.00% (29) | 21 | N/A | None (0 invalid / 100% valid) | None |
| `dock_doors` | `INTEGER` | 0.00% (0) | 20.00% (10) | 40 | N/A | None (0 invalid / 100% valid) | None |
| `operating_hours` | `VARCHAR (ENUM - 4 values)` | 0.00% (0) | 92.00% (46) | 4 | N/A | None (0 invalid / 100% valid) | None |

#### Observations & Domain Integrity Insights
- **Completeness:** 100% complete dataset. Zero null or missing values across all columns.
- **Categorical Enums:** `facility_type` (['Cross-Dock', 'Distribution Center', 'Terminal', 'Warehouse']); `operating_hours` (['24/7', '7AM-7PM', '8AM-5PM', '6AM-10PM'])

---

## Table: `fuel_purchases`
*Transaction-level fuel ledger recording fuel stops, gallon volume, pump price per gallon, total expenditure, fuel card, and associated trip/equipment.*

- **Source File:** `data/fuel_purchases.csv`
- **Total Row Count:** **196,442** rows
- **Total Column Count:** **11** columns
- **Duplicate Rows:** **0** (0.00%)
- **Primary Key:** `fuel_purchase_id` (100% unique, 0 duplicate keys)
- **Foreign Key Relationships:** `trip_id` &rarr; `trips(trip_id)`, `truck_id` &rarr; `trucks(truck_id)`, `driver_id` &rarr; `drivers(driver_id)` *(100% referential integrity verified, 0 orphan keys)*

### Column Specifications & Data Profiling

| Actual Column Name | Actual Data Type | Null % (Count) | Duplicate % (Count) | Unique Values | Date Format | Invalid Values | Foreign-Key Relationship |
| :--- | :--- | :---: | :---: | :---: | :---: | :--- | :--- |
| `fuel_purchase_id` | `VARCHAR / STRING` | 0.00% (0) | 0.00% (0) | 196,442 | N/A | None (0 invalid / 100% valid) | **PRIMARY KEY** |
| `trip_id` | `VARCHAR / STRING` | 0.00% (0) | 60.83% (119,503) | 76,939 | N/A | None (0 invalid / 100% valid) | **FK** &rarr; `trips(trip_id)` |
| `truck_id` | `VARCHAR / STRING` | 1.98% (3,880) | 99.95% (192,470) | 92 | N/A | None (0 invalid / 100% valid) | **FK** &rarr; `trucks(truck_id)` |
| `driver_id` | `VARCHAR / STRING` | 2.03% (3,988) | 99.94% (192,330) | 124 | N/A | None (0 invalid / 100% valid) | **FK** &rarr; `drivers(driver_id)` |
| `purchase_date` | `TIMESTAMP` | 0.00% (0) | 86.60% (170,111) | 26,331 | `YYYY-MM-DD HH:MM:SS` | None (0 invalid / 100% valid) | None |
| `location_city` | `VARCHAR / STRING` | 0.00% (0) | 99.99% (196,417) | 25 | N/A | None (0 invalid / 100% valid) | None |
| `location_state` | `VARCHAR / STRING` | 0.00% (0) | 99.99% (196,419) | 23 | N/A | None (0 invalid / 100% valid) | None |
| `gallons` | `FLOAT / DECIMAL` | 0.00% (0) | 99.24% (194,941) | 1,501 | N/A | None (0 invalid / 100% valid) | None |
| `price_per_gallon` | `FLOAT / DECIMAL` | 0.00% (0) | 99.06% (194,591) | 1,851 | N/A | None (0 invalid / 100% valid) | None |
| `total_cost` | `FLOAT / DECIMAL` | 0.00% (0) | 67.36% (132,329) | 64,113 | N/A | None (0 invalid / 100% valid) | None |
| `fuel_card_number` | `VARCHAR / STRING` | 0.00% (0) | 10.08% (19,797) | 176,645 | N/A | None (0 invalid / 100% valid) | None |

#### Observations & Domain Integrity Insights
- **Null `truck_id` (1.98%):** Represents unassigned or unlinked asset records (3,880 rows), typical for unallocated runs or card purchases where driver/vehicle was not scanned at point of fuel swipe.
- **Null `driver_id` (2.03%):** Represents unassigned or unlinked asset records (3,988 rows), typical for unallocated runs or card purchases where driver/vehicle was not scanned at point of fuel swipe.
- **Temporal Scope:** `purchase_date`: 2022-01-01 00:00:00 to 2025-01-02 23:00:00
- **Referential Integrity:** All foreign key constraints verified with 0 orphan keys (100% matched against parent primary keys).

---

## Table: `loads`
*Freight shipment contracts containing customer details, assigned route, freight type, weight, pieces, revenue, and fuel surcharges.*

- **Source File:** `data/loads.csv`
- **Total Row Count:** **85,410** rows
- **Total Column Count:** **12** columns
- **Duplicate Rows:** **0** (0.00%)
- **Primary Key:** `load_id` (100% unique, 0 duplicate keys)
- **Foreign Key Relationships:** `customer_id` &rarr; `customers(customer_id)`, `route_id` &rarr; `routes(route_id)` *(100% referential integrity verified, 0 orphan keys)*

### Column Specifications & Data Profiling

| Actual Column Name | Actual Data Type | Null % (Count) | Duplicate % (Count) | Unique Values | Date Format | Invalid Values | Foreign-Key Relationship |
| :--- | :--- | :---: | :---: | :---: | :---: | :--- | :--- |
| `load_id` | `VARCHAR / STRING` | 0.00% (0) | 0.00% (0) | 85,410 | N/A | None (0 invalid / 100% valid) | **PRIMARY KEY** |
| `customer_id` | `VARCHAR / STRING` | 0.00% (0) | 99.77% (85,210) | 200 | N/A | None (0 invalid / 100% valid) | **FK** &rarr; `customers(customer_id)` |
| `route_id` | `VARCHAR / STRING` | 0.00% (0) | 99.93% (85,352) | 58 | N/A | None (0 invalid / 100% valid) | **FK** &rarr; `routes(route_id)` |
| `load_date` | `DATE` | 0.00% (0) | 98.72% (84,314) | 1,096 | `YYYY-MM-DD` | None (0 invalid / 100% valid) | None |
| `load_type` | `VARCHAR (ENUM - 2 values)` | 0.00% (0) | 100.00% (85,408) | 2 | N/A | None (0 invalid / 100% valid) | None |
| `weight_lbs` | `INTEGER` | 0.00% (0) | 62.62% (53,483) | 31,927 | N/A | None (0 invalid / 100% valid) | None |
| `pieces` | `INTEGER` | 0.00% (0) | 99.97% (85,382) | 28 | N/A | None (0 invalid / 100% valid) | None |
| `revenue` | `FLOAT / DECIMAL` | 0.00% (0) | 7.35% (6,280) | 79,130 | N/A | None (0 invalid / 100% valid) | None |
| `fuel_surcharge` | `FLOAT / DECIMAL` | 0.00% (0) | 99.93% (85,353) | 57 | N/A | None (0 invalid / 100% valid) | None |
| `accessorial_charges` | `INTEGER` | 0.00% (0) | 99.99% (85,404) | 6 | N/A | None (0 invalid / 100% valid) | None |
| `load_status` | `VARCHAR (ENUM - 1 values)` | 0.00% (0) | 100.00% (85,409) | 1 | N/A | None (0 invalid / 100% valid) | None |
| `booking_type` | `VARCHAR (ENUM - 3 values)` | 0.00% (0) | 100.00% (85,407) | 3 | N/A | None (0 invalid / 100% valid) | None |

#### Observations & Domain Integrity Insights
- **Completeness:** 100% complete dataset. Zero null or missing values across all columns.
- **Temporal Scope:** `load_date`: 2022-01-01 00:00:00 to 2024-12-31 00:00:00
- **Categorical Enums:** `load_type` (['Dry Van', 'Refrigerated']); `accessorial_charges` ([100, 0, 50, 75, 150, 200]); `load_status` (['Completed']); `booking_type` (['Spot', 'Dedicated', 'Contract'])
- **Referential Integrity:** All foreign key constraints verified with 0 orphan keys (100% matched against parent primary keys).

---

## Table: `maintenance_records`
*Equipment maintenance events detailing service type, odometer reading, downtime, parts/labor expenditures, and servicing facility.*

- **Source File:** `data/maintenance_records.csv`
- **Total Row Count:** **2,920** rows
- **Total Column Count:** **12** columns
- **Duplicate Rows:** **0** (0.00%)
- **Primary Key:** `maintenance_id` (100% unique, 0 duplicate keys)
- **Foreign Key Relationships:** `truck_id` &rarr; `trucks(truck_id)` *(100% referential integrity verified, 0 orphan keys)*

### Column Specifications & Data Profiling

| Actual Column Name | Actual Data Type | Null % (Count) | Duplicate % (Count) | Unique Values | Date Format | Invalid Values | Foreign-Key Relationship |
| :--- | :--- | :---: | :---: | :---: | :---: | :--- | :--- |
| `maintenance_id` | `VARCHAR / STRING` | 0.00% (0) | 0.00% (0) | 2,920 | N/A | None (0 invalid / 100% valid) | **PRIMARY KEY** |
| `truck_id` | `VARCHAR / STRING` | 0.00% (0) | 95.89% (2,800) | 120 | N/A | None (0 invalid / 100% valid) | **FK** &rarr; `trucks(truck_id)` |
| `maintenance_date` | `DATE` | 0.00% (0) | 65.07% (1,900) | 1,020 | `YYYY-MM-DD` | None (0 invalid / 100% valid) | None |
| `maintenance_type` | `VARCHAR (ENUM - 7 values)` | 0.00% (0) | 99.76% (2,913) | 7 | N/A | None (0 invalid / 100% valid) | None |
| `odometer_reading` | `INTEGER` | 0.00% (0) | 0.27% (8) | 2,912 | N/A | None (0 invalid / 100% valid) | None |
| `labor_hours` | `FLOAT / DECIMAL` | 0.00% (0) | 97.40% (2,844) | 76 | N/A | None (0 invalid / 100% valid) | None |
| `labor_cost` | `FLOAT / DECIMAL` | 0.00% (0) | 2.36% (69) | 2,851 | N/A | None (0 invalid / 100% valid) | None |
| `parts_cost` | `FLOAT / DECIMAL` | 0.00% (0) | 0.58% (17) | 2,903 | N/A | None (0 invalid / 100% valid) | None |
| `total_cost` | `FLOAT / DECIMAL` | 0.00% (0) | 0.21% (6) | 2,914 | N/A | None (0 invalid / 100% valid) | None |
| `facility_location` | `VARCHAR / STRING` | 0.00% (0) | 99.14% (2,895) | 25 | N/A | None (0 invalid / 100% valid) | None |
| `downtime_hours` | `FLOAT / DECIMAL` | 0.00% (0) | 84.25% (2,460) | 460 | N/A | None (0 invalid / 100% valid) | None |
| `service_description` | `VARCHAR / STRING` | 0.00% (0) | 99.28% (2,899) | 21 | N/A | None (0 invalid / 100% valid) | None |

#### Observations & Domain Integrity Insights
- **Completeness:** 100% complete dataset. Zero null or missing values across all columns.
- **Temporal Scope:** `maintenance_date`: 2022-01-01 00:00:00 to 2024-12-31 00:00:00
- **Categorical Enums:** `maintenance_type` (['Inspection', 'Tire', 'Preventive', 'Repair', 'Transmission', 'Brake', 'Engine'])
- **Referential Integrity:** All foreign key constraints verified with 0 orphan keys (100% matched against parent primary keys).

---

## Table: `routes`
*Origin-destination freight lanes with standard transit miles, baseline rate per mile, fuel surcharge index, and expected transit duration.*

- **Source File:** `data/routes.csv`
- **Total Row Count:** **58** rows
- **Total Column Count:** **9** columns
- **Duplicate Rows:** **0** (0.00%)
- **Primary Key:** `route_id` (100% unique, 0 duplicate keys)
- **Foreign Key Relationships:** None (Root / Master Dimension Table)

### Column Specifications & Data Profiling

| Actual Column Name | Actual Data Type | Null % (Count) | Duplicate % (Count) | Unique Values | Date Format | Invalid Values | Foreign-Key Relationship |
| :--- | :--- | :---: | :---: | :---: | :---: | :--- | :--- |
| `route_id` | `VARCHAR / STRING` | 0.00% (0) | 0.00% (0) | 58 | N/A | None (0 invalid / 100% valid) | **PRIMARY KEY** |
| `origin_city` | `VARCHAR / STRING` | 0.00% (0) | 68.97% (40) | 18 | N/A | None (0 invalid / 100% valid) | None |
| `origin_state` | `VARCHAR / STRING` | 0.00% (0) | 70.69% (41) | 17 | N/A | None (0 invalid / 100% valid) | None |
| `destination_city` | `VARCHAR / STRING` | 0.00% (0) | 67.24% (39) | 19 | N/A | None (0 invalid / 100% valid) | None |
| `destination_state` | `VARCHAR / STRING` | 0.00% (0) | 68.97% (40) | 18 | N/A | None (0 invalid / 100% valid) | None |
| `typical_distance_miles` | `INTEGER` | 0.00% (0) | 10.34% (6) | 52 | N/A | None (0 invalid / 100% valid) | None |
| `base_rate_per_mile` | `FLOAT / DECIMAL` | 0.00% (0) | 20.69% (12) | 46 | N/A | None (0 invalid / 100% valid) | None |
| `fuel_surcharge_rate` | `FLOAT / DECIMAL` | 0.00% (0) | 65.52% (38) | 20 | N/A | None (0 invalid / 100% valid) | None |
| `typical_transit_days` | `INTEGER` | 0.00% (0) | 91.38% (53) | 5 | N/A | None (0 invalid / 100% valid) | None |

#### Observations & Domain Integrity Insights
- **Completeness:** 100% complete dataset. Zero null or missing values across all columns.
- **Categorical Enums:** `typical_transit_days` ([1, 3, 2, 4, 5])

---

## Table: `safety_incidents`
*Comprehensive incident records documenting accidents, DOT/moving violations, equipment damages, preventable classifications, and financial claim amounts.*

- **Source File:** `data/safety_incidents.csv`
- **Total Row Count:** **170** rows
- **Total Column Count:** **15** columns
- **Duplicate Rows:** **0** (0.00%)
- **Primary Key:** `incident_id` (100% unique, 0 duplicate keys)
- **Foreign Key Relationships:** `trip_id` &rarr; `trips(trip_id)`, `truck_id` &rarr; `trucks(truck_id)`, `driver_id` &rarr; `drivers(driver_id)` *(100% referential integrity verified, 0 orphan keys)*

### Column Specifications & Data Profiling

| Actual Column Name | Actual Data Type | Null % (Count) | Duplicate % (Count) | Unique Values | Date Format | Invalid Values | Foreign-Key Relationship |
| :--- | :--- | :---: | :---: | :---: | :---: | :--- | :--- |
| `incident_id` | `VARCHAR / STRING` | 0.00% (0) | 0.00% (0) | 170 | N/A | None (0 invalid / 100% valid) | **PRIMARY KEY** |
| `trip_id` | `VARCHAR / STRING` | 0.00% (0) | 0.00% (0) | 170 | N/A | None (0 invalid / 100% valid) | **FK** &rarr; `trips(trip_id)` |
| `truck_id` | `VARCHAR / STRING` | 0.59% (1) | 52.66% (89) | 80 | N/A | None (0 invalid / 100% valid) | **FK** &rarr; `trucks(truck_id)` |
| `driver_id` | `VARCHAR / STRING` | 0.59% (1) | 45.56% (77) | 92 | N/A | None (0 invalid / 100% valid) | **FK** &rarr; `drivers(driver_id)` |
| `incident_date` | `TIMESTAMP` | 0.00% (0) | 0.59% (1) | 169 | `YYYY-MM-DD HH:MM:SS` | None (0 invalid / 100% valid) | None |
| `incident_type` | `VARCHAR (ENUM - 5 values)` | 0.00% (0) | 97.06% (165) | 5 | N/A | None (0 invalid / 100% valid) | None |
| `location_city` | `VARCHAR / STRING` | 0.00% (0) | 85.29% (145) | 25 | N/A | None (0 invalid / 100% valid) | None |
| `location_state` | `VARCHAR / STRING` | 0.00% (0) | 86.47% (147) | 23 | N/A | None (0 invalid / 100% valid) | None |
| `at_fault_flag` | `BOOLEAN` | 0.00% (0) | 98.82% (168) | 2 | N/A | None (0 invalid / 100% valid) | None |
| `injury_flag` | `BOOLEAN` | 0.00% (0) | 98.82% (168) | 2 | N/A | None (0 invalid / 100% valid) | None |
| `vehicle_damage_cost` | `FLOAT / DECIMAL` | 0.00% (0) | 30.00% (51) | 119 | N/A | None (0 invalid / 100% valid) | None |
| `cargo_damage_cost` | `FLOAT / DECIMAL` | 0.00% (0) | 71.18% (121) | 49 | N/A | None (0 invalid / 100% valid) | None |
| `claim_amount` | `FLOAT / DECIMAL` | 0.00% (0) | 22.35% (38) | 132 | N/A | None (0 invalid / 100% valid) | None |
| `preventable_flag` | `BOOLEAN` | 0.00% (0) | 98.82% (168) | 2 | N/A | None (0 invalid / 100% valid) | None |
| `description` | `VARCHAR / STRING` | 0.00% (0) | 92.94% (158) | 12 | N/A | None (0 invalid / 100% valid) | None |

#### Observations & Domain Integrity Insights
- **Null `truck_id` (0.59%):** Represents unassigned or unlinked asset records (1 rows), typical for unallocated runs or card purchases where driver/vehicle was not scanned at point of fuel swipe.
- **Null `driver_id` (0.59%):** Represents unassigned or unlinked asset records (1 rows), typical for unallocated runs or card purchases where driver/vehicle was not scanned at point of fuel swipe.
- **Temporal Scope:** `incident_date`: 2022-01-05 02:00:00 to 2024-12-24 09:00:00
- **Referential Integrity:** All foreign key constraints verified with 0 orphan keys (100% matched against parent primary keys).

---

## Table: `trailers`
*Trailer equipment inventory covering trailer type (dry van/refrigerated), dimensions, model year, VIN, and operational status.*

- **Source File:** `data/trailers.csv`
- **Total Row Count:** **180** rows
- **Total Column Count:** **9** columns
- **Duplicate Rows:** **0** (0.00%)
- **Primary Key:** `trailer_id` (100% unique, 0 duplicate keys)
- **Foreign Key Relationships:** None (Root / Master Dimension Table)

### Column Specifications & Data Profiling

| Actual Column Name | Actual Data Type | Null % (Count) | Duplicate % (Count) | Unique Values | Date Format | Invalid Values | Foreign-Key Relationship |
| :--- | :--- | :---: | :---: | :---: | :---: | :--- | :--- |
| `trailer_id` | `VARCHAR / STRING` | 0.00% (0) | 0.00% (0) | 180 | N/A | None (0 invalid / 100% valid) | **PRIMARY KEY** |
| `trailer_number` | `INTEGER` | 0.00% (0) | 2.22% (4) | 176 | N/A | None (0 invalid / 100% valid) | None |
| `trailer_type` | `VARCHAR (ENUM - 2 values)` | 0.00% (0) | 98.89% (178) | 2 | N/A | None (0 invalid / 100% valid) | None |
| `length_feet` | `INTEGER` | 0.00% (0) | 99.44% (179) | 1 | N/A | None (0 invalid / 100% valid) | None |
| `model_year` | `INTEGER` | 0.00% (0) | 94.44% (170) | 10 | N/A | None (0 invalid / 100% valid) | None |
| `vin` | `VARCHAR / STRING` | 0.00% (0) | 0.00% (0) | 180 | N/A | None (0 invalid / 100% valid) | None |
| `acquisition_date` | `DATE` | 0.00% (0) | 3.89% (7) | 173 | `YYYY-MM-DD` | None (0 invalid / 100% valid) | None |
| `status` | `VARCHAR (ENUM - 1 values)` | 0.00% (0) | 99.44% (179) | 1 | N/A | None (0 invalid / 100% valid) | None |
| `current_location` | `VARCHAR / STRING` | 0.00% (0) | 86.11% (155) | 25 | N/A | None (0 invalid / 100% valid) | None |

#### Observations & Domain Integrity Insights
- **Completeness:** 100% complete dataset. Zero null or missing values across all columns.
- **Temporal Scope:** `acquisition_date`: 2015-01-06 00:00:00 to 2021-12-16 00:00:00
- **Categorical Enums:** `trailer_type` (['Refrigerated', 'Dry Van']); `length_feet` ([53]); `model_year` ([2016, 2018, 2022, 2024, 2023, 2015, 2020, 2017, 2021, 2019]); `status` (['Active'])

---

## Table: `trips`
*Operational trip executions linking loads to drivers, trucks, and trailers, with real-world distance, duration, fuel burn, and idle time.*

- **Source File:** `data/trips.csv`
- **Total Row Count:** **85,410** rows
- **Total Column Count:** **12** columns
- **Duplicate Rows:** **0** (0.00%)
- **Primary Key:** `trip_id` (100% unique, 0 duplicate keys)
- **Foreign Key Relationships:** `load_id` &rarr; `loads(load_id)`, `driver_id` &rarr; `drivers(driver_id)`, `truck_id` &rarr; `trucks(truck_id)`, `trailer_id` &rarr; `trailers(trailer_id)` *(100% referential integrity verified, 0 orphan keys)*

### Column Specifications & Data Profiling

| Actual Column Name | Actual Data Type | Null % (Count) | Duplicate % (Count) | Unique Values | Date Format | Invalid Values | Foreign-Key Relationship |
| :--- | :--- | :---: | :---: | :---: | :---: | :--- | :--- |
| `trip_id` | `VARCHAR / STRING` | 0.00% (0) | 0.00% (0) | 85,410 | N/A | None (0 invalid / 100% valid) | **PRIMARY KEY** |
| `load_id` | `VARCHAR / STRING` | 0.00% (0) | 0.00% (0) | 85,410 | N/A | None (0 invalid / 100% valid) | **FK** &rarr; `loads(load_id)` |
| `driver_id` | `VARCHAR / STRING` | 2.01% (1,714) | 99.85% (83,572) | 124 | N/A | None (0 invalid / 100% valid) | **FK** &rarr; `drivers(driver_id)` |
| `truck_id` | `VARCHAR / STRING` | 1.96% (1,672) | 99.89% (83,646) | 92 | N/A | None (0 invalid / 100% valid) | **FK** &rarr; `trucks(truck_id)` |
| `trailer_id` | `VARCHAR / STRING` | 1.97% (1,680) | 99.79% (83,550) | 180 | N/A | None (0 invalid / 100% valid) | **FK** &rarr; `trailers(trailer_id)` |
| `dispatch_date` | `DATE` | 0.00% (0) | 98.72% (84,314) | 1,096 | `YYYY-MM-DD` | None (0 invalid / 100% valid) | None |
| `actual_distance_miles` | `INTEGER` | 0.00% (0) | 96.70% (82,592) | 2,818 | N/A | None (0 invalid / 100% valid) | None |
| `actual_duration_hours` | `FLOAT / DECIMAL` | 0.00% (0) | 99.26% (84,778) | 632 | N/A | None (0 invalid / 100% valid) | None |
| `fuel_gallons_used` | `FLOAT / DECIMAL` | 0.00% (0) | 93.86% (80,163) | 5,247 | N/A | None (0 invalid / 100% valid) | None |
| `average_mpg` | `FLOAT / DECIMAL` | 0.00% (0) | 99.76% (85,209) | 201 | N/A | None (0 invalid / 100% valid) | None |
| `idle_time_hours` | `FLOAT / DECIMAL` | 0.00% (0) | 99.88% (85,309) | 101 | N/A | None (0 invalid / 100% valid) | None |
| `trip_status` | `VARCHAR (ENUM - 1 values)` | 0.00% (0) | 100.00% (85,409) | 1 | N/A | None (0 invalid / 100% valid) | None |

#### Observations & Domain Integrity Insights
- **Null `driver_id` (2.01%):** Represents unassigned or unlinked asset records (1,714 rows), typical for unallocated runs or card purchases where driver/vehicle was not scanned at point of fuel swipe.
- **Null `truck_id` (1.96%):** Represents unassigned or unlinked asset records (1,672 rows), typical for unallocated runs or card purchases where driver/vehicle was not scanned at point of fuel swipe.
- **Null `trailer_id` (1.97%):** Represents unassigned or unlinked asset records (1,680 rows), typical for unallocated runs or card purchases where driver/vehicle was not scanned at point of fuel swipe.
- **Temporal Scope:** `dispatch_date`: 2022-01-01 00:00:00 to 2024-12-31 00:00:00
- **Categorical Enums:** `trip_status` (['Completed'])
- **Referential Integrity:** All foreign key constraints verified with 0 orphan keys (100% matched against parent primary keys).

---

## Table: `truck_utilization_metrics`
*Aggregated monthly fleet asset utilization tracking truck mileage, monthly revenue generated, downtime, maintenance frequency, and utilization rate.*

- **Source File:** `data/truck_utilization_metrics.csv`
- **Total Row Count:** **3,312** rows
- **Total Column Count:** **10** columns
- **Duplicate Rows:** **0** (0.00%)
- **Primary Key:** `truck_id, month` (100% unique, 0 duplicate keys)
- **Foreign Key Relationships:** `truck_id` &rarr; `trucks(truck_id)` *(100% referential integrity verified, 0 orphan keys)*

### Column Specifications & Data Profiling

| Actual Column Name | Actual Data Type | Null % (Count) | Duplicate % (Count) | Unique Values | Date Format | Invalid Values | Foreign-Key Relationship |
| :--- | :--- | :---: | :---: | :---: | :---: | :--- | :--- |
| `truck_id` | `VARCHAR / STRING` | 0.00% (0) | 97.22% (3,220) | 92 | N/A | None (0 invalid / 100% valid) | **COMPOSITE PK** |
| `month` | `DATE` | 0.00% (0) | 98.91% (3,276) | 36 | `YYYY-MM-DD` | None (0 invalid / 100% valid) | **COMPOSITE PK** |
| `trips_completed` | `INTEGER` | 0.00% (0) | 98.94% (3,277) | 35 | N/A | None (0 invalid / 100% valid) | None |
| `total_miles` | `INTEGER` | 0.00% (0) | 5.37% (178) | 3,134 | N/A | None (0 invalid / 100% valid) | None |
| `total_revenue` | `FLOAT / DECIMAL` | 0.00% (0) | 0.06% (2) | 3,310 | N/A | None (0 invalid / 100% valid) | None |
| `average_mpg` | `FLOAT / DECIMAL` | 0.00% (0) | 97.71% (3,236) | 76 | N/A | None (0 invalid / 100% valid) | None |
| `maintenance_events` | `INTEGER` | 0.00% (0) | 99.85% (3,307) | 5 | N/A | None (0 invalid / 100% valid) | None |
| `maintenance_cost` | `FLOAT / DECIMAL` | 0.00% (0) | 50.88% (1,685) | 1,627 | N/A | None (0 invalid / 100% valid) | None |
| `downtime_hours` | `FLOAT / DECIMAL` | 0.00% (0) | 80.22% (2,657) | 655 | N/A | None (0 invalid / 100% valid) | None |
| `utilization_rate` | `FLOAT / DECIMAL` | 0.00% (0) | 96.89% (3,209) | 103 | N/A | None (0 invalid / 100% valid) | None |

#### Observations & Domain Integrity Insights
- **Completeness:** 100% complete dataset. Zero null or missing values across all columns.
- **Temporal Scope:** `month`: 2022-01-01 00:00:00 to 2024-12-01 00:00:00
- **Categorical Enums:** `maintenance_events` ([2, 0, 1, 3, 4])
- **Referential Integrity:** All foreign key constraints verified with 0 orphan keys (100% matched against parent primary keys).

---

## Table: `trucks`
*Tractor fleet inventory specifying make, model year, VIN, tank capacity, acquisition mileage, status, and assigned terminal.*

- **Source File:** `data/trucks.csv`
- **Total Row Count:** **120** rows
- **Total Column Count:** **11** columns
- **Duplicate Rows:** **0** (0.00%)
- **Primary Key:** `truck_id` (100% unique, 0 duplicate keys)
- **Foreign Key Relationships:** None (Root / Master Dimension Table)

### Column Specifications & Data Profiling

| Actual Column Name | Actual Data Type | Null % (Count) | Duplicate % (Count) | Unique Values | Date Format | Invalid Values | Foreign-Key Relationship |
| :--- | :--- | :---: | :---: | :---: | :---: | :--- | :--- |
| `truck_id` | `VARCHAR / STRING` | 0.00% (0) | 0.00% (0) | 120 | N/A | None (0 invalid / 100% valid) | **PRIMARY KEY** |
| `unit_number` | `INTEGER` | 0.00% (0) | 0.00% (0) | 120 | N/A | None (0 invalid / 100% valid) | None |
| `make` | `VARCHAR (ENUM - 6 values)` | 0.00% (0) | 95.00% (114) | 6 | N/A | None (0 invalid / 100% valid) | None |
| `model_year` | `INTEGER` | 0.00% (0) | 94.17% (113) | 7 | N/A | None (0 invalid / 100% valid) | None |
| `vin` | `VARCHAR / STRING` | 0.00% (0) | 0.00% (0) | 120 | N/A | None (0 invalid / 100% valid) | None |
| `acquisition_date` | `DATE` | 0.00% (0) | 0.83% (1) | 119 | `YYYY-MM-DD` | None (0 invalid / 100% valid) | None |
| `acquisition_mileage` | `INTEGER` | 0.00% (0) | 0.00% (0) | 120 | N/A | None (0 invalid / 100% valid) | None |
| `fuel_type` | `VARCHAR (ENUM - 1 values)` | 0.00% (0) | 99.17% (119) | 1 | N/A | None (0 invalid / 100% valid) | None |
| `tank_capacity_gallons` | `INTEGER` | 0.00% (0) | 97.50% (117) | 3 | N/A | None (0 invalid / 100% valid) | None |
| `status` | `VARCHAR (ENUM - 3 values)` | 0.00% (0) | 97.50% (117) | 3 | N/A | None (0 invalid / 100% valid) | None |
| `home_terminal` | `VARCHAR / STRING` | 0.00% (0) | 80.00% (96) | 24 | N/A | None (0 invalid / 100% valid) | None |

#### Observations & Domain Integrity Insights
- **Completeness:** 100% complete dataset. Zero null or missing values across all columns.
- **Temporal Scope:** `acquisition_date`: 2015-01-27 00:00:00 to 2021-10-08 00:00:00
- **Categorical Enums:** `make` (['Peterbilt', 'Kenworth', 'Freightliner', 'Volvo', 'Mack', 'International']); `model_year` ([2016, 2015, 2018, 2017, 2019, 2021, 2020]); `fuel_type` (['Diesel']); `tank_capacity_gallons` ([200, 150, 250]); `status` (['Active', 'Maintenance', 'Inactive'])

---
