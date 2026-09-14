-- ==============================================================================
-- DDL for Logistics Gold Medallion Layer (Star Schema in BigQuery)
-- Dataset: logistics_gold
-- Features: Partitioning (by event/transaction dates) and Clustering (by high-cardinality keys)
-- ==============================================================================

-- 1. DIMENSION TABLES
-- ------------------------------------------------------------------------------

-- Dim Date (Calendar Dimension)
CREATE TABLE IF NOT EXISTS `logistic-data-508513.logistics_gold.dim_date` (
    date_key INT64 NOT NULL,
    full_date DATE NOT NULL,
    day_of_week INT64,
    day_name STRING,
    day_of_month INT64,
    month INT64,
    month_name STRING,
    quarter INT64,
    year INT64,
    is_weekend BOOLEAN
);

-- Dim Customer
CREATE TABLE IF NOT EXISTS `logistic-data-508513.logistics_gold.dim_customer` (
    customer_sk INT64 NOT NULL,
    customer_id STRING NOT NULL,
    customer_name STRING,
    customer_type STRING,
    credit_terms_days INT64,
    primary_freight_type STRING,
    account_status STRING,
    contract_start_date DATE,
    annual_revenue_potential INT64,
    _created_at TIMESTAMP
);

-- Dim Driver (SCD Type 2 Capable)
CREATE TABLE IF NOT EXISTS `logistic-data-508513.logistics_gold.dim_driver` (
    driver_sk INT64 NOT NULL,
    driver_id STRING NOT NULL,
    first_name STRING,
    last_name STRING,
    full_name STRING,
    license_number STRING,
    license_state STRING,
    date_of_birth DATE,
    home_terminal STRING,
    employment_status STRING,
    cdl_class STRING,
    years_experience INT64,
    hire_date DATE,
    termination_date DATE,
    valid_from DATE NOT NULL,
    valid_to DATE,
    is_current BOOLEAN NOT NULL,
    _created_at TIMESTAMP
);

-- Dim Truck
CREATE TABLE IF NOT EXISTS `logistic-data-508513.logistics_gold.dim_truck` (
    truck_sk INT64 NOT NULL,
    truck_id STRING NOT NULL,
    unit_number INT64,
    make STRING,
    model_year INT64,
    vin STRING,
    acquisition_date DATE,
    acquisition_mileage INT64,
    fuel_type STRING,
    tank_capacity_gallons INT64,
    status STRING,
    home_terminal STRING,
    _created_at TIMESTAMP
);

-- Dim Trailer
CREATE TABLE IF NOT EXISTS `logistic-data-508513.logistics_gold.dim_trailer` (
    trailer_sk INT64 NOT NULL,
    trailer_id STRING NOT NULL,
    trailer_number INT64,
    trailer_type STRING,
    length_feet INT64,
    model_year INT64,
    vin STRING,
    acquisition_date DATE,
    status STRING,
    current_location STRING,
    _created_at TIMESTAMP
);

-- Dim Facility
CREATE TABLE IF NOT EXISTS `logistic-data-508513.logistics_gold.dim_facility` (
    facility_sk INT64 NOT NULL,
    facility_id STRING NOT NULL,
    facility_name STRING,
    facility_type STRING,
    city STRING,
    state STRING,
    latitude FLOAT64,
    longitude FLOAT64,
    dock_doors INT64,
    operating_hours STRING,
    _created_at TIMESTAMP
);

-- Dim Route
CREATE TABLE IF NOT EXISTS `logistic-data-508513.logistics_gold.dim_route` (
    route_sk INT64 NOT NULL,
    route_id STRING NOT NULL,
    origin_city STRING,
    origin_state STRING,
    destination_city STRING,
    destination_state STRING,
    typical_distance_miles INT64,
    base_rate_per_mile FLOAT64,
    fuel_surcharge_rate FLOAT64,
    typical_transit_days INT64,
    _created_at TIMESTAMP
);


-- 2. FACT TABLES (PARTITIONED & CLUSTERED)
-- ------------------------------------------------------------------------------

-- Fact Trips (Core Operational Fact - Joins Trip Execution & Load Contract)
CREATE TABLE IF NOT EXISTS `logistic-data-508513.logistics_gold.fact_trips` (
    trip_id STRING NOT NULL,
    load_id STRING NOT NULL,
    customer_id STRING,
    route_id STRING,
    driver_id STRING,
    truck_id STRING,
    trailer_id STRING,
    dispatch_date DATE NOT NULL,
    load_date DATE,
    load_type STRING,
    booking_type STRING,
    load_status STRING,
    trip_status STRING,
    weight_lbs INT64,
    pieces INT64,
    actual_distance_miles INT64,
    actual_duration_hours FLOAT64,
    fuel_gallons_used FLOAT64,
    average_mpg FLOAT64,
    idle_time_hours FLOAT64,
    freight_revenue FLOAT64,
    fuel_surcharge FLOAT64,
    accessorial_charges INT64,
    total_revenue FLOAT64,
    batch_id STRING,
    _gold_published_at TIMESTAMP
)
PARTITION BY dispatch_date
CLUSTER BY driver_id, truck_id, customer_id, route_id;

-- Fact Delivery Events (Milestone & Detention Tracking)
CREATE TABLE IF NOT EXISTS `logistic-data-508513.logistics_gold.fact_delivery_events` (
    event_id STRING NOT NULL,
    load_id STRING NOT NULL,
    trip_id STRING NOT NULL,
    facility_id STRING NOT NULL,
    event_type STRING NOT NULL,
    scheduled_datetime TIMESTAMP NOT NULL,
    actual_datetime TIMESTAMP NOT NULL,
    event_date DATE NOT NULL,
    detention_minutes INT64,
    on_time_flag BOOLEAN,
    location_city STRING,
    location_state STRING,
    batch_id STRING,
    _gold_published_at TIMESTAMP
)
PARTITION BY event_date
CLUSTER BY facility_id, event_type;

-- Fact Fuel Purchases (Fuel Expenditure Ledger)
CREATE TABLE IF NOT EXISTS `logistic-data-508513.logistics_gold.fact_fuel_purchases` (
    fuel_purchase_id STRING NOT NULL,
    trip_id STRING NOT NULL,
    truck_id STRING,
    driver_id STRING,
    purchase_datetime TIMESTAMP NOT NULL,
    purchase_date DATE NOT NULL,
    location_city STRING,
    location_state STRING,
    gallons FLOAT64,
    price_per_gallon FLOAT64,
    total_cost FLOAT64,
    fuel_card_number STRING,
    batch_id STRING,
    _gold_published_at TIMESTAMP
)
PARTITION BY purchase_date
CLUSTER BY truck_id, driver_id, location_state;

-- Fact Maintenance (Fleet Servicing & Downtime Costs)
CREATE TABLE IF NOT EXISTS `logistic-data-508513.logistics_gold.fact_maintenance` (
    maintenance_id STRING NOT NULL,
    truck_id STRING NOT NULL,
    maintenance_date DATE NOT NULL,
    maintenance_type STRING NOT NULL,
    odometer_reading INT64,
    labor_hours FLOAT64,
    labor_cost FLOAT64,
    parts_cost FLOAT64,
    total_cost FLOAT64,
    downtime_hours FLOAT64,
    facility_location STRING,
    service_description STRING,
    batch_id STRING,
    _gold_published_at TIMESTAMP
)
PARTITION BY maintenance_date
CLUSTER BY truck_id, maintenance_type;

-- Fact Safety Incidents (Risk, Claims & Damage Costs)
CREATE TABLE IF NOT EXISTS `logistic-data-508513.logistics_gold.fact_safety_incidents` (
    incident_id STRING NOT NULL,
    trip_id STRING NOT NULL,
    truck_id STRING,
    driver_id STRING,
    incident_datetime TIMESTAMP NOT NULL,
    incident_date DATE NOT NULL,
    incident_type STRING NOT NULL,
    location_city STRING,
    location_state STRING,
    at_fault_flag BOOLEAN,
    injury_flag BOOLEAN,
    preventable_flag BOOLEAN,
    vehicle_damage_cost FLOAT64,
    cargo_damage_cost FLOAT64,
    claim_amount FLOAT64,
    description STRING,
    batch_id STRING,
    _gold_published_at TIMESTAMP
)
PARTITION BY incident_date
CLUSTER BY driver_id, truck_id, incident_type;


-- 3. AGGREGATED SUMMARY PERFORMANCE MARTS
-- ------------------------------------------------------------------------------

-- Monthly Driver Performance
CREATE TABLE IF NOT EXISTS `logistic-data-508513.logistics_gold.agg_driver_monthly_performance` (
    driver_id STRING NOT NULL,
    month DATE NOT NULL,
    trips_completed INT64,
    total_miles INT64,
    total_revenue FLOAT64,
    average_mpg FLOAT64,
    total_fuel_gallons FLOAT64,
    on_time_delivery_rate FLOAT64,
    average_idle_hours FLOAT64,
    batch_id STRING,
    _gold_published_at TIMESTAMP
)
PARTITION BY month
CLUSTER BY driver_id;

-- Monthly Truck Utilization Performance
CREATE TABLE IF NOT EXISTS `logistic-data-508513.logistics_gold.agg_truck_utilization_performance` (
    truck_id STRING NOT NULL,
    month DATE NOT NULL,
    trips_completed INT64,
    total_miles INT64,
    total_revenue FLOAT64,
    average_mpg FLOAT64,
    maintenance_events INT64,
    maintenance_cost FLOAT64,
    downtime_hours FLOAT64,
    utilization_rate FLOAT64,
    batch_id STRING,
    _gold_published_at TIMESTAMP
)
PARTITION BY month
CLUSTER BY truck_id;

