-- ==============================================================================
-- Semantic BigQuery Views for Power BI Reporting & Analytics
-- Project: logistic-data-508513
-- Datasets: logistics_gold (Business Marts) & logistics_audit (Governance Marts)
-- ==============================================================================

-- ==============================================================================
-- 1. BUSINESS ANALYTICS VIEWS (logistics_gold)
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- View 1: v_bi_trip_details
-- Granularity: One row per trip
-- Purpose: Denormalized transactional view powering trip explorer & revenue analytics
-- ------------------------------------------------------------------------------
CREATE OR REPLACE VIEW `logistic-data-508513.logistics_gold.v_bi_trip_details` AS
SELECT
    t.trip_id,
    t.load_id,
    t.dispatch_date,
    t.load_date,
    t.load_type,
    t.booking_type,
    t.load_status,
    t.trip_status,
    t.weight_lbs,
    t.pieces,
    t.actual_distance_miles,
    t.actual_duration_hours,
    t.fuel_gallons_used,
    t.average_mpg,
    t.idle_time_hours,
    t.freight_revenue,
    t.fuel_surcharge,
    t.accessorial_charges,
    t.total_revenue,
    t.batch_id,
    
    -- Customer Attributes
    c.customer_id,
    c.customer_name,
    c.customer_type,
    c.primary_freight_type,
    c.account_status AS customer_account_status,
    
    -- Driver Attributes (Current Snapshot)
    d.driver_id,
    d.full_name AS driver_name,
    d.home_terminal AS driver_home_terminal,
    d.employment_status AS driver_status,
    d.years_experience AS driver_years_experience,
    
    -- Truck & Fleet Attributes
    tr.truck_id,
    tr.unit_number AS truck_unit_number,
    tr.make AS truck_make,
    tr.model_year AS truck_model_year,
    tr.fuel_type,
    
    -- Trailer Attributes
    tl.trailer_id,
    tl.trailer_type,
    tl.length_feet AS trailer_length_feet,
    
    -- Route Attributes
    r.route_id,
    r.origin_city,
    r.origin_state,
    r.destination_city,
    r.destination_state,
    CONCAT(r.origin_city, ', ', r.origin_state, ' -> ', r.destination_city, ', ', r.destination_state) AS route_lane_name,
    r.typical_distance_miles,
    r.base_rate_per_mile,
    r.typical_transit_days,
    
    -- Derived Financial & Operational Metrics
    SAFE_DIVIDE(t.total_revenue, NULLIF(t.actual_distance_miles, 0)) AS revenue_per_mile,
    SAFE_DIVIDE(t.fuel_gallons_used, NULLIF(t.actual_distance_miles, 0)) AS fuel_gallons_per_mile,
    SAFE_DIVIDE(t.actual_distance_miles, NULLIF(t.actual_duration_hours, 0)) AS average_speed_mph,
    (t.actual_distance_miles - r.typical_distance_miles) AS route_variance_miles

FROM `logistic-data-508513.logistics_gold.fact_trips` t
LEFT JOIN `logistic-data-508513.logistics_gold.dim_customer` c
    ON t.customer_id = c.customer_id
LEFT JOIN `logistic-data-508513.logistics_gold.dim_driver` d
    ON t.driver_id = d.driver_id AND d.is_current = TRUE
LEFT JOIN `logistic-data-508513.logistics_gold.dim_truck` tr
    ON t.truck_id = tr.truck_id
LEFT JOIN `logistic-data-508513.logistics_gold.dim_trailer` tl
    ON t.trailer_id = tl.trailer_id
LEFT JOIN `logistic-data-508513.logistics_gold.dim_route` r
    ON t.route_id = r.route_id;


-- ------------------------------------------------------------------------------
-- View 2: v_bi_monthly_fleet_metrics
-- Granularity: One row per truck per month
-- Purpose: Vehicle health, maintenance downtime, fuel expenditure, and utilization
-- ------------------------------------------------------------------------------
CREATE OR REPLACE VIEW `logistic-data-508513.logistics_gold.v_bi_monthly_fleet_metrics` AS
WITH monthly_fuel AS (
    SELECT
        truck_id,
        DATE_TRUNC(purchase_date, MONTH) AS month,
        SUM(gallons) AS total_fuel_gallons,
        SUM(total_cost) AS total_fuel_cost,
        SAFE_DIVIDE(SUM(total_cost), NULLIF(SUM(gallons), 0)) AS avg_price_per_gallon
    FROM `logistic-data-508513.logistics_gold.fact_fuel_purchases`
    GROUP BY truck_id, month
),
monthly_maint AS (
    SELECT
        truck_id,
        DATE_TRUNC(maintenance_date, MONTH) AS month,
        COUNT(DISTINCT maintenance_id) AS maintenance_event_count,
        SUM(labor_cost) AS total_labor_cost,
        SUM(parts_cost) AS total_parts_cost,
        SUM(total_cost) AS total_maintenance_cost,
        SUM(downtime_hours) AS total_downtime_hours
    FROM `logistic-data-508513.logistics_gold.fact_maintenance`
    GROUP BY truck_id, month
)
SELECT
    u.truck_id,
    u.month,
    u.trips_completed,
    u.total_miles,
    u.total_revenue,
    u.average_mpg,
    u.utilization_rate,
    
    -- Truck Dimension details
    tr.unit_number,
    tr.make,
    tr.model_year,
    tr.status AS truck_status,
    tr.home_terminal,
    
    -- Maintenance metrics
    COALESCE(m.maintenance_event_count, u.maintenance_events, 0) AS maintenance_event_count,
    COALESCE(m.total_maintenance_cost, u.maintenance_cost, 0.0) AS total_maintenance_cost,
    COALESCE(m.total_downtime_hours, u.downtime_hours, 0.0) AS total_downtime_hours,
    COALESCE(m.total_labor_cost, 0.0) AS total_labor_cost,
    COALESCE(m.total_parts_cost, 0.0) AS total_parts_cost,
    
    -- Fuel metrics
    COALESCE(f.total_fuel_gallons, 0.0) AS total_fuel_gallons,
    COALESCE(f.total_fuel_cost, 0.0) AS total_fuel_cost,
    COALESCE(f.avg_price_per_gallon, 0.0) AS avg_fuel_price_per_gallon,
    
    -- Operating Cost & Net Margin Estimation
    (COALESCE(f.total_fuel_cost, 0.0) + COALESCE(m.total_maintenance_cost, u.maintenance_cost, 0.0)) AS total_operating_cost,
    u.total_revenue - (COALESCE(f.total_fuel_cost, 0.0) + COALESCE(m.total_maintenance_cost, u.maintenance_cost, 0.0)) AS estimated_operating_margin,
    SAFE_DIVIDE(
        u.total_revenue - (COALESCE(f.total_fuel_cost, 0.0) + COALESCE(m.total_maintenance_cost, u.maintenance_cost, 0.0)),
        NULLIF(u.total_revenue, 0)
    ) AS operating_margin_pct,
    SAFE_DIVIDE(
        (COALESCE(f.total_fuel_cost, 0.0) + COALESCE(m.total_maintenance_cost, u.maintenance_cost, 0.0)),
        NULLIF(u.total_miles, 0)
    ) AS cost_per_mile

FROM `logistic-data-508513.logistics_gold.agg_truck_utilization_performance` u
LEFT JOIN `logistic-data-508513.logistics_gold.dim_truck` tr
    ON u.truck_id = tr.truck_id
LEFT JOIN monthly_fuel f
    ON u.truck_id = f.truck_id AND u.month = f.month
LEFT JOIN monthly_maint m
    ON u.truck_id = m.truck_id AND u.month = m.month;


-- ------------------------------------------------------------------------------
-- View 3: v_bi_driver_scorecard
-- Granularity: One row per driver per month
-- Purpose: Driver performance league table (safety, MPG, on-time delivery, revenue)
-- ------------------------------------------------------------------------------
CREATE OR REPLACE VIEW `logistic-data-508513.logistics_gold.v_bi_driver_scorecard` AS
WITH monthly_safety AS (
    SELECT
        driver_id,
        DATE_TRUNC(incident_date, MONTH) AS month,
        COUNT(incident_id) AS total_incidents,
        COUNTIF(preventable_flag = TRUE) AS preventable_incidents,
        COUNTIF(at_fault_flag = TRUE) AS at_fault_incidents,
        COUNTIF(injury_flag = TRUE) AS injury_incidents,
        SUM(vehicle_damage_cost + cargo_damage_cost) AS total_damage_cost,
        SUM(claim_amount) AS total_claim_amount
    FROM `logistic-data-508513.logistics_gold.fact_safety_incidents`
    GROUP BY driver_id, month
)
SELECT
    p.driver_id,
    p.month,
    p.trips_completed,
    p.total_miles,
    p.total_revenue,
    p.average_mpg,
    p.total_fuel_gallons,
    p.on_time_delivery_rate,
    p.average_idle_hours,
    
    -- Driver Profile
    d.full_name AS driver_name,
    d.home_terminal,
    d.cdl_class,
    d.years_experience,
    d.employment_status,
    
    -- Safety Metrics
    COALESCE(s.total_incidents, 0) AS total_incidents,
    COALESCE(s.preventable_incidents, 0) AS preventable_incidents,
    COALESCE(s.at_fault_incidents, 0) AS at_fault_incidents,
    COALESCE(s.injury_incidents, 0) AS injury_incidents,
    COALESCE(s.total_damage_cost, 0.0) AS total_damage_cost,
    COALESCE(s.total_claim_amount, 0.0) AS total_claim_amount,
    
    -- Safety Index (Incidents per 100,000 miles)
    SAFE_DIVIDE(COALESCE(s.total_incidents, 0) * 100000.0, NULLIF(p.total_miles, 0)) AS incidents_per_100k_miles,
    
    -- Commercial Productivity
    SAFE_DIVIDE(p.total_revenue, NULLIF(p.trips_completed, 0)) AS avg_revenue_per_trip,
    SAFE_DIVIDE(p.total_revenue, NULLIF(p.total_miles, 0)) AS revenue_per_mile

FROM `logistic-data-508513.logistics_gold.agg_driver_monthly_performance` p
LEFT JOIN `logistic-data-508513.logistics_gold.dim_driver` d
    ON p.driver_id = d.driver_id AND d.is_current = TRUE
LEFT JOIN monthly_safety s
    ON p.driver_id = s.driver_id AND p.month = s.month;


-- ------------------------------------------------------------------------------
-- View 4: v_bi_route_profitability
-- Granularity: One row per route lane
-- Purpose: Lane volume, revenue generation, distance variance, and transit efficiency
-- ------------------------------------------------------------------------------
CREATE OR REPLACE VIEW `logistic-data-508513.logistics_gold.v_bi_route_profitability` AS
SELECT
    r.route_id,
    r.origin_city,
    r.origin_state,
    r.destination_city,
    r.destination_state,
    CONCAT(r.origin_city, ', ', r.origin_state, ' -> ', r.destination_city, ', ', r.destination_state) AS route_lane,
    r.typical_distance_miles,
    r.base_rate_per_mile,
    r.typical_transit_days,
    
    COUNT(t.trip_id) AS total_trips_run,
    SUM(t.actual_distance_miles) AS total_miles_driven,
    SUM(t.weight_lbs) AS total_weight_carried_lbs,
    SUM(t.freight_revenue) AS total_freight_revenue,
    SUM(t.fuel_surcharge) AS total_fuel_surcharge,
    SUM(t.total_revenue) AS total_revenue_generated,
    
    AVG(t.actual_distance_miles) AS avg_actual_miles,
    AVG(t.actual_duration_hours) AS avg_actual_duration_hours,
    AVG(t.average_mpg) AS avg_lane_mpg,
    AVG(t.idle_time_hours) AS avg_idle_hours_per_trip,
    
    SAFE_DIVIDE(SUM(t.total_revenue), NULLIF(SUM(t.actual_distance_miles), 0)) AS lane_revenue_per_mile,
    SAFE_DIVIDE(SUM(t.total_revenue), NULLIF(COUNT(t.trip_id), 0)) AS avg_revenue_per_trip,
    (AVG(t.actual_distance_miles) - r.typical_distance_miles) AS avg_lane_mileage_variance

FROM `logistic-data-508513.logistics_gold.dim_route` r
LEFT JOIN `logistic-data-508513.logistics_gold.fact_trips` t
    ON r.route_id = t.route_id
GROUP BY
    r.route_id,
    r.origin_city,
    r.origin_state,
    r.destination_city,
    r.destination_state,
    r.typical_distance_miles,
    r.base_rate_per_mile,
    r.typical_transit_days;


-- ------------------------------------------------------------------------------
-- View 5: v_bi_facility_turnaround
-- Granularity: One row per facility per month
-- Purpose: Facility turnaround, dwell/detention time, and on-time performance
-- ------------------------------------------------------------------------------
CREATE OR REPLACE VIEW `logistic-data-508513.logistics_gold.v_bi_facility_turnaround` AS
SELECT
    f.facility_id,
    f.facility_name,
    f.facility_type,
    f.city,
    f.state,
    f.dock_doors,
    f.latitude,
    f.longitude,
    DATE_TRUNC(e.event_date, MONTH) AS month,
    
    COUNT(e.event_id) AS total_events,
    COUNTIF(e.event_type = 'Pickup') AS pickup_events,
    COUNTIF(e.event_type = 'Delivery') AS delivery_events,
    COUNTIF(e.on_time_flag = TRUE) AS on_time_events,
    COUNTIF(e.on_time_flag = FALSE) AS delayed_events,
    
    SAFE_DIVIDE(COUNTIF(e.on_time_flag = TRUE), NULLIF(COUNT(e.event_id), 0)) * 100.0 AS on_time_percentage,
    SUM(e.detention_minutes) AS total_detention_minutes,
    AVG(e.detention_minutes) AS avg_detention_minutes_per_visit,
    COUNTIF(e.detention_minutes > 120) AS severe_detention_count -- Industry standard threshold: >2 hours

FROM `logistic-data-508513.logistics_gold.dim_facility` f
LEFT JOIN `logistic-data-508513.logistics_gold.fact_delivery_events` e
    ON f.facility_id = e.facility_id
GROUP BY
    f.facility_id,
    f.facility_name,
    f.facility_type,
    f.city,
    f.state,
    f.dock_doors,
    f.latitude,
    f.longitude,
    month;


-- ==============================================================================
-- 2. DATA QUALITY & PIPELINE AUDIT VIEWS (logistics_audit)
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- View 6: v_bi_pipeline_run_summary
-- Granularity: One row per batch per medallion stage
-- Purpose: Pipeline execution tracking, stage duration, throughput, and status
-- ------------------------------------------------------------------------------
CREATE OR REPLACE VIEW `logistic-data-508513.logistics_audit.v_bi_pipeline_run_summary` AS
SELECT
    pa.batch_id,
    pa.stage,
    pa.status,
    COUNT(DISTINCT pa.table_name) AS tables_processed_count,
    SUM(pa.records_in) AS total_records_in,
    SUM(pa.records_out) AS total_records_out,
    SUM(pa.records_quarantined) AS total_records_quarantined,
    SAFE_DIVIDE(SUM(pa.records_quarantined), NULLIF(SUM(pa.records_in), 0)) * 100.0 AS stage_rejection_rate_pct,
    MIN(pa.start_time) AS stage_start_time,
    MAX(pa.end_time) AS stage_end_time,
    ROUND(SUM(pa.duration_seconds), 2) AS total_duration_seconds,
    ROUND(SUM(pa.duration_seconds) / 60.0, 2) AS total_duration_minutes

FROM `logistic-data-508513.logistics_audit.pipeline_audit` pa
GROUP BY
    pa.batch_id,
    pa.stage,
    pa.status;


-- ------------------------------------------------------------------------------
-- View 7: v_bi_table_dq_history
-- Granularity: One row per table per batch
-- Purpose: Table-level Data Quality scorecard, score trends, and pass/fail volume
-- ------------------------------------------------------------------------------
CREATE OR REPLACE VIEW `logistic-data-508513.logistics_audit.v_bi_table_dq_history` AS
SELECT
    dq.batch_id,
    dq.table_name,
    dq.total_records,
    dq.valid_records,
    dq.quarantined_records,
    dq.data_quality_score_pct,
    SAFE_DIVIDE(dq.quarantined_records, NULLIF(dq.total_records, 0)) * 100.0 AS rejection_rate_pct,
    dq.evaluated_at,
    CASE
        WHEN dq.data_quality_score_pct >= 99.0 THEN 'EXCELLENT'
        WHEN dq.data_quality_score_pct >= 95.0 THEN 'GOOD'
        WHEN dq.data_quality_score_pct >= 80.0 THEN 'ACCEPTABLE'
        ELSE 'CRITICAL_BREACH'
    END AS dq_health_category

FROM `logistic-data-508513.logistics_audit.data_quality_metrics` dq;


-- ------------------------------------------------------------------------------
-- View 8: v_bi_quarantine_deep_dive
-- Granularity: Aggregated breakdown of rejections by table, rule, and reason
-- Purpose: Root-cause analysis of quarantined records
-- ------------------------------------------------------------------------------
CREATE OR REPLACE VIEW `logistic-data-508513.logistics_audit.v_bi_quarantine_deep_dive` AS
SELECT
    rl.batch_id,
    rl.table_name,
    rl.rule_id,
    rl.rule_type,
    rl.rejection_reason,
    COUNT(1) AS rejection_count,
    MIN(rl.rejected_at) AS first_detected_at,
    MAX(rl.rejected_at) AS last_detected_at

FROM `logistic-data-508513.logistics_audit.rejection_log` rl
GROUP BY
    rl.batch_id,
    rl.table_name,
    rl.rule_id,
    rl.rule_type,
    rl.rejection_reason;

