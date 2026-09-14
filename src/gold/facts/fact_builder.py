"""
Fact Model Builder for Logistics Gold Layer.
Transforms and joins validated Silver tables into high-performance Star Schema Fact tables:
- fact_trips (Joins trips + loads, calculates total charges and operational metrics)
- fact_delivery_events (Milestone events and detention minutes)
- fact_fuel_purchases (Fuel ledger)
- fact_maintenance (Fleet service history & costs)
- fact_safety_incidents (Incident claims & damage costs)
- agg_driver_monthly_performance
- agg_truck_utilization_performance
"""

import sys
import datetime
from pathlib import Path
from typing import Dict, Any, Optional
import pandas as pd
import numpy as np

# Ensure project root is in sys.path
PROJECT_ROOT = str(Path(__file__).resolve().parents[3])
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

def build_fact_trips(
    silver_trips: pd.DataFrame,
    silver_loads: pd.DataFrame,
    batch_id: str
) -> pd.DataFrame:
    """
    Joins trips with loads to produce the central operational fact table.
    Enriches with calculated total_revenue = freight_revenue + fuel_surcharge + accessorial_charges.
    """
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    
    # Merge trips and loads on load_id
    merged = pd.merge(
        silver_trips,
        silver_loads,
        on="load_id",
        how="inner",
        suffixes=("_trip", "_load")
    )

    # Calculate total revenue
    freight_rev = pd.to_numeric(merged.get("revenue", 0.0), errors="coerce").fillna(0.0)
    fuel_surch = pd.to_numeric(merged.get("fuel_surcharge", 0.0), errors="coerce").fillna(0.0)
    accessorial = pd.to_numeric(merged.get("accessorial_charges", 0), errors="coerce").fillna(0)
    merged["total_revenue"] = freight_rev + fuel_surch + accessorial
    merged["freight_revenue"] = freight_rev
    merged["fuel_surcharge"] = fuel_surch
    merged["accessorial_charges"] = accessorial.astype("int64")

    # Dates
    merged["dispatch_date"] = pd.to_datetime(merged["dispatch_date"]).dt.date
    if "load_date" in merged.columns:
        merged["load_date"] = pd.to_datetime(merged["load_date"]).dt.date

    merged["batch_id"] = batch_id
    merged["_gold_published_at"] = now_utc

    target_cols = [
        "trip_id", "load_id", "customer_id", "route_id", "driver_id", "truck_id", "trailer_id",
        "dispatch_date", "load_date", "load_type", "booking_type", "load_status", "trip_status",
        "weight_lbs", "pieces", "actual_distance_miles", "actual_duration_hours",
        "fuel_gallons_used", "average_mpg", "idle_time_hours",
        "freight_revenue", "fuel_surcharge", "accessorial_charges", "total_revenue",
        "batch_id", "_gold_published_at"
    ]
    return merged[[c for c in target_cols if c in merged.columns]]

def build_fact_delivery_events(silver_events: pd.DataFrame, batch_id: str) -> pd.DataFrame:
    """Builds fact_delivery_events with event_date partition column."""
    df = silver_events.copy()
    now_utc = datetime.datetime.now(datetime.timezone.utc)

    df["scheduled_datetime"] = pd.to_datetime(df["scheduled_datetime"])
    df["actual_datetime"] = pd.to_datetime(df["actual_datetime"])
    df["event_date"] = df["scheduled_datetime"].dt.date
    df["batch_id"] = batch_id
    df["_gold_published_at"] = now_utc

    target_cols = [
        "event_id", "load_id", "trip_id", "facility_id", "event_type",
        "scheduled_datetime", "actual_datetime", "event_date",
        "detention_minutes", "on_time_flag", "location_city", "location_state",
        "batch_id", "_gold_published_at"
    ]
    return df[[c for c in target_cols if c in df.columns]]

def build_fact_fuel_purchases(silver_fuel: pd.DataFrame, batch_id: str) -> pd.DataFrame:
    """Builds fact_fuel_purchases with purchase_date partition column."""
    df = silver_fuel.copy()
    now_utc = datetime.datetime.now(datetime.timezone.utc)

    df["purchase_datetime"] = pd.to_datetime(df["purchase_date"])
    df["purchase_date"] = df["purchase_datetime"].dt.date
    df["batch_id"] = batch_id
    df["_gold_published_at"] = now_utc

    target_cols = [
        "fuel_purchase_id", "trip_id", "truck_id", "driver_id",
        "purchase_datetime", "purchase_date", "location_city", "location_state",
        "gallons", "price_per_gallon", "total_cost", "fuel_card_number",
        "batch_id", "_gold_published_at"
    ]
    return df[[c for c in target_cols if c in df.columns]]

def build_fact_maintenance(silver_maint: pd.DataFrame, batch_id: str) -> pd.DataFrame:
    """Builds fact_maintenance with maintenance_date partition column."""
    df = silver_maint.copy()
    now_utc = datetime.datetime.now(datetime.timezone.utc)

    df["maintenance_date"] = pd.to_datetime(df["maintenance_date"]).dt.date
    df["batch_id"] = batch_id
    df["_gold_published_at"] = now_utc

    target_cols = [
        "maintenance_id", "truck_id", "maintenance_date", "maintenance_type",
        "odometer_reading", "labor_hours", "labor_cost", "parts_cost", "total_cost",
        "downtime_hours", "facility_location", "service_description",
        "batch_id", "_gold_published_at"
    ]
    return df[[c for c in target_cols if c in df.columns]]

def build_fact_safety_incidents(silver_incidents: pd.DataFrame, batch_id: str) -> pd.DataFrame:
    """Builds fact_safety_incidents with incident_date partition column."""
    df = silver_incidents.copy()
    now_utc = datetime.datetime.now(datetime.timezone.utc)

    df["incident_datetime"] = pd.to_datetime(df["incident_date"])
    df["incident_date"] = df["incident_datetime"].dt.date
    df["batch_id"] = batch_id
    df["_gold_published_at"] = now_utc

    target_cols = [
        "incident_id", "trip_id", "truck_id", "driver_id",
        "incident_datetime", "incident_date", "incident_type",
        "location_city", "location_state", "at_fault_flag", "injury_flag",
        "preventable_flag", "vehicle_damage_cost", "cargo_damage_cost",
        "claim_amount", "description", "batch_id", "_gold_published_at"
    ]
    return df[[c for c in target_cols if c in df.columns]]

def build_agg_driver_monthly_performance(silver_metrics: pd.DataFrame, batch_id: str) -> pd.DataFrame:
    """Builds agg_driver_monthly_performance with month partition column."""
    df = silver_metrics.copy()
    now_utc = datetime.datetime.now(datetime.timezone.utc)

    df["month"] = pd.to_datetime(df["month"]).dt.date
    df["batch_id"] = batch_id
    df["_gold_published_at"] = now_utc

    target_cols = [
        "driver_id", "month", "trips_completed", "total_miles", "total_revenue",
        "average_mpg", "total_fuel_gallons", "on_time_delivery_rate", "average_idle_hours",
        "batch_id", "_gold_published_at"
    ]
    return df[[c for c in target_cols if c in df.columns]]

def build_agg_truck_utilization_performance(silver_metrics: pd.DataFrame, batch_id: str) -> pd.DataFrame:
    """Builds agg_truck_utilization_performance with month partition column."""
    df = silver_metrics.copy()
    now_utc = datetime.datetime.now(datetime.timezone.utc)

    df["month"] = pd.to_datetime(df["month"]).dt.date
    df["batch_id"] = batch_id
    df["_gold_published_at"] = now_utc

    target_cols = [
        "truck_id", "month", "trips_completed", "total_miles", "total_revenue",
        "average_mpg", "maintenance_events", "maintenance_cost", "downtime_hours",
        "utilization_rate", "batch_id", "_gold_published_at"
    ]
    return df[[c for c in target_cols if c in df.columns]]

