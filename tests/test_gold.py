"""
Tests for Gold Layer: Dimension Builders, Fact Builders, and SCD Type 2 logic.
"""

import pytest
import datetime
import pandas as pd
from src.gold.dimensions.dim_builder import build_dim_date, build_dim_driver, build_dim_customer
from src.gold.facts.fact_builder import build_fact_trips, build_fact_delivery_events

def test_dim_date_generation():
    df = build_dim_date(2022, 2023)
    assert len(df) == 730 # 365 * 2
    assert "date_key" in df.columns
    assert "is_weekend" in df.columns
    assert df.iloc[0]["date_key"] == 20220101
    assert df.iloc[0]["year"] == 2022

def test_dim_driver_scd2_initial_and_update():
    # 1. Initial Load
    drivers_v1 = pd.DataFrame([
        {
            "driver_id": "DRV001",
            "first_name": "John",
            "last_name": "Doe",
            "hire_date": "2020-01-15",
            "employment_status": "Active",
            "termination_date": None
        }
    ])
    dim_v1 = build_dim_driver(drivers_v1)
    assert len(dim_v1) == 1
    assert dim_v1.iloc[0]["driver_sk"] == 1
    assert dim_v1.iloc[0]["is_current"] == True
    assert dim_v1.iloc[0]["valid_to"] is None

    # 2. Incoming Update (Terminated driver)
    drivers_v2 = pd.DataFrame([
        {
            "driver_id": "DRV001",
            "first_name": "John",
            "last_name": "Doe",
            "hire_date": "2020-01-15",
            "employment_status": "Terminated",
            "termination_date": "2023-06-30"
        }
    ])
    dim_v2 = build_dim_driver(drivers_v2, existing_dim=dim_v1)
    # Should now have 2 records: 1 expired historical record and 1 active current record
    assert len(dim_v2) == 2
    old_rec = dim_v2[dim_v2["is_current"] == False].iloc[0]
    new_rec = dim_v2[dim_v2["is_current"] == True].iloc[0]

    assert old_rec["employment_status"] == "Active"
    assert old_rec["valid_to"] is not None
    assert new_rec["employment_status"] == "Terminated"
    assert new_rec["driver_sk"] == 2
    assert new_rec["valid_to"] is None

def test_fact_trips_join_and_metrics():
    trips = pd.DataFrame([
        {
            "trip_id": "TRIP_101",
            "load_id": "LD_101",
            "driver_id": "DRV001",
            "truck_id": "TRK001",
            "dispatch_date": "2022-03-01",
            "actual_distance_miles": 500,
            "actual_duration_hours": 9.5,
            "fuel_gallons_used": 75.0,
            "average_mpg": 6.67,
            "idle_time_hours": 1.2
        }
    ])
    loads = pd.DataFrame([
        {
            "load_id": "LD_101",
            "customer_id": "CUST_001",
            "route_id": "RT_001",
            "revenue": 1500.0,
            "fuel_surcharge": 200.0,
            "accessorial_charges": 50,
            "weight_lbs": 32000,
            "pieces": 12
        }
    ])

    fact_df = build_fact_trips(trips, loads, batch_id="batch_001")
    assert len(fact_df) == 1
    row = fact_df.iloc[0]
    assert row["trip_id"] == "TRIP_101"
    assert row["customer_id"] == "CUST_001"
    assert row["freight_revenue"] == 1500.0
    assert row["total_revenue"] == 1750.0 # 1500 + 200 + 50
    assert row["batch_id"] == "batch_001"

