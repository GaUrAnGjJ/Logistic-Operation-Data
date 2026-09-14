"""
Dimension Model Builder for Logistics Gold Layer.
Transforms cleaned Silver datasets into standardized Star Schema Dimension tables:
- dim_date (generated calendar)
- dim_customer
- dim_driver (with SCD Type 2 tracking)
- dim_truck
- dim_trailer
- dim_facility
- dim_route
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

def build_dim_date(start_year: int = 2020, end_year: int = 2026) -> pd.DataFrame:
    """Generates an exhaustive calendar dimension for analytical queries."""
    start_date = datetime.date(start_year, 1, 1)
    end_date = datetime.date(end_year, 12, 31)
    date_range = pd.date_range(start=start_date, end=end_date, freq="D")

    df = pd.DataFrame({"full_date": date_range.date})
    dt_series = pd.to_datetime(df["full_date"])

    df["date_key"] = dt_series.dt.strftime("%Y%m%d").astype("int64")
    df["day_of_week"] = dt_series.dt.dayofweek + 1 # 1=Monday, 7=Sunday
    df["day_name"] = dt_series.dt.day_name()
    df["day_of_month"] = dt_series.dt.day
    df["month"] = dt_series.dt.month
    df["month_name"] = dt_series.dt.month_name()
    df["quarter"] = dt_series.dt.quarter
    df["year"] = dt_series.dt.year
    df["is_weekend"] = df["day_of_week"].isin([6, 7])

    cols = [
        "date_key", "full_date", "day_of_week", "day_name",
        "day_of_month", "month", "month_name", "quarter", "year", "is_weekend"
    ]
    return df[cols]

def build_dim_customer(silver_df: pd.DataFrame) -> pd.DataFrame:
    """Builds dim_customer with surrogate keys."""
    df = silver_df.copy()
    df = df.sort_values("customer_id").reset_index(drop=True)
    df["customer_sk"] = df.index + 1
    df["_created_at"] = datetime.datetime.now(datetime.timezone.utc)

    cols = [
        "customer_sk", "customer_id", "customer_name", "customer_type",
        "credit_terms_days", "primary_freight_type", "account_status",
        "contract_start_date", "annual_revenue_potential", "_created_at"
    ]
    return df[[c for c in cols if c in df.columns]]

def build_dim_driver(silver_df: pd.DataFrame, existing_dim: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    """
    Builds dim_driver supporting SCD Type 2 tracking:
    Columns: driver_sk, driver_id, full_name, valid_from, valid_to, is_current.
    """
    df = silver_df.copy()
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    today_date = now_utc.date()

    df["full_name"] = df["first_name"].fillna("") + " " + df["last_name"].fillna("")
    target_cols = [
        "driver_sk", "driver_id", "first_name", "last_name", "full_name",
        "license_number", "license_state", "date_of_birth", "home_terminal",
        "employment_status", "cdl_class", "years_experience", "hire_date",
        "termination_date", "valid_from", "valid_to", "is_current", "_created_at"
    ]

    if existing_dim is None or existing_dim.empty:
        # Initial Load (Batch 001)
        df = df.sort_values("driver_id").reset_index(drop=True)
        df["driver_sk"] = df.index + 1
        # Set valid_from to hire_date, or default to 2020-01-01
        df["valid_from"] = pd.to_datetime(df["hire_date"], errors="coerce").dt.date.fillna(datetime.date(2020, 1, 1))
        df["valid_to"] = None
        df["is_current"] = True
        df["_created_at"] = now_utc
        return df[[c for c in target_cols if c in df.columns]]

    # Incremental SCD Type 2 Logic
    max_sk = int(existing_dim["driver_sk"].max()) if not existing_dim.empty else 0
    updated_records = []

    # Preserve historical non-current records
    historical = existing_dim[existing_dim["is_current"] == False].copy()

    for _, row in existing_dim[existing_dim["is_current"] == True].iterrows():
        driver_id = row["driver_id"]
        incoming = df[df["driver_id"] == driver_id]

        if not incoming.empty:
            inc_row = incoming.iloc[0]
            status_changed = inc_row["employment_status"] != row["employment_status"]
            term_changed = str(inc_row.get("termination_date", "")) != str(row.get("termination_date", ""))

            if status_changed or term_changed:
                # Expire previous record
                row_copy = row.copy()
                row_copy["valid_to"] = today_date
                row_copy["is_current"] = False
                updated_records.append(row_copy)

                # Create new current record
                max_sk += 1
                new_row = inc_row.copy()
                new_row["driver_sk"] = max_sk
                new_row["valid_from"] = today_date
                new_row["valid_to"] = None
                new_row["is_current"] = True
                new_row["_created_at"] = now_utc
                updated_records.append(new_row)
            else:
                updated_records.append(row)
        else:
            updated_records.append(row)

    # Check for brand new drivers not previously in dim
    existing_ids = set(existing_dim["driver_id"].unique())
    new_drivers = df[~df["driver_id"].isin(existing_ids)].copy()
    for _, inc_row in new_drivers.iterrows():
        max_sk += 1
        new_row = inc_row.copy()
        new_row["driver_sk"] = max_sk
        new_row["valid_from"] = pd.to_datetime(new_row.get("hire_date"), errors="coerce").dt.date or today_date
        new_row["valid_to"] = None
        new_row["is_current"] = True
        new_row["_created_at"] = now_utc
        updated_records.append(new_row)

    combined_df = pd.concat([historical, pd.DataFrame(updated_records)], ignore_index=True)
    return combined_df[[c for c in target_cols if c in combined_df.columns]]

def build_dim_truck(silver_df: pd.DataFrame) -> pd.DataFrame:
    """Builds dim_truck with surrogate keys."""
    df = silver_df.copy()
    df = df.sort_values("truck_id").reset_index(drop=True)
    df["truck_sk"] = df.index + 1
    df["_created_at"] = datetime.datetime.now(datetime.timezone.utc)
    target_cols = [
        "truck_sk", "truck_id", "unit_number", "make", "model_year", "vin",
        "acquisition_date", "acquisition_mileage", "fuel_type", "tank_capacity_gallons",
        "status", "home_terminal", "_created_at"
    ]
    return df[[c for c in target_cols if c in df.columns]]

def build_dim_trailer(silver_df: pd.DataFrame) -> pd.DataFrame:
    """Builds dim_trailer with surrogate keys."""
    df = silver_df.copy()
    df = df.sort_values("trailer_id").reset_index(drop=True)
    df["trailer_sk"] = df.index + 1
    df["_created_at"] = datetime.datetime.now(datetime.timezone.utc)
    target_cols = [
        "trailer_sk", "trailer_id", "trailer_number", "trailer_type", "length_feet",
        "model_year", "vin", "acquisition_date", "status", "current_location", "_created_at"
    ]
    return df[[c for c in target_cols if c in df.columns]]

def build_dim_facility(silver_df: pd.DataFrame) -> pd.DataFrame:
    """Builds dim_facility with surrogate keys."""
    df = silver_df.copy()
    df = df.sort_values("facility_id").reset_index(drop=True)
    df["facility_sk"] = df.index + 1
    df["_created_at"] = datetime.datetime.now(datetime.timezone.utc)
    target_cols = [
        "facility_sk", "facility_id", "facility_name", "facility_type", "city",
        "state", "latitude", "longitude", "dock_doors", "operating_hours", "_created_at"
    ]
    return df[[c for c in target_cols if c in df.columns]]

def build_dim_route(silver_df: pd.DataFrame) -> pd.DataFrame:
    """Builds dim_route with surrogate keys."""
    df = silver_df.copy()
    df = df.sort_values("route_id").reset_index(drop=True)
    df["route_sk"] = df.index + 1
    df["_created_at"] = datetime.datetime.now(datetime.timezone.utc)
    target_cols = [
        "route_sk", "route_id", "origin_city", "origin_state", "destination_city",
        "destination_state", "typical_distance_miles", "base_rate_per_mile",
        "fuel_surcharge_rate", "typical_transit_days", "_created_at"
    ]
    return df[[c for c in target_cols if c in df.columns]]

