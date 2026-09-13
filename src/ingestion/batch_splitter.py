"""
Batch Splitter for Logistics Data Platform.
Splits 14 raw CSV datasets into 3 chronological batches (2022, 2023, 2024+)
and prepares simulated dimension updates for SCD Type 2 validation.
Generates batch manifests (manifest.json) for each batch.
"""

import os
import json
import hashlib
import datetime
from pathlib import Path
import pandas as pd
import yaml

def compute_sha256(file_path: Path) -> str:
    """Compute SHA-256 hash of a file."""
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(65536), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()

def load_config(config_path: str = "config/pipeline_config.yaml") -> dict:
    """Load yaml configuration."""
    if os.path.exists(config_path):
        with open(config_path, "r") as f:
            return yaml.safe_load(f)
    return {
        "paths": {
            "raw_data_dir": "data/raw",
            "batches_dir": "data/batches"
        }
    }

def split_batches():
    config = load_config()
    raw_dir = Path(config.get("paths", {}).get("raw_data_dir", "data/raw"))
    batches_base_dir = Path(config.get("paths", {}).get("batches_dir", "data/batches"))
    batches_base_dir.mkdir(parents=True, exist_ok=True)

    print(f"Reading raw data from: {raw_dir}")
    print(f"Outputting batches to: {batches_base_dir}")

    # Transaction table mapping: (table_name, date_column)
    transaction_tables = {
        "loads": "load_date",
        "trips": "dispatch_date",
        "delivery_events": "scheduled_datetime",
        "fuel_purchases": "purchase_date",
        "maintenance_records": "maintenance_date",
        "safety_incidents": "incident_date",
        "driver_monthly_metrics": "month",
        "truck_utilization_metrics": "month"
    }

    dimension_tables = [
        "customers",
        "drivers",
        "facilities",
        "routes",
        "trailers",
        "trucks"
    ]

    # Batch definitions
    batches = [
        {"batch_id": "batch_001", "year": 2022, "mode": "exact"},
        {"batch_id": "batch_002", "year": 2023, "mode": "exact"},
        {"batch_id": "batch_003", "year": 2024, "mode": "gte"} # 2024 and beyond
    ]

    # Preload all raw dataframes
    raw_dfs = {}
    for table in list(transaction_tables.keys()) + dimension_tables:
        csv_file = raw_dir / f"{table}.csv"
        if not csv_file.exists():
            raise FileNotFoundError(f"Missing required raw CSV: {csv_file}")
        raw_dfs[table] = pd.read_csv(csv_file)
        print(f"Loaded {table}.csv: {len(raw_dfs[table])} rows")

    # Generate each batch
    for batch_info in batches:
        batch_id = batch_info["batch_id"]
        target_year = batch_info["year"]
        mode = batch_info["mode"]

        batch_dir = batches_base_dir / batch_id
        batch_dir.mkdir(parents=True, exist_ok=True)
        print(f"\n--- Generating {batch_id} (Target Year: {target_year}{'+' if mode == 'gte' else ''}) ---")

        manifest_tables = []

        # 1. Process Transaction Tables
        for table, date_col in transaction_tables.items():
            df = raw_dfs[table].copy()
            # Extract year safely
            dt_series = pd.to_datetime(df[date_col], errors="coerce")
            if mode == "exact":
                filtered_df = df[dt_series.dt.year == target_year]
            else:
                filtered_df = df[dt_series.dt.year >= target_year]

            out_file_name = f"{table}_{batch_id}.csv"
            out_file_path = batch_dir / out_file_name
            filtered_df.to_csv(out_file_path, index=False)

            sha256 = compute_sha256(out_file_path)
            file_size = out_file_path.stat().st_size

            manifest_tables.append({
                "table": table,
                "file": out_file_name,
                "row_count": len(filtered_df),
                "file_size_bytes": file_size,
                "sha256": sha256
            })
            print(f"  [Tx] {table}: {len(filtered_df)} rows -> {out_file_name}")

        # 2. Process Dimension Tables
        for table in dimension_tables:
            df = raw_dfs[table].copy()
            out_file_name = f"{table}_{batch_id}.csv"
            out_file_path = batch_dir / out_file_name

            if batch_id == "batch_001":
                # Batch 001 gets full baseline dataset for initial load
                batch_dim_df = df
            elif batch_id == "batch_002":
                # Batch 002: Simulate updates / changes for SCD Type 2 testing
                if table == "drivers":
                    # Sample 15 drivers, simulate employment status changes
                    sample = df.head(15).copy()
                    sample["employment_status"] = "Terminated"
                    sample["termination_date"] = "2023-06-30"
                    batch_dim_df = sample
                elif table == "trucks":
                    # Sample 10 trucks, simulate status change to Maintenance
                    sample = df.head(10).copy()
                    sample["status"] = "Maintenance"
                    sample["acquisition_mileage"] = sample["acquisition_mileage"] + 45000
                    batch_dim_df = sample
                else:
                    # Stable dimensions: take a slice or full snapshot
                    batch_dim_df = df.head(20).copy()
            else: # batch_003
                # Batch 003: Further simulated updates
                if table == "drivers":
                    sample = df.tail(15).copy()
                    sample["employment_status"] = "On Leave"
                    batch_dim_df = sample
                elif table == "trucks":
                    sample = df.tail(10).copy()
                    sample["status"] = "Inactive"
                    sample["acquisition_mileage"] = sample["acquisition_mileage"] + 90000
                    batch_dim_df = sample
                else:
                    batch_dim_df = df.tail(20).copy()

            batch_dim_df.to_csv(out_file_path, index=False)
            sha256 = compute_sha256(out_file_path)
            file_size = out_file_path.stat().st_size

            manifest_tables.append({
                "table": table,
                "file": out_file_name,
                "row_count": len(batch_dim_df),
                "file_size_bytes": file_size,
                "sha256": sha256
            })
            print(f"  [Dim] {table}: {len(batch_dim_df)} rows -> {out_file_name}")

        # 3. Create Manifest JSON
        manifest = {
            "batch_id": batch_id,
            "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "target_year": target_year,
            "total_tables": len(manifest_tables),
            "tables": manifest_tables
        }

        manifest_path = batch_dir / "manifest.json"
        with open(manifest_path, "w", encoding="utf-8") as f:
            json.dump(manifest, f, indent=2)

        print(f"  Manifest written to {manifest_path}")

    print("\nBatch splitting completed successfully!")

if __name__ == "__main__":
    split_batches()
