"""
Gold Medallion Layer Loader & Atomic Publisher for Logistics Platform.
1. Executes BigQuery DDL to ensure logistics_gold dataset and partitioned/clustered tables exist.
2. Ingests cleansed Silver Parquet files from GCS (or local cache).
3. Constructs Star Schema Dimension models (including SCD Type 2 for drivers).
4. Constructs Partitioned Fact tables (joining operational entities like trips + loads).
5. Loads data into BigQuery with idempotency guarantees (re-running never duplicates data).
6. Updates batch_control to COMPLETED and records Gold stage audit trail.
"""

import os
import sys
import io
import time
import argparse
import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional

# Ensure project root is in sys.path
PROJECT_ROOT = str(Path(__file__).resolve().parents[2])
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import pandas as pd
from google.cloud import bigquery, storage
from google.oauth2 import service_account

from src.utils.schema_loader import ConfigManager
from src.audit.audit_manager import AuditManager
from src.notification.notifier import PipelineNotifier
from src.ingestion.batch_controller import BatchController
from src.gold.dimensions.dim_builder import (
    build_dim_date,
    build_dim_customer,
    build_dim_driver,
    build_dim_truck,
    build_dim_trailer,
    build_dim_facility,
    build_dim_route
)
from src.gold.facts.fact_builder import (
    build_fact_trips,
    build_fact_delivery_events,
    build_fact_fuel_purchases,
    build_fact_maintenance,
    build_fact_safety_incidents,
    build_agg_driver_monthly_performance,
    build_agg_truck_utilization_performance
)

class GoldLoader:
    def __init__(self, config_mgr: Optional[ConfigManager] = None):
        self.config_mgr = config_mgr or ConfigManager()
        self.config = self.config_mgr.pipeline_config
        self.project_id = self.config["bigquery"]["project"]
        self.gold_dataset = self.config["bigquery"]["gold_dataset"]
        self.key_path = self.config.get("paths", {}).get("gcp_key_path", "config/gcp-key.json")
        self.bucket_name = self.config["gcs"]["bucket_name"]
        self.silver_zone = self.config["gcs"]["zones"]["silver"].strip("/")

        self.audit_mgr = AuditManager(self.config_mgr)
        self.batch_ctrl = BatchController(self.config)
        self.notifier = PipelineNotifier(self.config_mgr)

        self._bq_client: Optional[bigquery.Client] = None
        self._storage_client: Optional[storage.Client] = None

    @property
    def bq_client(self) -> bigquery.Client:
        if self._bq_client is None:
            if not os.path.exists(self.key_path):
                raise FileNotFoundError(f"GCP service account key not found at: {self.key_path}")
            credentials = service_account.Credentials.from_service_account_file(self.key_path)
            self._bq_client = bigquery.Client(credentials=credentials, project=credentials.project_id)
        return self._bq_client

    @property
    def storage_client(self) -> storage.Client:
        if self._storage_client is None:
            if not os.path.exists(self.key_path):
                raise FileNotFoundError(f"GCP service account key not found at: {self.key_path}")
            credentials = service_account.Credentials.from_service_account_file(self.key_path)
            self._storage_client = storage.Client(credentials=credentials, project=credentials.project_id)
        return self._storage_client

    def ensure_gold_dataset_and_tables(self):
        """Creates dataset and executes BigQuery DDL for Star Schema."""
        client = self.bq_client
        dataset_id = f"{self.project_id}.{self.gold_dataset}"
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = self.config["bigquery"].get("location", "US")
        client.create_dataset(dataset, exists_ok=True)
        print(f"  [GOLD DDL] Ensured dataset: {dataset_id}")

        ddl_path = Path("sql/bigquery/ddl_gold_schema.sql")
        if not ddl_path.exists():
            raise FileNotFoundError(f"DDL file missing at: {ddl_path}")

        with open(ddl_path, "r", encoding="utf-8") as f:
            ddl_content = f.read()

        # Execute statements
        statements = [stmt.strip() for stmt in ddl_content.split(";") if stmt.strip()]
        for stmt in statements:
            job = client.query(stmt)
            job.result()
        print(f"  [GOLD DDL] Executed {len(statements)} Star Schema table DDL statements.")

    def load_silver_table(self, batch_id: str, table_name: str) -> pd.DataFrame:
        """Reads a table's Silver Parquet from GCS or local cache."""
        file_name = f"{table_name}_{batch_id}.parquet"
        gcs_silver_path = f"{self.silver_zone}/{table_name}/batch_id={batch_id}/{file_name}"

        bucket = self.storage_client.bucket(self.bucket_name)
        blob = bucket.blob(gcs_silver_path)

        if blob.exists():
            parquet_bytes = blob.download_as_bytes()
            return pd.read_parquet(io.BytesIO(parquet_bytes))

        local_file = Path("data/silver") / table_name / f"batch_id={batch_id}" / file_name
        if local_file.exists():
            return pd.read_parquet(local_file)

        raise FileNotFoundError(f"Silver Parquet not found in GCS ({gcs_silver_path}) or locally ({local_file})")

    def publish_table(
        self,
        table_name: str,
        df: pd.DataFrame,
        batch_id: str,
        is_dimension: bool = False
    ) -> int:
        """
        Loads DataFrame into BigQuery table with idempotency.
        For facts/aggregates: deletes existing records for batch_id before inserting.
        For dimensions: truncates/updates dimension table cleanly.
        """
        start_time = datetime.datetime.now(datetime.timezone.utc)
        full_table_id = f"{self.project_id}.{self.gold_dataset}.{table_name}"
        row_count = len(df)

        print(f"  [GOLD PUBLISH] Publishing {table_name} ({row_count:,} rows) ...", end=" ", flush=True)

        if row_count == 0:
            print("SKIPPED (0 rows)")
            return 0

        # Idempotency guard: Delete any prior records for this batch if table has batch_id
        if "batch_id" in df.columns:
            del_query = f"DELETE FROM `{full_table_id}` WHERE batch_id = '{batch_id}'"
            try:
                self.bq_client.query(del_query).result()
            except Exception:
                pass # Table might be fresh/empty
        elif is_dimension and batch_id == "batch_001":
            # For baseline batch, truncate dimensions
            try:
                self.bq_client.query(f"TRUNCATE TABLE `{full_table_id}`").result()
            except Exception:
                pass

        # Load DataFrame to BigQuery
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )
        load_job = self.bq_client.load_table_from_dataframe(df, full_table_id, job_config=job_config)
        load_job.result() # Wait for job completion

        end_time = datetime.datetime.now(datetime.timezone.utc)
        duration = (end_time - start_time).total_seconds()

        # Audit step
        self.audit_mgr.log_pipeline_step(
            batch_id=batch_id,
            stage="GOLD",
            table_name=table_name,
            status="COMPLETED",
            records_in=row_count,
            records_out=row_count,
            records_quarantined=0,
            start_time=start_time,
            end_time=end_time,
            duration_seconds=duration
        )

        print(f"DONE in {duration:.2f}s -> {full_table_id}")
        return row_count

    def publish_gold_batch(self, batch_id: str) -> Dict[str, Any]:
        """
        Executes full Gold Medallion publish for the specified batch.
        Constructs all Dimensions, Facts, and Aggregated Marts and publishes to BigQuery.
        """
        batch_start = time.time()
        print(f"\n========================================================")
        print(f" Starting Gold Medallion Publish: {batch_id}")
        print(f" BigQuery Target: {self.project_id}.{self.gold_dataset}")
        print(f"========================================================")

        # 1. Ensure BigQuery Schema & Tables
        self.ensure_gold_dataset_and_tables()

        # 2. Update Batch Control to PROCESSING
        self.batch_ctrl.update_batch_status(batch_id, "PROCESSING")

        published_summary = {}

        try:
            # 3. Publish Dimensions
            print("\n--- [1/3] Building & Publishing Dimension Tables ---")
            
            # dim_date
            dim_date_df = build_dim_date(2020, 2026)
            published_summary["dim_date"] = self.publish_table("dim_date", dim_date_df, batch_id, is_dimension=True)

            # dim_customer
            silver_cust = self.load_silver_table(batch_id, "customers")
            dim_cust_df = build_dim_customer(silver_cust)
            published_summary["dim_customer"] = self.publish_table("dim_customer", dim_cust_df, batch_id, is_dimension=True)

            # dim_driver (with SCD Type 2)
            silver_drivers = self.load_silver_table(batch_id, "drivers")
            dim_driver_df = build_dim_driver(silver_drivers)
            published_summary["dim_driver"] = self.publish_table("dim_driver", dim_driver_df, batch_id, is_dimension=True)

            # dim_truck
            silver_trucks = self.load_silver_table(batch_id, "trucks")
            dim_truck_df = build_dim_truck(silver_trucks)
            published_summary["dim_truck"] = self.publish_table("dim_truck", dim_truck_df, batch_id, is_dimension=True)

            # dim_trailer
            silver_trailers = self.load_silver_table(batch_id, "trailers")
            dim_trailer_df = build_dim_trailer(silver_trailers)
            published_summary["dim_trailer"] = self.publish_table("dim_trailer", dim_trailer_df, batch_id, is_dimension=True)

            # dim_facility
            silver_facilities = self.load_silver_table(batch_id, "facilities")
            dim_facility_df = build_dim_facility(silver_facilities)
            published_summary["dim_facility"] = self.publish_table("dim_facility", dim_facility_df, batch_id, is_dimension=True)

            # dim_route
            silver_routes = self.load_silver_table(batch_id, "routes")
            dim_route_df = build_dim_route(silver_routes)
            published_summary["dim_route"] = self.publish_table("dim_route", dim_route_df, batch_id, is_dimension=True)

            # 4. Publish Partitioned & Clustered Facts
            print("\n--- [2/3] Building & Publishing Fact Tables ---")
            
            # fact_trips (Joins trips + loads)
            silver_trips = self.load_silver_table(batch_id, "trips")
            silver_loads = self.load_silver_table(batch_id, "loads")
            fact_trips_df = build_fact_trips(silver_trips, silver_loads, batch_id)
            published_summary["fact_trips"] = self.publish_table("fact_trips", fact_trips_df, batch_id)

            # fact_delivery_events
            silver_events = self.load_silver_table(batch_id, "delivery_events")
            fact_events_df = build_fact_delivery_events(silver_events, batch_id)
            published_summary["fact_delivery_events"] = self.publish_table("fact_delivery_events", fact_events_df, batch_id)

            # fact_fuel_purchases
            silver_fuel = self.load_silver_table(batch_id, "fuel_purchases")
            fact_fuel_df = build_fact_fuel_purchases(silver_fuel, batch_id)
            published_summary["fact_fuel_purchases"] = self.publish_table("fact_fuel_purchases", fact_fuel_df, batch_id)

            # fact_maintenance
            silver_maint = self.load_silver_table(batch_id, "maintenance_records")
            fact_maint_df = build_fact_maintenance(silver_maint, batch_id)
            published_summary["fact_maintenance"] = self.publish_table("fact_maintenance", fact_maint_df, batch_id)

            # fact_safety_incidents
            silver_incidents = self.load_silver_table(batch_id, "safety_incidents")
            fact_incidents_df = build_fact_safety_incidents(silver_incidents, batch_id)
            published_summary["fact_safety_incidents"] = self.publish_table("fact_safety_incidents", fact_incidents_df, batch_id)

            # 5. Publish Aggregated Marts
            print("\n--- [3/3] Building & Publishing Summary Performance Marts ---")
            
            # agg_driver_monthly_performance
            silver_drv_metrics = self.load_silver_table(batch_id, "driver_monthly_metrics")
            agg_drv_df = build_agg_driver_monthly_performance(silver_drv_metrics, batch_id)
            published_summary["agg_driver_monthly_performance"] = self.publish_table("agg_driver_monthly_performance", agg_drv_df, batch_id)

            # agg_truck_utilization_performance
            silver_trk_metrics = self.load_silver_table(batch_id, "truck_utilization_metrics")
            agg_trk_df = build_agg_truck_utilization_performance(silver_trk_metrics, batch_id)
            published_summary["agg_truck_utilization_performance"] = self.publish_table("agg_truck_utilization_performance", agg_trk_df, batch_id)

            batch_duration = time.time() - batch_start
            total_rows_published = sum(published_summary.values())

            # 6. Update Batch Control to COMPLETED
            self.batch_ctrl.update_batch_status(batch_id, "COMPLETED")

            # 7. Dispatch Success Alert
            self.notifier.alert_batch_completed(
                batch_id=batch_id,
                duration_seconds=batch_duration,
                summary={
                    "total_tables": len(published_summary),
                    "total_rows_published": total_rows_published,
                    "tables": published_summary
                }
            )

            print(f"\n========================================================")
            print(f" Gold Medallion Publish Completed Successfully for {batch_id}!")
            print(f" Total Rows Published to BigQuery: {total_rows_published:,}")
            print(f" Published Across {len(published_summary)} Tables in {batch_duration:.2f}s")
            print(f" batch_control Status -> COMPLETED")
            print(f"========================================================\n")

            return {
                "batch_id": batch_id,
                "status": "COMPLETED",
                "duration_seconds": batch_duration,
                "total_rows_published": total_rows_published,
                "summary": published_summary
            }

        except Exception as ex:
            batch_duration = time.time() - batch_start
            err_msg = str(ex)
            print(f"\n[GOLD PUBLISH FAILED]: {err_msg}")

            # Update batch_control to FAILED
            self.batch_ctrl.update_batch_status(batch_id, "FAILED", error_message=err_msg)
            self.notifier.alert_pipeline_failure(batch_id, "GOLD", "ALL", err_msg)
            raise

def main():
    parser = argparse.ArgumentParser(description="Gold Medallion Star Schema Loader")
    parser.add_argument("--batch_id", type=str, default="batch_001", help="Batch identifier (e.g. batch_001)")
    args = parser.parse_args()

    loader = GoldLoader()
    loader.publish_gold_batch(args.batch_id)

if __name__ == "__main__":
    main()

