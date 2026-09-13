"""
Batch Controller & Idempotency Guard for Logistics Data Platform.
Interacts with BigQuery logistics_audit.batch_control to ensure:
1. No duplicate processing of batches (Idempotency).
2. Audit trail for every batch arrival and processing lifecycle.
"""

import os
import sys
import json
import datetime
from pathlib import Path
import yaml
from google.cloud import bigquery
from google.oauth2 import service_account

def load_config(config_path: str = "config/pipeline_config.yaml") -> dict:
    if os.path.exists(config_path):
        with open(config_path, "r") as f:
            return yaml.safe_load(f)
    raise FileNotFoundError(f"Configuration file not found: {config_path}")

def get_bigquery_client(key_path: str = "config/gcp-key.json") -> bigquery.Client:
    if not os.path.exists(key_path):
        raise FileNotFoundError(f"GCP service account key not found at {key_path}")
    credentials = service_account.Credentials.from_service_account_file(key_path)
    return bigquery.Client(credentials=credentials, project=credentials.project_id)

class BatchController:
    def __init__(self, config: dict = None):
        self.config = config or load_config()
        self.project_id = self.config["bigquery"]["project"]
        self.audit_dataset = self.config["bigquery"]["audit_dataset"]
        self.control_table_name = "batch_control"
        self.full_table_id = f"{self.project_id}.{self.audit_dataset}.{self.control_table_name}"
        self.key_path = self.config.get("paths", {}).get("gcp_key_path", "config/gcp-key.json")
        self._client = None

    @property
    def client(self) -> bigquery.Client:
        if self._client is None:
            self._client = get_bigquery_client(self.key_path)
        return self._client

    def ensure_audit_dataset_and_table(self):
        """Ensure logistics_audit dataset and batch_control table exist in BigQuery."""
        client = self.client
        dataset_id = f"{self.project_id}.{self.audit_dataset}"
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"
        client.create_dataset(dataset, exists_ok=True)
        print(f"Ensured dataset: {dataset_id}")

        schema = [
            bigquery.SchemaField("batch_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("source_file", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("table_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("gcs_raw_path", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("arrival_time", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("processing_start", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("processing_end", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("records_in_file", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("status", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("error_message", "STRING", mode="NULLABLE")
        ]

        table = bigquery.Table(self.full_table_id, schema=schema)
        client.create_table(table, exists_ok=True)
        print(f"Ensured table: {self.full_table_id}")

    def is_batch_completed(self, batch_id: str) -> bool:
        """Idempotency check: returns True if batch already successfully processed."""
        self.ensure_audit_dataset_and_table()
        query = f"""
            SELECT COUNT(1) as cnt
            FROM `{self.full_table_id}`
            WHERE batch_id = @batch_id AND status = 'COMPLETED'
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("batch_id", "STRING", batch_id)
            ]
        )
        query_job = self.client.query(query, job_config=job_config)
        results = list(query_job.result())
        if results and results[0]["cnt"] > 0:
            return True
        return False

    def register_batch_arrival(self, batch_id: str, manifest: dict):
        """Register the arrival of batch files into batch_control as PENDING."""
        self.ensure_audit_dataset_and_table()
        now_ts = datetime.datetime.now(datetime.timezone.utc).isoformat()
        bucket_name = self.config["gcs"]["bucket_name"]
        raw_zone = self.config["gcs"]["zones"]["raw"].strip("/")

        rows_to_insert = []
        for table_info in manifest.get("tables", []):
            tbl = table_info["table"]
            file_name = table_info["file"]
            gcs_path = f"gs://{bucket_name}/{raw_zone}/{tbl}/{file_name}"
            rows_to_insert.append({
                "batch_id": batch_id,
                "source_file": file_name,
                "table_name": tbl,
                "gcs_raw_path": gcs_path,
                "arrival_time": now_ts,
                "processing_start": None,
                "processing_end": None,
                "records_in_file": table_info.get("row_count", 0),
                "status": "PENDING",
                "error_message": None
            })

        if rows_to_insert:
            errors = self.client.insert_rows_json(self.full_table_id, rows_to_insert)
            if errors:
                raise RuntimeError(f"Error inserting batch control rows: {errors}")
            print(f"Registered {len(rows_to_insert)} tables for {batch_id} in {self.full_table_id} as PENDING.")

    def update_batch_status(self, batch_id: str, status: str, error_message: str = None):
        """Update batch status (e.g. PROCESSING, COMPLETED, FAILED)."""
        now_ts = datetime.datetime.now(datetime.timezone.utc).isoformat()
        if status == "PROCESSING":
            query = f"""
                UPDATE `{self.full_table_id}`
                SET status = 'PROCESSING', processing_start = TIMESTAMP('{now_ts}')
                WHERE batch_id = '{batch_id}'
            """
        elif status == "COMPLETED":
            query = f"""
                UPDATE `{self.full_table_id}`
                SET status = 'COMPLETED', processing_end = TIMESTAMP('{now_ts}')
                WHERE batch_id = '{batch_id}'
            """
        else: # FAILED / SKIPPED
            err_clean = (error_message or "").replace("'", "\\'")
            query = f"""
                UPDATE `{self.full_table_id}`
                SET status = '{status}', processing_end = TIMESTAMP('{now_ts}'), error_message = '{err_clean}'
                WHERE batch_id = '{batch_id}'
            """
        query_job = self.client.query(query)
        query_job.result()
        print(f"Updated status for {batch_id} -> {status}")

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Batch Controller and Idempotency Guard")
    parser.add_argument("--check", type=str, help="Check if a batch is already completed")
    parser.add_argument("--register", type=str, help="Register batch arrival from local manifest")
    parser.add_argument("--init", action="store_true", help="Initialize dataset and control table")
    parser.add_argument("--status", type=str, choices=["PENDING", "PROCESSING", "COMPLETED", "FAILED"], help="Status to set")
    parser.add_argument("--batch_id", type=str, help="Batch ID for status update")
    parser.add_argument("--error", type=str, default=None, help="Error message if failed")
    args = parser.parse_args()

    controller = BatchController()

    if args.init:
        controller.ensure_audit_dataset_and_table()
        print("Initialization complete.")
    elif args.check:
        is_done = controller.is_batch_completed(args.check)
        print(f"Batch {args.check} completed: {is_done}")
        sys.exit(0 if not is_done else 1)
    elif args.register:
        config = load_config()
        manifest_path = Path(config["paths"]["batches_dir"]) / args.register / "manifest.json"
        if not manifest_path.exists():
            print(f"Error: Manifest not found at {manifest_path}")
            sys.exit(1)
        with open(manifest_path, "r") as f:
            manifest = json.load(f)
        controller.register_batch_arrival(args.register, manifest)
    elif args.status and args.batch_id:
        controller.update_batch_status(args.batch_id, args.status, args.error)
    else:
        controller.ensure_audit_dataset_and_table()
        print("BatchController ready.")

if __name__ == "__main__":
    main()
