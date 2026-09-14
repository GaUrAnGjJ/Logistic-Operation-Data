"""
Bronze Medallion Layer Ingestion Processor for Logistics Platform.
Ingests raw CSV files from GCS raw zone (or local batch directory),
appends Bronze audit metadata (_batch_id, _ingested_at, _source_file),
converts data into optimized columnar Parquet format,
and uploads to GCS bronze/ zone while logging audit metrics to BigQuery.
"""

import os
import sys
from pathlib import Path

# Add project root to sys.path
_project_root = str(Path(__file__).resolve().parent.parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

import io
import time
import argparse
import datetime
from typing import Dict, List, Any, Optional
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage
from google.oauth2 import service_account

from src.utils.schema_loader import ConfigManager
from src.audit.audit_manager import AuditManager
from src.notification.notifier import PipelineNotifier

class BronzeProcessor:
    def __init__(self, config_mgr: Optional[ConfigManager] = None):
        self.config_mgr = config_mgr or ConfigManager()
        self.config = self.config_mgr.pipeline_config
        self.audit_mgr = AuditManager(self.config_mgr)
        self.notifier = PipelineNotifier(self.config_mgr)

        self.bucket_name = self.config["gcs"]["bucket_name"]
        self.raw_zone = self.config["gcs"]["zones"]["raw"].strip("/")
        self.bronze_zone = self.config["gcs"]["zones"]["bronze"].strip("/")
        self.key_path = self.config.get("paths", {}).get("gcp_key_path", "config/gcp-key.json")
        self.batches_dir = Path(self.config.get("paths", {}).get("batches_dir", "data/batches"))
        self._storage_client: Optional[storage.Client] = None

    @property
    def storage_client(self) -> storage.Client:
        if self._storage_client is None:
            if not os.path.exists(self.key_path):
                raise FileNotFoundError(f"GCP service account key not found at: {self.key_path}")
            credentials = service_account.Credentials.from_service_account_file(self.key_path)
            self._storage_client = storage.Client(credentials=credentials, project=credentials.project_id)
        return self._storage_client

    def process_table(self, batch_id: str, table_name: str) -> Dict[str, Any]:
        """
        Ingests a single raw table CSV for the given batch_id,
        converts to Parquet with bronze metadata, and writes to GCS bronze zone.
        """
        start_time = datetime.datetime.now(datetime.timezone.utc)
        file_name = f"{table_name}_{batch_id}.csv"
        gcs_raw_path = f"{self.raw_zone}/{table_name}/{file_name}"
        gcs_bronze_path = f"{self.bronze_zone}/{table_name}/batch_id={batch_id}/{table_name}_{batch_id}.parquet"

        print(f"  [BRONZE] Ingesting {table_name} ({file_name}) ...", end=" ", flush=True)

        try:
            # 1. Read Raw CSV (attempt from GCS, fallback to local batches)
            bucket = self.storage_client.bucket(self.bucket_name)
            raw_blob = bucket.blob(gcs_raw_path)

            if raw_blob.exists():
                csv_bytes = raw_blob.download_as_bytes()
                df = pd.read_csv(io.BytesIO(csv_bytes), dtype=str)
            else:
                # Local fallback
                local_file = self.batches_dir / batch_id / file_name
                if not local_file.exists():
                    raise FileNotFoundError(f"Raw file not found in GCS ({gcs_raw_path}) or locally ({local_file})")
                df = pd.read_csv(local_file, dtype=str)

            row_count = len(df)

            # 2. Append Bronze Medallion Metadata
            ingested_at = datetime.datetime.now(datetime.timezone.utc).isoformat()
            df["_batch_id"] = batch_id
            df["_ingested_at"] = ingested_at
            df["_source_file"] = file_name

            # 3. Convert to Parquet
            table = pa.Table.from_pandas(df, preserve_index=False)
            parquet_buffer = io.BytesIO()
            pq.write_table(table, parquet_buffer, compression="SNAPPY")
            parquet_bytes = parquet_buffer.getvalue()

            # 4. Upload to GCS Bronze Zone
            bronze_blob = bucket.blob(gcs_bronze_path)
            bronze_blob.upload_from_string(parquet_bytes, content_type="application/octet-stream")

            # Also cache local bronze copy for local testing
            local_bronze_dir = Path("data/bronze") / table_name / f"batch_id={batch_id}"
            local_bronze_dir.mkdir(parents=True, exist_ok=True)
            with open(local_bronze_dir / f"{table_name}_{batch_id}.parquet", "wb") as f:
                f.write(parquet_bytes)

            end_time = datetime.datetime.now(datetime.timezone.utc)
            duration = (end_time - start_time).total_seconds()

            # 5. Log to BigQuery pipeline_audit
            self.audit_mgr.log_pipeline_step(
                batch_id=batch_id,
                stage="BRONZE",
                table_name=table_name,
                status="COMPLETED",
                records_in=row_count,
                records_out=row_count,
                records_quarantined=0,
                start_time=start_time,
                end_time=end_time,
                duration_seconds=duration
            )

            print(f"DONE ({row_count} rows in {duration:.2f}s) -> gs://{self.bucket_name}/{gcs_bronze_path}")
            return {
                "table": table_name,
                "status": "COMPLETED",
                "rows": row_count,
                "duration_seconds": duration,
                "bronze_path": f"gs://{self.bucket_name}/{gcs_bronze_path}"
            }

        except Exception as ex:
            end_time = datetime.datetime.now(datetime.timezone.utc)
            duration = (end_time - start_time).total_seconds()
            err_msg = str(ex)
            print(f"FAILED: {err_msg}")

            # Log failure to BigQuery
            self.audit_mgr.log_pipeline_step(
                batch_id=batch_id,
                stage="BRONZE",
                table_name=table_name,
                status="FAILED",
                records_in=0,
                records_out=0,
                records_quarantined=0,
                start_time=start_time,
                end_time=end_time,
                duration_seconds=duration,
                error_message=err_msg
            )
            # Alert
            self.notifier.alert_pipeline_failure(batch_id, "BRONZE", table_name, err_msg)
            raise

    def process_batch(self, batch_id: str) -> Dict[str, Any]:
        """Processes all 14 tables in the batch for the Bronze medallion layer."""
        print(f"\n========================================================")
        print(f" Starting Bronze Ingestion: {batch_id}")
        print(f" GCS Bronze Target: gs://{self.bucket_name}/{self.bronze_zone}")
        print(f"========================================================")

        tables = list(self.config_mgr.schemas.keys())
        batch_start = time.time()
        results = []
        total_rows = 0

        for tbl in tables:
            res = self.process_table(batch_id, tbl)
            results.append(res)
            total_rows += res.get("rows", 0)

        batch_duration = time.time() - batch_start
        print(f"\n========================================================")
        print(f" Bronze Processing Completed for {batch_id}!")
        print(f" Processed {len(results)} tables ({total_rows:,} total rows) in {batch_duration:.2f}s")
        print(f"========================================================\n")

        return {
            "batch_id": batch_id,
            "stage": "BRONZE",
            "total_tables": len(results),
            "total_rows": total_rows,
            "duration_seconds": batch_duration,
            "tables": results
        }

def main():
    parser = argparse.ArgumentParser(description="Bronze Medallion Ingestion Processor")
    parser.add_argument("--batch_id", type=str, default="batch_001", help="Batch identifier (e.g. batch_001)")
    parser.add_argument("--table", type=str, default=None, help="Process single table only (optional)")
    args = parser.parse_args()

    processor = BronzeProcessor()
    if args.table:
        processor.process_table(args.batch_id, args.table)
    else:
        processor.process_batch(args.batch_id)

if __name__ == "__main__":
    main()

