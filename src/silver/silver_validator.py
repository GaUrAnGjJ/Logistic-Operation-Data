"""
Silver Medallion Layer Validator & Quarantine Engine for Logistics Platform.
1. Reads Bronze Parquet from GCS bronze/ (or local cache).
2. Performs data cleansing, string trimming, and format normalization.
3. Evaluates DQ validation rules via RulesEngine.
4. Quarantines invalid records with detailed reasons to GCS quarantine/ and BigQuery rejection_log.
5. Casts validated records to strict target schemas and saves to GCS silver/.
6. Enforces maximum rejection threshold (20%) to prevent tainted batches from reaching Gold.
7. Logs execution metrics and quality scores to BigQuery audit tables.
"""

import os
import sys
import io
import json
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
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage
from google.oauth2 import service_account

from src.utils.schema_loader import ConfigManager
from src.audit.audit_manager import AuditManager
from src.notification.notifier import PipelineNotifier
from src.silver.rules_engine import RulesEngine

class DataQualityThresholdException(Exception):
    """Raised when the rejection rate exceeds the maximum allowable threshold."""
    pass

class SilverValidator:
    def __init__(self, config_mgr: Optional[ConfigManager] = None):
        self.config_mgr = config_mgr or ConfigManager()
        self.config = self.config_mgr.pipeline_config
        self.audit_mgr = AuditManager(self.config_mgr)
        self.notifier = PipelineNotifier(self.config_mgr)
        self.rules_engine = RulesEngine(self.config_mgr)

        self.bucket_name = self.config["gcs"]["bucket_name"]
        self.bronze_zone = self.config["gcs"]["zones"]["bronze"].strip("/")
        self.silver_zone = self.config["gcs"]["zones"]["silver"].strip("/")
        self.quarantine_zone = self.config["gcs"]["zones"]["quarantine"].strip("/")
        self.max_rejection_rate = float(self.config.get("batch", {}).get("max_rejection_rate_pct", 20.0))
        self.key_path = self.config.get("paths", {}).get("gcp_key_path", "config/gcp-key.json")
        self._storage_client: Optional[storage.Client] = None

    @property
    def storage_client(self) -> storage.Client:
        if self._storage_client is None:
            if not os.path.exists(self.key_path):
                raise FileNotFoundError(f"GCP service account key not found at: {self.key_path}")
            credentials = service_account.Credentials.from_service_account_file(self.key_path)
            self._storage_client = storage.Client(credentials=credentials, project=credentials.project_id)
        return self._storage_client

    def _cast_to_target_schema(self, df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        """Casts DataFrame columns to strict types according to tables_schema.yaml."""
        table_meta = self.config_mgr.get_table_schema(table_name)
        col_defs = table_meta.get("columns", {})
        result_df = df.copy()

        for col_name, col_meta in col_defs.items():
            if col_name not in result_df.columns:
                continue

            target_type = col_meta.get("type", "string").lower()
            is_nullable = col_meta.get("nullable", True)

            try:
                if target_type == "integer":
                    result_df[col_name] = pd.to_numeric(result_df[col_name], errors="coerce").astype("Int64")
                elif target_type in {"double", "float"}:
                    result_df[col_name] = pd.to_numeric(result_df[col_name], errors="coerce").astype("float64")
                elif target_type == "boolean":
                    result_df[col_name] = result_df[col_name].astype(str).str.strip().str.lower().map({
                        "true": True, "1": True, "t": True, "yes": True,
                        "false": False, "0": False, "f": False, "no": False
                    }).astype("boolean")
                elif target_type == "date":
                    result_df[col_name] = pd.to_datetime(result_df[col_name], errors="coerce").dt.date
                elif target_type == "timestamp":
                    result_df[col_name] = pd.to_datetime(result_df[col_name], errors="coerce")
                else: # string
                    result_df[col_name] = result_df[col_name].astype(str).str.strip()
                    if is_nullable:
                        result_df[col_name] = result_df[col_name].replace(["nan", "None", "NULL", ""], None)
            except Exception as ex:
                print(f"[SILVER CAST] Warning casting {col_name} to {target_type}: {ex}")

        return result_df

    def process_table(self, batch_id: str, table_name: str) -> Dict[str, Any]:
        """
        Processes a single Bronze table: validates rules, separates quarantined rows,
        casts to strict Silver schema, and writes both streams to GCS & BigQuery.
        """
        start_time = datetime.datetime.now(datetime.timezone.utc)
        file_name = f"{table_name}_{batch_id}.parquet"
        gcs_bronze_path = f"{self.bronze_zone}/{table_name}/batch_id={batch_id}/{file_name}"
        gcs_silver_path = f"{self.silver_zone}/{table_name}/batch_id={batch_id}/{file_name}"
        gcs_quarantine_path = f"{self.quarantine_zone}/{table_name}/batch_id={batch_id}/{table_name}_quarantined.parquet"

        print(f"  [SILVER] Validating {table_name} ...", end=" ", flush=True)

        try:
            # 1. Read Bronze Parquet from GCS (fallback to local data/bronze cache)
            bucket = self.storage_client.bucket(self.bucket_name)
            bronze_blob = bucket.blob(gcs_bronze_path)

            if bronze_blob.exists():
                parquet_bytes = bronze_blob.download_as_bytes()
                bronze_df = pd.read_parquet(io.BytesIO(parquet_bytes))
            else:
                local_file = Path("data/bronze") / table_name / f"batch_id={batch_id}" / file_name
                if not local_file.exists():
                    raise FileNotFoundError(f"Bronze Parquet not found in GCS ({gcs_bronze_path}) or locally ({local_file})")
                bronze_df = pd.read_parquet(local_file)

            total_records = len(bronze_df)

            # 2. Evaluate DQ Rules via Rules Engine
            valid_df, quarantined_df = self.rules_engine.evaluate_table(table_name, bronze_df, batch_id)

            valid_count = len(valid_df)
            quarantined_count = len(quarantined_df)
            rejection_rate_pct = (quarantined_count / total_records * 100.0) if total_records > 0 else 0.0
            dq_score_pct = 100.0 - rejection_rate_pct

            # 3. Handle Quarantined Records
            if quarantined_count > 0:
                # Save to GCS Quarantine Zone
                q_table = pa.Table.from_pandas(quarantined_df, preserve_index=False)
                q_buffer = io.BytesIO()
                pq.write_table(q_table, q_buffer, compression="SNAPPY")
                q_blob = bucket.blob(gcs_quarantine_path)
                q_blob.upload_from_string(q_buffer.getvalue(), content_type="application/octet-stream")

                # Local cache
                local_q_dir = Path("data/quarantine") / table_name / f"batch_id={batch_id}"
                local_q_dir.mkdir(parents=True, exist_ok=True)
                with open(local_q_dir / f"{table_name}_quarantined.parquet", "wb") as f:
                    f.write(q_buffer.getvalue())

                # Prepare rejection log rows for BigQuery
                rejections_to_log = []
                for _, r in quarantined_df.head(200).iterrows(): # Log up to 200 sample rows
                    payload_dict = {k: str(v) for k, v in r.items() if not k.startswith("_")}
                    rejections_to_log.append({
                        "record_pk": r.get("_record_pk", ""),
                        "rule_id": r.get("_rejection_rule", ""),
                        "rule_type": "DATA_QUALITY_RULE",
                        "rejection_reason": r.get("_rejection_reason", ""),
                        "rejected_payload": json.dumps(payload_dict)
                    })
                self.audit_mgr.log_rejections(batch_id, table_name, rejections_to_log)

            # 4. Handle Valid Records (Type Casting & Silver Write)
            if valid_count > 0:
                valid_df = self._cast_to_target_schema(valid_df, table_name)
                valid_df["_silver_processed_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()

                s_table = pa.Table.from_pandas(valid_df, preserve_index=False)
                s_buffer = io.BytesIO()
                pq.write_table(s_table, s_buffer, compression="SNAPPY")
                s_blob = bucket.blob(gcs_silver_path)
                s_blob.upload_from_string(s_buffer.getvalue(), content_type="application/octet-stream")

                # Local cache
                local_s_dir = Path("data/silver") / table_name / f"batch_id={batch_id}"
                local_s_dir.mkdir(parents=True, exist_ok=True)
                with open(local_s_dir / file_name, "wb") as f:
                    f.write(s_buffer.getvalue())

            end_time = datetime.datetime.now(datetime.timezone.utc)
            duration = (end_time - start_time).total_seconds()

            # 5. Log Quality Metrics to BigQuery
            self.audit_mgr.log_dq_metrics(
                batch_id=batch_id,
                table_name=table_name,
                total_records=total_records,
                valid_records=valid_count,
                quarantined_records=quarantined_count
            )

            # 6. Log Stage Step to BigQuery pipeline_audit
            self.audit_mgr.log_pipeline_step(
                batch_id=batch_id,
                stage="SILVER",
                table_name=table_name,
                status="COMPLETED",
                records_in=total_records,
                records_out=valid_count,
                records_quarantined=quarantined_count,
                start_time=start_time,
                end_time=end_time,
                duration_seconds=duration
            )

            print(f"DONE (Valid: {valid_count:,} | Quarantined: {quarantined_count:,} | DQ Score: {dq_score_pct:.1f}%) in {duration:.2f}s")

            # 7. Check Rejection Threshold
            if rejection_rate_pct > self.max_rejection_rate:
                self.notifier.alert_rejection_threshold_exceeded(
                    batch_id=batch_id,
                    table_name=table_name,
                    rejection_rate_pct=rejection_rate_pct,
                    threshold_pct=self.max_rejection_rate,
                    total_records=total_records,
                    quarantined_records=quarantined_count
                )
                raise DataQualityThresholdException(
                    f"Table '{table_name}' exceeded maximum rejection threshold: "
                    f"{rejection_rate_pct:.2f}% > {self.max_rejection_rate:.2f}%"
                )

            return {
                "table": table_name,
                "status": "COMPLETED",
                "total_records": total_records,
                "valid_records": valid_count,
                "quarantined_records": quarantined_count,
                "dq_score_pct": dq_score_pct,
                "duration_seconds": duration,
                "silver_path": f"gs://{self.bucket_name}/{gcs_silver_path}"
            }

        except Exception as ex:
            end_time = datetime.datetime.now(datetime.timezone.utc)
            duration = (end_time - start_time).total_seconds()
            err_msg = str(ex)
            print(f"FAILED: {err_msg}")

            self.audit_mgr.log_pipeline_step(
                batch_id=batch_id,
                stage="SILVER",
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
            self.notifier.alert_pipeline_failure(batch_id, "SILVER", table_name, err_msg)
            raise

    def process_batch(self, batch_id: str) -> Dict[str, Any]:
        """Processes and validates all 14 tables for the Silver medallion layer."""
        print(f"\n========================================================")
        print(f" Starting Silver Validation & Cleansing: {batch_id}")
        print(f" GCS Silver Target: gs://{self.bucket_name}/{self.silver_zone}")
        print(f" Max Allowed Rejection Rate: {self.max_rejection_rate}%")
        print(f"========================================================")

        tables = list(self.config_mgr.schemas.keys())
        batch_start = time.time()
        results = []
        total_in = 0
        total_valid = 0
        total_quarantined = 0

        for tbl in tables:
            res = self.process_table(batch_id, tbl)
            results.append(res)
            total_in += res["total_records"]
            total_valid += res["valid_records"]
            total_quarantined += res["quarantined_records"]

        batch_duration = time.time() - batch_start
        overall_dq_score = (total_valid / total_in * 100.0) if total_in > 0 else 100.0

        print(f"\n========================================================")
        print(f" Silver Validation Completed for {batch_id}!")
        print(f" Total Ingested:   {total_in:,}")
        print(f" Successfully Validated (Silver): {total_valid:,}")
        print(f" Quarantined Rows: {total_quarantined:,}")
        print(f" Overall DQ Score: {overall_dq_score:.2f}%")
        print(f" Batch Duration:   {batch_duration:.2f}s")
        print(f"========================================================\n")

        return {
            "batch_id": batch_id,
            "stage": "SILVER",
            "total_tables": len(results),
            "total_records": total_in,
            "valid_records": total_valid,
            "quarantined_records": total_quarantined,
            "dq_score_pct": overall_dq_score,
            "duration_seconds": batch_duration,
            "tables": results
        }

def main():
    parser = argparse.ArgumentParser(description="Silver Medallion Validator & Quarantine Engine")
    parser.add_argument("--batch_id", type=str, default="batch_001", help="Batch identifier (e.g. batch_001)")
    parser.add_argument("--table", type=str, default=None, help="Process single table only (optional)")
    args = parser.parse_args()

    validator = SilverValidator()
    if args.table:
        validator.process_table(args.batch_id, args.table)
    else:
        validator.process_batch(args.batch_id)

if __name__ == "__main__":
    main()

