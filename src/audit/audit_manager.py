"""
Audit Manager for Logistics Data Engineering Platform.
Manages all audit logging tables in BigQuery logistics_audit:
1. batch_control (lifecycle of batches)
2. pipeline_audit (step-level stage tracking: BRONZE, SILVER, GOLD)
3. rejection_log (record-level quarantine reasons)
4. data_quality_metrics (aggregated table quality metrics)
"""

import os
import sys
from pathlib import Path

# Ensure project root is in sys.path
PROJECT_ROOT = str(Path(__file__).resolve().parents[2])
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import datetime
from typing import Dict, List, Any, Optional
from google.cloud import bigquery
from google.oauth2 import service_account
from src.utils.schema_loader import ConfigManager

class AuditManager:
    def __init__(self, config_mgr: Optional[ConfigManager] = None):
        self.config_mgr = config_mgr or ConfigManager()
        self.config = self.config_mgr.pipeline_config
        self.project_id = self.config["bigquery"]["project"]
        self.audit_dataset = self.config["bigquery"]["audit_dataset"]
        self.key_path = self.config.get("paths", {}).get("gcp_key_path", "config/gcp-key.json")
        self._client: Optional[bigquery.Client] = None

    @property
    def client(self) -> bigquery.Client:
        if self._client is None:
            if not os.path.exists(self.key_path):
                raise FileNotFoundError(f"GCP service account key not found at: {self.key_path}")
            credentials = service_account.Credentials.from_service_account_file(self.key_path)
            self._client = bigquery.Client(credentials=credentials, project=credentials.project_id)
        return self._client

    def ensure_audit_tables(self):
        """Creates dataset and audit tables if they do not exist."""
        client = self.client
        dataset_id = f"{self.project_id}.{self.audit_dataset}"
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = self.config["bigquery"].get("location", "US")
        client.create_dataset(dataset, exists_ok=True)

        # 1. pipeline_audit
        pipeline_audit_table_id = f"{dataset_id}.pipeline_audit"
        pipeline_audit_schema = [
            bigquery.SchemaField("batch_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("stage", "STRING", mode="REQUIRED"), # BRONZE, SILVER, GOLD
            bigquery.SchemaField("table_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("status", "STRING", mode="REQUIRED"), # STARTED, COMPLETED, FAILED
            bigquery.SchemaField("records_in", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("records_out", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("records_quarantined", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("rejection_rate_pct", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("start_time", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("end_time", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("duration_seconds", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("error_message", "STRING", mode="NULLABLE")
        ]
        t1 = bigquery.Table(pipeline_audit_table_id, schema=pipeline_audit_schema)
        client.create_table(t1, exists_ok=True)

        # 2. rejection_log
        rejection_log_table_id = f"{dataset_id}.rejection_log"
        rejection_log_schema = [
            bigquery.SchemaField("batch_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("table_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("record_pk", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("rule_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("rule_type", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("rejection_reason", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("rejected_payload", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("rejected_at", "TIMESTAMP", mode="NULLABLE")
        ]
        t2 = bigquery.Table(rejection_log_table_id, schema=rejection_log_schema)
        client.create_table(t2, exists_ok=True)

        # 3. data_quality_metrics
        dq_metrics_table_id = f"{dataset_id}.data_quality_metrics"
        dq_metrics_schema = [
            bigquery.SchemaField("batch_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("table_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("total_records", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("valid_records", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("quarantined_records", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("data_quality_score_pct", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("evaluated_at", "TIMESTAMP", mode="REQUIRED")
        ]
        t3 = bigquery.Table(dq_metrics_table_id, schema=dq_metrics_schema)
        client.create_table(t3, exists_ok=True)

    def log_pipeline_step(
        self,
        batch_id: str,
        stage: str,
        table_name: str,
        status: str,
        records_in: Optional[int] = 0,
        records_out: Optional[int] = 0,
        records_quarantined: Optional[int] = 0,
        start_time: Optional[datetime.datetime] = None,
        end_time: Optional[datetime.datetime] = None,
        duration_seconds: Optional[float] = None,
        error_message: Optional[str] = None
    ):
        """Inserts a record into pipeline_audit."""
        self.ensure_audit_tables()
        now_ts = datetime.datetime.now(datetime.timezone.utc)
        s_time = start_time or now_ts
        e_time = end_time or now_ts
        duration = duration_seconds or ((e_time - s_time).total_seconds())

        rejection_rate = 0.0
        if records_in and records_in > 0 and records_quarantined is not None:
            rejection_rate = (records_quarantined / records_in) * 100.0

        row = {
            "batch_id": batch_id,
            "stage": stage,
            "table_name": table_name,
            "status": status,
            "records_in": records_in,
            "records_out": records_out,
            "records_quarantined": records_quarantined,
            "rejection_rate_pct": round(rejection_rate, 2),
            "start_time": s_time.isoformat(),
            "end_time": e_time.isoformat(),
            "duration_seconds": round(duration, 2),
            "error_message": error_message
        }
        table_id = f"{self.project_id}.{self.audit_dataset}.pipeline_audit"
        errors = self.client.insert_rows_json(table_id, [row])
        if errors:
            raise RuntimeError(f"Error inserting into pipeline_audit: {errors}")

    def log_rejections(self, batch_id: str, table_name: str, rejections: List[Dict[str, Any]]):
        """Inserts quarantined rows into rejection_log."""
        if not rejections:
            return
        self.ensure_audit_tables()
        now_ts = datetime.datetime.now(datetime.timezone.utc).isoformat()
        rows = []
        for r in rejections:
            rows.append({
                "batch_id": batch_id,
                "table_name": table_name,
                "record_pk": str(r.get("record_pk", "")),
                "rule_id": str(r.get("rule_id", "")),
                "rule_type": str(r.get("rule_type", "")),
                "rejection_reason": str(r.get("rejection_reason", "")),
                "rejected_payload": str(r.get("rejected_payload", "")),
                "rejected_at": now_ts
            })
        table_id = f"{self.project_id}.{self.audit_dataset}.rejection_log"
        errors = self.client.insert_rows_json(table_id, rows)
        if errors:
            raise RuntimeError(f"Error inserting into rejection_log: {errors}")

    def log_dq_metrics(
        self,
        batch_id: str,
        table_name: str,
        total_records: int,
        valid_records: int,
        quarantined_records: int
    ):
        """Inserts summary score into data_quality_metrics."""
        self.ensure_audit_tables()
        now_ts = datetime.datetime.now(datetime.timezone.utc).isoformat()
        score = (valid_records / total_records * 100.0) if total_records > 0 else 100.0
        row = {
            "batch_id": batch_id,
            "table_name": table_name,
            "total_records": total_records,
            "valid_records": valid_records,
            "quarantined_records": quarantined_records,
            "data_quality_score_pct": round(score, 2),
            "evaluated_at": now_ts
        }
        table_id = f"{self.project_id}.{self.audit_dataset}.data_quality_metrics"
        errors = self.client.insert_rows_json(table_id, [row])
        if errors:
            raise RuntimeError(f"Error inserting into data_quality_metrics: {errors}")

