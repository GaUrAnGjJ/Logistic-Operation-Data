"""
End-to-End Pipeline Orchestrator for Logistics Data Platform.
Executes the full Medallion Architecture:
Raw / Ingestion -> Bronze -> Silver (DQ & Quarantine) -> Gold (BigQuery Star Schema)
with complete idempotency, audit logging, and alerting.
"""

import os
import sys
import time
import json
import argparse
import datetime
from pathlib import Path
from typing import Dict, Any, Optional

# Ensure project root is in sys.path
PROJECT_ROOT = str(Path(__file__).resolve().parents[2])
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.utils.schema_loader import ConfigManager
from src.ingestion.batch_controller import BatchController
from src.ingestion.gcs_uploader import upload_batch_to_gcs
from src.ingestion.batch_splitter import split_batches
from src.bronze.bronze_processor import BronzeProcessor
from src.silver.silver_validator import SilverValidator
from src.gold.gold_loader import GoldLoader
from src.notification.notifier import PipelineNotifier

class PipelineOrchestrator:
    def __init__(self, config_mgr: Optional[ConfigManager] = None):
        self.config_mgr = config_mgr or ConfigManager()
        self.config = self.config_mgr.pipeline_config
        self.batches_dir = Path(self.config.get("paths", {}).get("batches_dir", "data/batches"))

        self.batch_ctrl = BatchController(self.config)
        self.bronze_proc = BronzeProcessor(self.config_mgr)
        self.silver_val = SilverValidator(self.config_mgr)
        self.gold_load = GoldLoader(self.config_mgr)
        self.notifier = PipelineNotifier(self.config_mgr)

    def run_pipeline(self, batch_id: str, force: bool = False) -> Dict[str, Any]:
        """
        Executes end-to-end pipeline for the specified batch:
        1. Checks / generates local batch files.
        2. Idempotency check: skips if batch is already COMPLETED.
        3. Uploads batch to GCS raw/ and registers PENDING in batch_control.
        4. Ingests raw CSVs into Bronze Parquet.
        5. Validates, cleanses, and quarantines into Silver Parquet.
        6. Builds Dimensions & Partitioned/Clustered Facts and publishes to BigQuery Gold.
        7. Marks batch COMPLETED and sends notification.
        """
        pipeline_start = time.time()
        print(f"\n================================================================================")
        print(f" [ORCHESTRATOR] Starting End-to-End Pipeline Run for: {batch_id}")
        print(f" Timestamp: {datetime.datetime.now(datetime.timezone.utc).isoformat()}")
        print(f"================================================================================")

        # 1. Local Batch Files Check
        batch_folder = self.batches_dir / batch_id
        manifest_file = batch_folder / "manifest.json"
        if not batch_folder.exists() or not manifest_file.exists():
            print(f"[STEP 1/6] Batch folder {batch_folder} not found. Running batch_splitter...")
            split_batches()
        else:
            print(f"[STEP 1/6] Local batch files verified in: {batch_folder}")

        # 2. Idempotency Check in BigQuery
        print(f"[STEP 2/6] Checking batch idempotency in BigQuery batch_control...")
        if not force and self.batch_ctrl.is_batch_completed(batch_id):
            msg = f"[IDEMPOTENCY] Batch '{batch_id}' has ALREADY been COMPLETED in BigQuery. Skipping to prevent duplicate ingestion."
            print(f"\n>>> {msg}\n")
            return {
                "batch_id": batch_id,
                "status": "SKIPPED",
                "reason": "ALREADY_COMPLETED"
            }
        print(f"  -> Batch '{batch_id}' is eligible for processing.")

        # 3. GCS Raw Upload & Batch Control Registration
        print(f"[STEP 3/6] Uploading batch data to GCS raw/ and manifests/...")
        upload_batch_to_gcs(batch_id, self.config)
        with open(manifest_file, "r", encoding="utf-8") as f:
            manifest = json.load(f)
        self.batch_ctrl.register_batch_arrival(batch_id, manifest)
        print(f"  -> Batch '{batch_id}' registered in BigQuery batch_control as PENDING.")

        # 4. Bronze Medallion Layer
        print(f"\n[STEP 4/6] Executing Bronze Medallion Ingestion...")
        bronze_summary = self.bronze_proc.process_batch(batch_id)

        # 5. Silver Medallion Layer (Cleansing, Validation, Quarantine)
        print(f"\n[STEP 5/6] Executing Silver Medallion Validation & Cleansing...")
        silver_summary = self.silver_val.process_batch(batch_id)

        # 6. Gold Medallion Layer (Star Schema & BigQuery Publish)
        print(f"\n[STEP 6/6] Executing Gold Medallion Star Schema Publish...")
        gold_summary = self.gold_load.publish_gold_batch(batch_id)

        total_duration = time.time() - pipeline_start

        print(f"\n================================================================================")
        print(f" [ORCHESTRATOR] Pipeline Run Completed Successfully for: {batch_id}")
        print(f" Total Duration:         {total_duration:.2f}s")
        print(f" Bronze Rows Ingested:   {bronze_summary['total_rows']:,}")
        print(f" Silver Rows Validated:  {silver_summary['valid_records']:,}")
        print(f" Quarantined Rows:       {silver_summary['quarantined_records']:,}")
        print(f" Overall Quality Score:  {silver_summary['dq_score_pct']:.2f}%")
        print(f" Gold Published Rows:    {gold_summary['total_rows_published']:,}")
        print(f" BigQuery Batch Status:  COMPLETED")
        print(f"================================================================================\n")

        return {
            "batch_id": batch_id,
            "status": "COMPLETED",
            "total_duration_seconds": total_duration,
            "bronze": bronze_summary,
            "silver": silver_summary,
            "gold": gold_summary
        }

    def run_all_batches(self, force: bool = False) -> Dict[str, Any]:
        """Runs all configured batches sequentially."""
        batches = self.config.get("batch", {}).get("batches", [])
        overall_start = time.time()
        results = []

        for b in batches:
            b_id = b["id"]
            print(f"\n>>> Triggering Batch: {b_id} ({b.get('description', '')})")
            res = self.run_pipeline(b_id, force=force)
            results.append(res)

        total_elapsed = time.time() - overall_start
        print(f"\n================================================================================")
        print(f" [ORCHESTRATOR] All Batches Completed in {total_elapsed:.2f}s!")
        print(f" Batches Executed: {[r['batch_id'] for r in results]}")
        print(f"================================================================================\n")
        return {
            "status": "COMPLETED",
            "total_duration_seconds": total_elapsed,
            "batch_results": results
        }

def main():
    parser = argparse.ArgumentParser(description="Logistics End-to-End Pipeline Orchestrator")
    parser.add_argument("--batch_id", type=str, default=None, help="Batch ID to process (e.g. batch_002)")
    parser.add_argument("--all", action="store_true", help="Process all batches sequentially")
    parser.add_argument("--force", action="store_true", help="Force re-run even if already completed")
    args = parser.parse_args()

    orchestrator = PipelineOrchestrator()

    if args.all:
        orchestrator.run_all_batches(force=args.force)
    elif args.batch_id:
        orchestrator.run_pipeline(args.batch_id, force=args.force)
    else:
        # Default to batch_002 as incremental batch
        print("No batch specified. Defaulting to incremental 'batch_002'...")
        orchestrator.run_pipeline("batch_002", force=args.force)

if __name__ == "__main__":
    main()

