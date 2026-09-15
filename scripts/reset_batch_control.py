"""
reset_batch_control.py
======================
Utility script to clear BigQuery batch_control streaming buffer by dropping and
recreating the table, then re-seeding known-COMPLETED batches.

Run from project root:
    python scripts/reset_batch_control.py
"""

import sys
import datetime
from pathlib import Path

PROJECT_ROOT = str(Path(__file__).resolve().parent.parent)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.ingestion.batch_controller import BatchController, load_config, get_bigquery_client
from google.cloud import bigquery

def reset_batch_control():
    config = load_config()
    project_id = config["bigquery"]["project"]
    audit_dataset = config["bigquery"]["audit_dataset"]
    full_table_id = f"{project_id}.{audit_dataset}.batch_control"
    key_path = config.get("paths", {}).get("gcp_key_path", "config/gcp-key.json")
    client = get_bigquery_client(key_path)

    print(f"\n{'='*60}")
    print(f"  Resetting batch_control: {full_table_id}")
    print(f"{'='*60}")

    # Step 1: Drop the table (clears all data + streaming buffer)
    print("\n[1/3] Dropping batch_control table...")
    drop_sql = f"DROP TABLE IF EXISTS `{full_table_id}`"
    client.query(drop_sql).result()
    print("      Done.")

    # Step 2: Recreate via BatchController (creates fresh empty table)
    print("\n[2/3] Recreating batch_control table...")
    ctrl = BatchController(config)
    ctrl.ensure_audit_dataset_and_table()
    print("      Done.")

    # Step 3: Re-seed completed batches using SQL INSERT
    now = datetime.datetime.now(datetime.timezone.utc).isoformat()
    completed_batches = ["batch_001", "batch_002"]  # Known completed batches

    print(f"\n[3/3] Re-seeding {len(completed_batches)} completed batches...")
    for batch_id in completed_batches:
        insert_sql = f"""
            INSERT INTO `{full_table_id}`
                (batch_id, source_file, table_name, gcs_raw_path,
                 arrival_time, processing_start, processing_end,
                 records_in_file, status, error_message)
            VALUES
                ('{batch_id}', '__STATUS__', 'COMPLETED', NULL,
                 TIMESTAMP('{now}'), NULL, TIMESTAMP('{now}'),
                 0, 'COMPLETED', NULL)
        """
        client.query(insert_sql).result()
        print(f"      Re-seeded {batch_id} -> COMPLETED")

    print(f"\n{'='*60}")
    print(f"  batch_control reset complete!")
    print(f"  {len(completed_batches)} batches restored as COMPLETED.")
    print(f"  batch_003 is now eligible for fresh processing.")
    print(f"{'='*60}\n")

if __name__ == "__main__":
    reset_batch_control()

