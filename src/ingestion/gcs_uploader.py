"""
GCS Uploader for Logistics Data Platform.
Uploads local batch CSVs and manifest to Google Cloud Storage raw zone.
Structure:
  gs://<bucket>/raw/<table_name>/<table_name>_<batch_id>.csv
  gs://<bucket>/manifests/<batch_id>_manifest.json
"""

import os
import sys
import json
import argparse
from pathlib import Path
import yaml
from google.cloud import storage
from google.oauth2 import service_account

def load_config(config_path: str = "config/pipeline_config.yaml") -> dict:
    if os.path.exists(config_path):
        with open(config_path, "r") as f:
            return yaml.safe_load(f)
    raise FileNotFoundError(f"Configuration file not found: {config_path}")

def get_storage_client(key_path: str = "config/gcp-key.json") -> storage.Client:
    if not os.path.exists(key_path):
        raise FileNotFoundError(f"GCP service account key not found at {key_path}")
    credentials = service_account.Credentials.from_service_account_file(key_path)
    return storage.Client(credentials=credentials, project=credentials.project_id)

def upload_batch_to_gcs(batch_id: str, config: dict, dry_run: bool = False):
    bucket_name = config["gcs"]["bucket_name"]
    raw_zone = config["gcs"]["zones"]["raw"].strip("/")
    manifest_zone = config["gcs"]["zones"]["manifests"].strip("/")
    batches_dir = Path(config.get("paths", {}).get("batches_dir", "data/batches"))
    key_path = config.get("paths", {}).get("gcp_key_path", "config/gcp-key.json")

    batch_path = batches_dir / batch_id
    manifest_file = batch_path / "manifest.json"

    if not batch_path.exists():
        raise FileNotFoundError(f"Batch directory not found: {batch_path}")
    if not manifest_file.exists():
        raise FileNotFoundError(f"Manifest not found for batch {batch_id}: {manifest_file}")

    with open(manifest_file, "r") as f:
        manifest = json.load(f)

    print(f"\n========================================================")
    print(f"Uploading {batch_id} to GCS bucket: gs://{bucket_name}")
    print(f"========================================================")

    client = get_storage_client(key_path)
    bucket = client.bucket(bucket_name)

    # Upload each table CSV
    for entry in manifest.get("tables", []):
        table_name = entry["table"]
        file_name = entry["file"]
        local_file_path = batch_path / file_name

        if not local_file_path.exists():
            print(f"[WARNING] Local file missing: {local_file_path}, skipping.")
            continue

        destination_blob_name = f"{raw_zone}/{table_name}/{file_name}"
        print(f"  Uploading {file_name} -> gs://{bucket_name}/{destination_blob_name} ...", end=" ", flush=True)

        if not dry_run:
            blob = bucket.blob(destination_blob_name)
            blob.upload_from_filename(str(local_file_path), content_type="text/csv")
            print("DONE")
        else:
            print("[DRY RUN]")

    # Upload manifest.json
    manifest_blob_name = f"{manifest_zone}/{batch_id}_manifest.json"
    print(f"  Uploading manifest -> gs://{bucket_name}/{manifest_blob_name} ...", end=" ", flush=True)
    if not dry_run:
        blob = bucket.blob(manifest_blob_name)
        blob.upload_from_filename(str(manifest_file), content_type="application/json")
        print("DONE")
    else:
        print("[DRY RUN]")

    print(f"\nBatch {batch_id} successfully uploaded to GCS!\n")

def main():
    parser = argparse.ArgumentParser(description="Upload batch files to GCS raw zone.")
    parser.add_argument("--batch_id", type=str, default="batch_001", help="Batch ID to upload (e.g. batch_001)")
    parser.add_argument("--all", action="store_true", help="Upload all batches in data/batches/")
    parser.add_argument("--dry-run", action="store_true", help="Print actions without uploading")
    args = parser.parse_args()

    config = load_config()
    batches_dir = Path(config.get("paths", {}).get("batches_dir", "data/batches"))

    if args.all:
        for batch_dir in sorted(batches_dir.glob("batch_*")):
            if batch_dir.is_dir():
                upload_batch_to_gcs(batch_dir.name, config, dry_run=args.dry_run)
    else:
        upload_batch_to_gcs(args.batch_id, config, dry_run=args.dry_run)

if __name__ == "__main__":
    main()
