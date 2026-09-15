"""
deploy_powerbi_views.py
=======================
Deploys the 8 semantic BigQuery SQL views defined in sql/bigquery/powerbi_views.sql
into Google BigQuery:
  - 5 views in logistics_gold
  - 3 views in logistics_audit

Usage:
  python scripts/deploy_powerbi_views.py
"""

import os
import sys
from pathlib import Path

PROJECT_ROOT = str(Path(__file__).resolve().parent.parent)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import yaml
from google.cloud import bigquery
from google.oauth2 import service_account

def load_config(config_path: str = "config/pipeline_config.yaml") -> dict:
    with open(config_path, "r") as f:
        return yaml.safe_load(f)

def get_bigquery_client(key_path: str) -> bigquery.Client:
    if not os.path.exists(key_path):
        raise FileNotFoundError(f"Service account key not found: {key_path}")
    credentials = service_account.Credentials.from_service_account_file(key_path)
    return bigquery.Client(credentials=credentials, project=credentials.project_id)

def deploy_views():
    config = load_config()
    key_path = config.get("paths", {}).get("gcp_key_path", "config/gcp-key.json")
    client = get_bigquery_client(key_path)

    sql_file = Path(PROJECT_ROOT) / "sql" / "bigquery" / "powerbi_views.sql"
    if not sql_file.exists():
        print(f"Error: SQL file not found at {sql_file}")
        sys.exit(1)

    print(f"\n========================================================")
    print(f" Deploying Semantic Views to Google BigQuery")
    print(f" Source: {sql_file}")
    print(f" Target Project: {client.project}")
    print(f"========================================================\n")

    with open(sql_file, "r", encoding="utf-8") as f:
        full_sql = f.read()

    # Split on semicolon, filter out empty statements
    statements = [stmt.strip() for stmt in full_sql.split(";") if stmt.strip()]

    view_count = 0
    for idx, stmt in enumerate(statements, 1):
        # Extract view name from statement for friendly logging
        view_name = "Unknown"
        lines = stmt.splitlines()
        for line in lines:
            if "CREATE OR REPLACE VIEW" in line.upper():
                parts = line.split("`")
                if len(parts) >= 2:
                    view_name = parts[1]
                else:
                    view_name = line.strip()
                break

        print(f"[{idx}/{len(statements)}] Deploying: {view_name} ...", end=" ", flush=True)
        try:
            query_job = client.query(stmt)
            query_job.result()  # Wait for view creation to complete
            print("SUCCESS")
            view_count += 1
        except Exception as e:
            print(f"FAILED!\nError: {e}\n")

    print(f"\n========================================================")
    print(f" Successfully deployed {view_count}/{len(statements)} views in BigQuery!")
    print(f"========================================================\n")

if __name__ == "__main__":
    deploy_views()

