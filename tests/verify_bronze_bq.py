"""
Quick verification of BigQuery pipeline_audit table.
"""

from google.cloud import bigquery

def verify_audit():
    client = bigquery.Client.from_service_account_json("config/gcp-key.json")
    query = """
        SELECT table_name, stage, status, records_in, records_out, duration_seconds
        FROM `logistic-data-508513.logistics_audit.pipeline_audit`
        WHERE batch_id = 'batch_001' AND stage = 'BRONZE'
        ORDER BY start_time DESC
    """
    rows = list(client.query(query).result())
    print(f"\nSuccessfully retrieved {len(rows)} Bronze audit records from BigQuery:")
    for r in rows[:14]:
        print(f"  - {r.table_name:<25}: {r.stage:<8} {r.status:<10} {r.records_in:>6} rows in {r.duration_seconds:>5.2f}s")

if __name__ == "__main__":
    verify_audit()

