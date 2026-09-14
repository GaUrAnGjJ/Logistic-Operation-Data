"""
Verification script for BigQuery Silver audit tables:
- data_quality_metrics
- rejection_log
"""

from google.cloud import bigquery

def verify_silver_audit():
    client = bigquery.Client.from_service_account_json("config/gcp-key.json")
    
    print("\n--- BigQuery: logistics_audit.data_quality_metrics (batch_001) ---")
    query_dq = """
        SELECT table_name, total_records, valid_records, quarantined_records, data_quality_score_pct
        FROM `logistic-data-508513.logistics_audit.data_quality_metrics`
        WHERE batch_id = 'batch_001'
        ORDER BY evaluated_at DESC
        LIMIT 14
    """
    for r in client.query(query_dq).result():
        print(f"  {r.table_name:<26}: Total {r.total_records:>6} | Valid {r.valid_records:>6} | Quarantined {r.quarantined_records:>4} | Score {r.data_quality_score_pct:>5.1f}%")

    print("\n--- BigQuery: logistics_audit.rejection_log (Sample Quarantined Rows) ---")
    query_rej = """
        SELECT table_name, record_pk, rule_id, rejection_reason
        FROM `logistic-data-508513.logistics_audit.rejection_log`
        WHERE batch_id = 'batch_001'
        LIMIT 5
    """
    for r in client.query(query_rej).result():
        print(f"  [{r.table_name}] PK: {r.record_pk} | Rule: {r.rule_id} | Reason: {r.rejection_reason}")

if __name__ == "__main__":
    verify_silver_audit()

