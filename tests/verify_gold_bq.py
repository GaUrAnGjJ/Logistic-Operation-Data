"""
Verification script for BigQuery logistics_gold dataset:
Checks all dimension and fact tables, row counts, partitioning, and clustering.
"""

from google.cloud import bigquery

def verify_gold():
    client = bigquery.Client.from_service_account_json("config/gcp-key.json")
    dataset_id = "logistic-data-508513.logistics_gold"

    tables = list(client.list_tables(dataset_id))
    print(f"\n========================================================")
    print(f" BigQuery Star Schema Verification: {dataset_id}")
    print(f" Total Tables: {len(tables)}")
    print(f"========================================================")

    for t_item in sorted(tables, key=lambda x: x.table_id):
        table = client.get_table(t_item.reference)
        part_info = f"Partition: {table.time_partitioning.field}" if table.time_partitioning else "No Partition"
        clust_info = f"Clustered: {', '.join(table.clustering_fields)}" if table.clustering_fields else "No Clustering"
        print(f"  {table.table_id:<36} | {table.num_rows:>7,} rows | {part_info:<26} | {clust_info}")

    print("\n--- Testing Key Measure Query (Fact Trips) ---")
    query = """
        SELECT 
            COUNT(1) as total_trips,
            ROUND(SUM(total_revenue), 2) as total_revenue,
            ROUND(SUM(actual_distance_miles), 0) as total_miles,
            ROUND(AVG(average_mpg), 2) as fleet_avg_mpg
        FROM `logistic-data-508513.logistics_gold.fact_trips`
        WHERE batch_id = 'batch_001'
    """
    for r in client.query(query).result():
        print(f"  Trips: {r.total_trips:,} | Revenue: ${r.total_revenue:,.2f} | Miles: {r.total_miles:,.0f} | MPG: {r.fleet_avg_mpg}")

if __name__ == "__main__":
    verify_gold()

