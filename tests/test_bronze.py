"""
Tests for Bronze Layer Ingestion Processor.
"""

import pytest
import pyarrow.parquet as pq
from pathlib import Path
from src.bronze.bronze_processor import BronzeProcessor

def test_bronze_process_table_customers():
    processor = BronzeProcessor()
    # Process customers for batch_001
    res = processor.process_table(batch_id="batch_001", table_name="customers")
    
    assert res["status"] == "COMPLETED"
    assert res["rows"] == 200
    assert "bronze_path" in res
    
    # Check local cached parquet
    local_parquet = Path("data/bronze/customers/batch_id=batch_001/customers_batch_001.parquet")
    assert local_parquet.exists()
    
    table = pq.read_table(local_parquet)
    assert "_batch_id" in table.column_names
    assert "_ingested_at" in table.column_names
    assert "_source_file" in table.column_names
    assert len(table) == 200

def test_bronze_process_table_routes():
    processor = BronzeProcessor()
    res = processor.process_table(batch_id="batch_001", table_name="routes")
    
    assert res["status"] == "COMPLETED"
    assert res["rows"] == 58
    
    local_parquet = Path("data/bronze/routes/batch_id=batch_001/routes_batch_001.parquet")
    assert local_parquet.exists()
    
    table = pq.read_table(local_parquet)
    assert "_batch_id" in table.column_names
    assert len(table) == 58

