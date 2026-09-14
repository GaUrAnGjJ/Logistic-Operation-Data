"""
Tests for RulesEngine, SilverValidator, and Quarantine Mechanism.
"""

import pytest
import pandas as pd
from pathlib import Path
from src.silver.rules_engine import RulesEngine
from src.silver.silver_validator import SilverValidator, DataQualityThresholdException

def test_rules_engine_not_null_and_range():
    engine = RulesEngine()
    # Create sample loads DataFrame with 1 valid row, 1 null PK row, 1 out-of-range weight row
    sample_loads = pd.DataFrame([
        {
            "load_id": "LD_001",
            "customer_id": "CUST_001",
            "route_id": "RT_001",
            "load_date": "2022-01-01",
            "load_type": "Dry Van",
            "weight_lbs": 35000,
            "pieces": 10,
            "revenue": 1500.0,
            "fuel_surcharge": 120.0,
            "accessorial_charges": 0,
            "load_status": "Completed",
            "booking_type": "Dedicated"
        },
        {
            "load_id": None, # Should fail not_null
            "customer_id": "CUST_002",
            "route_id": "RT_002",
            "load_date": "2022-01-02",
            "load_type": "Dry Van",
            "weight_lbs": 25000,
            "pieces": 5,
            "revenue": 1200.0,
            "fuel_surcharge": 100.0,
            "accessorial_charges": 0,
            "load_status": "Completed",
            "booking_type": "Spot"
        },
        {
            "load_id": "LD_003",
            "customer_id": "CUST_003",
            "route_id": "RT_003",
            "load_date": "2022-01-03",
            "load_type": "Dry Van",
            "weight_lbs": 95000, # Should fail range (legal max 80,000)
            "pieces": 8,
            "revenue": 2000.0,
            "fuel_surcharge": 150.0,
            "accessorial_charges": 0,
            "load_status": "Completed",
            "booking_type": "Contract"
        }
    ])

    valid_df, quarantined_df = engine.evaluate_table("loads", sample_loads, batch_id="batch_test")
    assert len(valid_df) == 1
    assert len(quarantined_df) == 2
    assert "load_pk_not_null" in quarantined_df["_rejection_rule"].values
    assert "load_weight_positive" in quarantined_df["_rejection_rule"].values

def test_rules_engine_enum_validation():
    engine = RulesEngine()
    sample_customers = pd.DataFrame([
        {
            "customer_id": "CUST_100",
            "customer_name": "Valid Corp",
            "customer_type": "Contract",
            "credit_terms_days": 30,
            "primary_freight_type": "General",
            "account_status": "Active",
            "contract_start_date": "2020-01-01",
            "annual_revenue_potential": 500000
        },
        {
            "customer_id": "CUST_200",
            "customer_name": "Bad Enum Corp",
            "customer_type": "INVALID_TYPE", # Should fail enum check
            "credit_terms_days": 30,
            "primary_freight_type": "General",
            "account_status": "Active",
            "contract_start_date": "2020-01-01",
            "annual_revenue_potential": 250000
        }
    ])

    valid_df, quarantined_df = engine.evaluate_table("customers", sample_customers, batch_id="batch_test")
    assert len(valid_df) == 1
    assert len(quarantined_df) == 1
    assert quarantined_df.iloc[0]["_rejection_rule"] == "cust_type_enum"

def test_silver_process_table():
    validator = SilverValidator()
    # Process customers for batch_001
    res = validator.process_table(batch_id="batch_001", table_name="customers")
    assert res["status"] == "COMPLETED"
    assert res["total_records"] == 200
    assert res["valid_records"] > 0
    assert res["dq_score_pct"] >= 95.0

    # Verify silver parquet written
    silver_parquet = Path("data/silver/customers/batch_id=batch_001/customers_batch_001.parquet")
    assert silver_parquet.exists()

