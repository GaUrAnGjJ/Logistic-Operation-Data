"""
Tests for Schema Definitions, DQ Rules, and ConfigManager.
"""

import pytest
from src.utils.schema_loader import ConfigManager

EXPECTED_TABLES = [
    "customers",
    "drivers",
    "facilities",
    "routes",
    "trailers",
    "trucks",
    "loads",
    "trips",
    "delivery_events",
    "fuel_purchases",
    "maintenance_records",
    "safety_incidents",
    "driver_monthly_metrics",
    "truck_utilization_metrics"
]

@pytest.fixture
def config_mgr():
    return ConfigManager()

def test_pipeline_config_loaded(config_mgr):
    config = config_mgr.pipeline_config
    assert "gcs" in config
    assert "bigquery" in config
    assert "batch" in config
    assert config["bigquery"]["project"] == "logistic-data-508513"
    assert config["bigquery"]["audit_dataset"] == "logistics_audit"
    assert config["bigquery"]["gold_dataset"] == "logistics_gold"

def test_all_14_tables_defined(config_mgr):
    schemas = config_mgr.schemas
    assert len(schemas) == 14, f"Expected 14 tables, found {len(schemas)}"
    for tbl in EXPECTED_TABLES:
        assert tbl in schemas, f"Missing table: {tbl}"
        schema = schemas[tbl]
        assert "columns" in schema
        assert "primary_keys" in schema
        assert len(schema["primary_keys"]) >= 1, f"Table {tbl} must have at least one PK"
        assert len(schema["columns"]) >= 5, f"Table {tbl} should have columns defined"

def test_pyspark_schema_generation(config_mgr):
    for tbl in EXPECTED_TABLES:
        spark_schema = config_mgr.get_pyspark_schema(tbl)
        raw_schema = config_mgr.get_raw_csv_schema(tbl)
        assert len(spark_schema.fields) == len(config_mgr.get_table_schema(tbl)["columns"])
        assert len(raw_schema.fields) == len(spark_schema.fields)

def test_dq_rules_validity(config_mgr):
    rules = config_mgr.dq_rules
    assert len(rules) >= 12, "DQ rules should be defined for majority of operational tables"
    valid_rule_types = {"not_null", "enum", "range", "sql_expression", "regex"}
    
    for tbl, rule_list in rules.items():
        assert tbl in EXPECTED_TABLES, f"Rule table {tbl} is not a valid dataset table"
        for rule in rule_list:
            assert "id" in rule, f"Rule in {tbl} missing id"
            assert "type" in rule, f"Rule {rule.get('id')} missing type"
            assert rule["type"] in valid_rule_types, f"Unknown rule type: {rule['type']}"
            assert "severity" in rule
            assert rule["severity"] in {"ERROR", "WARNING"}
            
            # If column-based rule, verify column exists in schema
            if "column" in rule:
                tbl_cols = config_mgr.get_table_schema(tbl)["columns"]
                assert rule["column"] in tbl_cols, f"Column '{rule['column']}' in rule '{rule['id']}' not in {tbl} schema"

def test_foreign_key_references(config_mgr):
    schemas = config_mgr.schemas
    for tbl, tbl_meta in schemas.items():
        fks = tbl_meta.get("foreign_keys", [])
        for fk in fks:
            parent_table = fk["parent_table"]
            parent_col = fk["parent_column"]
            col = fk["column"]
            assert col in tbl_meta["columns"], f"FK col {col} not in table {tbl}"
            assert parent_table in schemas, f"FK parent table {parent_table} does not exist"
            assert parent_col in schemas[parent_table]["columns"], f"FK parent col {parent_col} not in parent {parent_table}"

