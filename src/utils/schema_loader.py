"""
Schema & Configuration Loader for Logistics Medallion Platform.
Provides unified access to:
- pipeline_config.yaml
- tables_schema.yaml (with PySpark StructType conversions)
- quality_rules.yaml
"""

import os
from pathlib import Path
from typing import Dict, List, Any, Optional
import yaml
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    DoubleType,
    FloatType,
    DateType,
    TimestampType,
    BooleanType,
    DataType
)

SPARK_TYPE_MAP: Dict[str, DataType] = {
    "string": StringType(),
    "integer": IntegerType(),
    "long": LongType(),
    "double": DoubleType(),
    "float": FloatType(),
    "date": DateType(),
    "timestamp": TimestampType(),
    "boolean": BooleanType()
}

class ConfigManager:
    def __init__(self, base_dir: Optional[str] = None):
        self.base_dir = Path(base_dir) if base_dir else Path(__file__).resolve().parents[2]
        self._pipeline_config: Optional[Dict[str, Any]] = None
        self._schemas: Optional[Dict[str, Any]] = None
        self._rules: Optional[Dict[str, Any]] = None

    @property
    def pipeline_config(self) -> Dict[str, Any]:
        if self._pipeline_config is None:
            config_path = self.base_dir / "config" / "pipeline_config.yaml"
            if not config_path.exists():
                raise FileNotFoundError(f"Pipeline config not found at: {config_path}")
            with open(config_path, "r", encoding="utf-8") as f:
                self._pipeline_config = yaml.safe_load(f)
        return self._pipeline_config

    @property
    def schemas(self) -> Dict[str, Any]:
        if self._schemas is None:
            schema_rel_path = self.pipeline_config.get("data_quality", {}).get(
                "schema_definitions_path", "config/schema_definitions/tables_schema.yaml"
            )
            schema_path = self.base_dir / schema_rel_path
            if not schema_path.exists():
                raise FileNotFoundError(f"Schema definitions not found at: {schema_path}")
            with open(schema_path, "r", encoding="utf-8") as f:
                content = yaml.safe_load(f)
                self._schemas = content.get("tables", {})
        return self._schemas

    @property
    def dq_rules(self) -> Dict[str, Any]:
        if self._rules is None:
            rules_rel_path = self.pipeline_config.get("data_quality", {}).get(
                "dq_rules_path", "config/dq_rules/quality_rules.yaml"
            )
            rules_path = self.base_dir / rules_rel_path
            if not rules_path.exists():
                raise FileNotFoundError(f"DQ rules not found at: {rules_path}")
            with open(rules_path, "r", encoding="utf-8") as f:
                content = yaml.safe_load(f)
                self._rules = content.get("rules", {})
        return self._rules

    def get_table_schema(self, table_name: str) -> Dict[str, Any]:
        """Returns the dictionary schema definition for a given table."""
        if table_name not in self.schemas:
            raise KeyError(f"Table '{table_name}' not found in schema definitions.")
        return self.schemas[table_name]

    def get_pyspark_schema(self, table_name: str) -> StructType:
        """Converts the YAML schema of a table into a PySpark StructType."""
        table_meta = self.get_table_schema(table_name)
        fields = []
        for col_name, col_def in table_meta.get("columns", {}).items():
            type_str = col_def.get("type", "string").lower()
            spark_type = SPARK_TYPE_MAP.get(type_str, StringType())
            nullable = col_def.get("nullable", True)
            fields.append(StructField(col_name, spark_type, nullable=nullable))
        return StructType(fields)

    def get_raw_csv_schema(self, table_name: str) -> StructType:
        """
        Returns a schema suitable for raw CSV reading where all columns are StringType
        to prevent parse crashes during Bronze ingestion, allowing Silver to handle
        rigorous casting, cleansing, and validation.
        """
        table_meta = self.get_table_schema(table_name)
        fields = []
        for col_name in table_meta.get("columns", {}).keys():
            fields.append(StructField(col_name, StringType(), nullable=True))
        return StructType(fields)

    def get_table_rules(self, table_name: str) -> List[Dict[str, Any]]:
        """Returns list of DQ rules for a given table."""
        return self.dq_rules.get(table_name, [])

    def get_primary_keys(self, table_name: str) -> List[str]:
        return self.get_table_schema(table_name).get("primary_keys", [])

    def get_foreign_keys(self, table_name: str) -> List[Dict[str, Any]]:
        return self.get_table_schema(table_name).get("foreign_keys", [])

