"""
Rules Engine for Silver Medallion Layer.
Evaluates declarative Data Quality rules defined in quality_rules.yaml
against in-memory DataFrames and splits records into:
1. Valid records (moving to Silver)
2. Quarantined records (with specific rejection reasons and rule IDs)
"""

import os
import sys
import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Any, Optional
import pandas as pd
import numpy as np

# Ensure project root is in sys.path
PROJECT_ROOT = str(Path(__file__).resolve().parents[2])
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.utils.schema_loader import ConfigManager

class RulesEngine:
    def __init__(self, config_mgr: Optional[ConfigManager] = None):
        self.config_mgr = config_mgr or ConfigManager()

    def evaluate_table(
        self,
        table_name: str,
        df: pd.DataFrame,
        batch_id: str
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Applies DQ rules for table_name to df.
        Returns:
            (valid_df, quarantined_df)
        """
        if df.empty:
            return df.copy(), pd.DataFrame()

        rules = self.config_mgr.get_table_rules(table_name)
        pk_cols = self.config_mgr.get_primary_keys(table_name)

        # Working copy
        work_df = df.copy()

        # Add tracking columns for validation failure
        failed_mask = pd.Series(False, index=work_df.index)
        rejection_rules = pd.Series("", index=work_df.index, dtype=str)
        rejection_reasons = pd.Series("", index=work_df.index, dtype=str)

        # 1. Primary Key Uniqueness Check
        if pk_cols and all(col in work_df.columns for col in pk_cols):
            dup_mask = work_df.duplicated(subset=pk_cols, keep=False)
            if dup_mask.any():
                failed_mask |= dup_mask
                reason = f"Duplicate primary key ({', '.join(pk_cols)})"
                rejection_rules = np.where(dup_mask & (rejection_rules == ""), "pk_uniqueness", rejection_rules)
                rejection_reasons = np.where(dup_mask & (rejection_reasons == ""), reason, rejection_reasons)

        # 2. Process Declarative Rules from quality_rules.yaml
        for rule in rules:
            rule_id = rule.get("id", "unnamed_rule")
            rule_type = rule.get("type")
            col = rule.get("column")
            desc = rule.get("description", rule_id)
            severity = rule.get("severity", "ERROR")

            if severity != "ERROR":
                continue # Warnings are logged without quarantining

            current_failed = pd.Series(False, index=work_df.index)

            if rule_type == "not_null" and col in work_df.columns:
                current_failed = work_df[col].isna() | (work_df[col].astype(str).str.strip().isin(["", "nan", "None", "NULL"]))

            elif rule_type == "enum" and col in work_df.columns:
                allowed = [str(v).strip().lower() for v in rule.get("allowed_values", [])]
                clean_vals = work_df[col].astype(str).str.strip().str.lower()
                # If column allows null, don't fail null values in enum check
                is_null = work_df[col].isna() | (clean_vals.isin(["", "nan", "none", "null"]))
                current_failed = (~clean_vals.isin(allowed)) & (~is_null)

            elif rule_type == "range" and col in work_df.columns:
                min_val = rule.get("min")
                max_val = rule.get("max")
                num_series = pd.to_numeric(work_df[col], errors="coerce")
                
                # Check for invalid non-null numbers
                non_null = work_df[col].notna() & (~work_df[col].astype(str).str.strip().isin(["", "nan", "none", "null"]))
                is_invalid_num = non_null & num_series.isna()
                
                out_of_bounds = pd.Series(False, index=work_df.index)
                if min_val is not None:
                    out_of_bounds |= (num_series < min_val)
                if max_val is not None:
                    out_of_bounds |= (num_series > max_val)
                
                current_failed = is_invalid_num | (out_of_bounds & num_series.notna())

            elif rule_type == "sql_expression":
                expr = rule.get("expression", "")
                try:
                    # Handle common date logic, e.g. termination_date IS NULL OR termination_date >= hire_date
                    if "termination_date" in expr and "hire_date" in expr:
                        hire_dt = pd.to_datetime(work_df["hire_date"], errors="coerce")
                        term_dt = pd.to_datetime(work_df["termination_date"], errors="coerce")
                        # Failed if term_dt is not null AND term_dt < hire_dt
                        current_failed = term_dt.notna() & (term_dt < hire_dt)
                    elif "scheduled_datetime" in expr and "actual_datetime" in expr:
                        sched = pd.to_datetime(work_df["scheduled_datetime"], errors="coerce")
                        act = pd.to_datetime(work_df["actual_datetime"], errors="coerce")
                        current_failed = sched.isna() | act.isna()
                except Exception as ex:
                    print(f"[RULES ENGINE] Error evaluating sql expression '{expr}': {ex}")

            # Apply failures
            if current_failed.any():
                newly_failed = current_failed & (~failed_mask)
                failed_mask |= current_failed
                rejection_rules = np.where(newly_failed, rule_id, rejection_rules)
                rejection_reasons = np.where(newly_failed, desc, rejection_reasons)

        # Construct primary key string identifier for quarantined rows
        if pk_cols and all(col in work_df.columns for col in pk_cols):
            pk_series = work_df[pk_cols[0]].astype(str)
            for c in pk_cols[1:]:
                pk_series = pk_series + "_" + work_df[c].astype(str)
        else:
            pk_series = pd.Series([f"row_{i}" for i in range(len(work_df))], index=work_df.index)

        now_ts = datetime.datetime.now(datetime.timezone.utc).isoformat()

        # Split into Valid and Quarantined
        valid_df = work_df[~failed_mask].copy()
        quarantined_df = work_df[failed_mask].copy()

        if not quarantined_df.empty:
            quarantined_df["_record_pk"] = pk_series[failed_mask]
            quarantined_df["_rejection_rule"] = rejection_rules[failed_mask]
            quarantined_df["_rejection_reason"] = rejection_reasons[failed_mask]
            quarantined_df["_rejected_at"] = now_ts
            quarantined_df["_batch_id"] = batch_id

        return valid_df, quarantined_df

