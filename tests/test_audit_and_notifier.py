"""
Tests for Notifier and Audit Manager.
"""

import pytest
from src.notification.notifier import PipelineNotifier
from src.audit.audit_manager import AuditManager
from src.utils.schema_loader import ConfigManager

def test_notifier_alert_formatting():
    notifier = PipelineNotifier()
    res = notifier.send_alert("TEST_SUBJECT", "Test message", level="INFO", details={"sample": 123})
    assert res["delivered"] is True
    assert res["channel"] == "console"
    assert res["payload"]["level"] == "INFO"
    assert res["payload"]["subject"] == "TEST_SUBJECT"

def test_notifier_threshold_exceeded():
    notifier = PipelineNotifier()
    res = notifier.alert_rejection_threshold_exceeded(
        batch_id="batch_001",
        table_name="loads",
        rejection_rate_pct=25.5,
        threshold_pct=20.0,
        total_records=100,
        quarantined_records=25
    )
    assert res["delivered"] is True
    assert res["payload"]["level"] == "CRITICAL"
    assert "DATA QUALITY THRESHOLD BREACH" in res["payload"]["subject"]

def test_audit_manager_initialization():
    mgr = AuditManager()
    assert mgr.project_id == "logistic-data-508513"
    assert mgr.audit_dataset == "logistics_audit"
    # Ensure client connects cleanly with credentials
    assert mgr.client is not None
    assert mgr.client.project == "logistic-data-508513"

def test_audit_tables_ensured():
    mgr = AuditManager()
    mgr.ensure_audit_tables()
    # List tables in audit dataset
    tables = [t.table_id for t in mgr.client.list_tables(f"{mgr.project_id}.{mgr.audit_dataset}")]
    assert "batch_control" in tables
    assert "pipeline_audit" in tables
    assert "rejection_log" in tables
    assert "data_quality_metrics" in tables

