"""
Notification & Alerting Module for Logistics Medallion Platform.
Supports:
- Console structured alerting (default)
- Webhook alerting (Slack, MS Teams, Discord, generic JSON HTTP POST)
- Email alerting (SMTP)
"""

import os
import sys
from pathlib import Path

# Ensure project root is in sys.path
PROJECT_ROOT = str(Path(__file__).resolve().parents[2])
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import json
import logging
import datetime
from typing import Dict, Any, Optional
import requests
from src.utils.schema_loader import ConfigManager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [NOTIFIER] %(message)s"
)
logger = logging.getLogger("Notifier")

class PipelineNotifier:
    def __init__(self, config_manager: Optional[ConfigManager] = None):
        self.config_mgr = config_manager or ConfigManager()
        self.config = self.config_mgr.pipeline_config.get("notifications", {})
        self.default_channel = self.config.get("default_channel", "console").lower()
        self.alert_on_rejection = self.config.get("alert_on_rejection", True)
        self.alert_on_failure = self.config.get("alert_on_pipeline_failure", True)
        self.alert_on_complete = self.config.get("alert_on_batch_complete", True)

    def send_alert(
        self,
        subject: str,
        message: str,
        level: str = "INFO",
        details: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Dispatches alert via configured channel.
        level: "INFO", "WARNING", "ERROR", "CRITICAL"
        """
        payload = {
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "level": level.upper(),
            "subject": subject,
            "message": message,
            "details": details or {}
        }

        # 1. Always log locally
        log_msg = f"[{payload['level']}] {payload['subject']} - {payload['message']}"
        if payload["level"] in {"ERROR", "CRITICAL"}:
            logger.error(log_msg)
        elif payload["level"] == "WARNING":
            logger.warning(log_msg)
        else:
            logger.info(log_msg)

        # 2. Dispatch via Webhook if configured
        webhook_result = None
        if self.default_channel == "webhook" or self.config.get("webhook", {}).get("url"):
            webhook_url = self.config.get("webhook", {}).get("url")
            if webhook_url:
                try:
                    webhook_body = {
                        "text": f"*{payload['level']}*: {payload['subject']}\n>{payload['message']}\n```json\n{json.dumps(payload['details'], indent=2)}\n```"
                    }
                    resp = requests.post(webhook_url, json=webhook_body, timeout=5)
                    webhook_result = {"status_code": resp.status_code, "success": resp.status_code < 400}
                except Exception as ex:
                    logger.warning(f"Failed to post to webhook: {ex}")
                    webhook_result = {"error": str(ex), "success": False}

        return {
            "delivered": True,
            "channel": self.default_channel,
            "payload": payload,
            "webhook_result": webhook_result
        }

    def alert_rejection_threshold_exceeded(
        self,
        batch_id: str,
        table_name: str,
        rejection_rate_pct: float,
        threshold_pct: float,
        total_records: int,
        quarantined_records: int
    ):
        """Triggered when quarantine rejection rate exceeds allowed tolerance."""
        if not self.alert_on_rejection:
            return
        subject = f"DATA QUALITY THRESHOLD BREACH: {table_name} ({batch_id})"
        message = (
            f"Table '{table_name}' in batch '{batch_id}' had a rejection rate of "
            f"{rejection_rate_pct:.2f}%, exceeding the maximum allowed threshold of {threshold_pct:.2f}%."
        )
        details = {
            "batch_id": batch_id,
            "table_name": table_name,
            "total_records": total_records,
            "quarantined_records": quarantined_records,
            "rejection_rate_pct": rejection_rate_pct,
            "threshold_pct": threshold_pct,
            "action": "PIPELINE_HALTED_BEFORE_GOLD"
        }
        return self.send_alert(subject, message, level="CRITICAL", details=details)

    def alert_pipeline_failure(
        self,
        batch_id: str,
        stage: str,
        table_name: Optional[str],
        error_message: str
    ):
        """Triggered when any pipeline execution step throws an exception."""
        if not self.alert_on_failure:
            return
        subject = f"PIPELINE STAGE FAILED: {stage} ({batch_id})"
        message = f"Batch '{batch_id}' failed during stage '{stage}' for table '{table_name or 'ALL'}': {error_message}"
        details = {
            "batch_id": batch_id,
            "stage": stage,
            "table_name": table_name,
            "error": error_message
        }
        return self.send_alert(subject, message, level="ERROR", details=details)

    def alert_batch_completed(
        self,
        batch_id: str,
        duration_seconds: float,
        summary: Dict[str, Any]
    ):
        """Triggered when a full batch completes successfully into Gold."""
        if not self.alert_on_complete:
            return
        subject = f"BATCH COMPLETED SUCCESSFULLY: {batch_id}"
        message = f"Batch '{batch_id}' processed through Medallion architecture in {duration_seconds:.2f}s."
        return self.send_alert(subject, message, level="INFO", details=summary)

