"""
=============================================================
Data Pipeline Health Monitor — Core Engine
Author: Francian Rodrigues Santos
Context: Inspired by real results — +30% data reliability
         achieved at Very Tecnologia & JSB Distribuidora.
=============================================================
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime
from dataclasses import dataclass, field
from typing import Optional
from src.alerting import AlertManager
from src.checks import NullCheck, DuplicateCheck, StatisticalDriftCheck
from src.report import ReportGenerator


@dataclass
class PipelineHealthResult:
    pipeline_name: str
    table_name: str
    run_at: str
    total_rows: int
    checks_passed: int
    checks_failed: int
    issues: list = field(default_factory=list)
    overall_status: str = "OK"  # OK | WARNING | CRITICAL

    def to_dict(self):
        return {
            "pipeline_name": self.pipeline_name,
            "table_name": self.table_name,
            "run_at": self.run_at,
            "total_rows": self.total_rows,
            "checks_passed": self.checks_passed,
            "checks_failed": self.checks_failed,
            "issues": self.issues,
            "overall_status": self.overall_status,
        }


class DataPipelineMonitor:
    """
    Orchestrates all health checks for a given data pipeline.
    Designed to run autonomously — zero manual intervention needed.
    """

    def __init__(self, config: dict, alert_manager: AlertManager):
        self.config = config
        self.alert_manager = alert_manager
        self.checks = [
            NullCheck(config.get("null_thresholds", {})),
            DuplicateCheck(config.get("duplicate_keys", [])),
            StatisticalDriftCheck(config.get("drift_thresholds", {})),
        ]

    def run(self, df: pd.DataFrame, pipeline_name: str, table_name: str) -> PipelineHealthResult:
        run_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        all_issues = []
        checks_passed = 0
        checks_failed = 0

        for check in self.checks:
            result = check.execute(df)
            if result["passed"]:
                checks_passed += 1
            else:
                checks_failed += 1
                all_issues.extend(result["issues"])

        status = self._compute_status(checks_failed, len(self.checks))

        health = PipelineHealthResult(
            pipeline_name=pipeline_name,
            table_name=table_name,
            run_at=run_at,
            total_rows=len(df),
            checks_passed=checks_passed,
            checks_failed=checks_failed,
            issues=all_issues,
            overall_status=status,
        )

        if status in ("WARNING", "CRITICAL"):
            self.alert_manager.send(health)

        return health

    def _compute_status(self, failed: int, total: int) -> str:
        if failed == 0:
            return "OK"
        ratio = failed / total
        if ratio >= 0.5:
            return "CRITICAL"
        return "WARNING"
