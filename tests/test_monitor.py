"""
Unit tests for Pipeline Health Monitor
Run: pytest tests/ -v --cov=src
"""

import sys
import pytest
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from pipeline_monitor import (
    NullChecker,
    DuplicateChecker,
    StatisticalDriftChecker,
    VolumeChecker,
    FreshnessChecker,
)


# ─────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────
@pytest.fixture
def clean_df():
    """Perfectly healthy DataFrame — should produce zero alerts."""
    rng = np.random.default_rng(0)
    return pd.DataFrame({
        "id":      range(1000),
        "revenue": rng.normal(1000, 100, 1000),
        "qty":     rng.integers(1, 50, 1000),
        "date":    [datetime.today() - timedelta(hours=i % 24) for i in range(1000)],
    })


@pytest.fixture
def default_config():
    return {
        "null_checks":      {"warning_threshold": 0.05, "critical_threshold": 0.20},
        "duplicate_checks": {"key_columns": ["id"], "threshold": 0.01},
        "drift_checks":     {"z_score_threshold": 3.0, "ks_pvalue_threshold": 0.05},
        "volume_checks":    {"enabled": True, "drop_threshold": 0.30, "spike_threshold": 2.0},
        "freshness_checks": {"date_columns": ["date"], "max_age_hours": 48},
    }


# ─────────────────────────────────────────────
# NullChecker
# ─────────────────────────────────────────────
class TestNullChecker:
    def test_no_nulls_no_alerts(self, clean_df, default_config):
        alerts = NullChecker().run(clean_df, "test", default_config)
        assert len(alerts) == 0

    def test_warning_on_moderate_nulls(self, default_config):
        df = pd.DataFrame({"id": range(100), "value": [None] * 6 + list(range(94))})
        alerts = NullChecker().run(df, "test", default_config)
        assert any(a.check_type == "null" and a.severity == "WARNING" for a in alerts)

    def test_critical_on_high_nulls(self, default_config):
        df = pd.DataFrame({"id": range(100), "value": [None] * 25 + list(range(75))})
        alerts = NullChecker().run(df, "test", default_config)
        assert any(a.check_type == "null" and a.severity == "CRITICAL" for a in alerts)

    def test_excluded_columns_ignored(self, default_config):
        df = pd.DataFrame({"id": range(100), "notes": [None] * 100})
        config = {**default_config, "null_checks": {
            "warning_threshold": 0.01, "critical_threshold": 0.05,
            "exclude_columns": ["notes"]
        }}
        alerts = NullChecker().run(df, "test", config)
        assert len(alerts) == 0


# ─────────────────────────────────────────────
# DuplicateChecker
# ─────────────────────────────────────────────
class TestDuplicateChecker:
    def test_no_duplicates_no_alerts(self, clean_df, default_config):
        alerts = DuplicateChecker().run(clean_df, "test", default_config)
        assert len(alerts) == 0

    def test_duplicate_detection(self, default_config):
        df = pd.DataFrame({"id": [1, 2, 3, 3, 3] * 20, "value": range(100)})
        # 3 duplicates out of 100 = 3% > 1% threshold
        alerts = DuplicateChecker().run(df, "test", default_config)
        assert any(a.check_type == "duplicate" for a in alerts)

    def test_critical_at_high_dup_rate(self, default_config):
        df = pd.DataFrame({"id": [1] * 100, "value": range(100)})
        alerts = DuplicateChecker().run(df, "test", default_config)
        assert any(a.severity == "CRITICAL" for a in alerts)


# ─────────────────────────────────────────────
# FreshnessChecker
# ─────────────────────────────────────────────
class TestFreshnessChecker:
    def test_fresh_data_no_alert(self, default_config):
        df = pd.DataFrame({
            "id": range(10),
            "date": [datetime.utcnow() - timedelta(hours=1)] * 10
        })
        alerts = FreshnessChecker().run(df, "test", default_config)
        assert len(alerts) == 0

    def test_stale_data_triggers_warning(self, default_config):
        df = pd.DataFrame({
            "id": range(10),
            "date": [datetime.utcnow() - timedelta(hours=50)] * 10
        })
        alerts = FreshnessChecker().run(df, "test", default_config)
        assert any(a.check_type == "freshness" for a in alerts)

    def test_very_stale_data_triggers_critical(self, default_config):
        df = pd.DataFrame({
            "id": range(10),
            "date": [datetime.utcnow() - timedelta(hours=200)] * 10
        })
        alerts = FreshnessChecker().run(df, "test", default_config)
        assert any(a.check_type == "freshness" and a.severity == "CRITICAL" for a in alerts)


# ─────────────────────────────────────────────
# Alert dataclass
# ─────────────────────────────────────────────
class TestAlertModel:
    def test_to_dict_structure(self):
        from pipeline_monitor import Alert
        alert = Alert(
            pipeline_name="sales",
            check_type="null",
            severity="WARNING",
            column="revenue",
            message="High null rate",
            metric_value=0.07,
            threshold=0.05,
        )
        d = alert.to_dict()
        assert set(d.keys()) == {"pipeline", "check", "severity", "column", "message",
                                  "metric_value", "threshold", "timestamp"}
        assert d["severity"] == "WARNING"
