"""
=============================================================
Unit Tests — Checks & Monitor Core Logic
=============================================================
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pandas as pd
import numpy as np
import pytest
from src.checks import NullCheck, DuplicateCheck, StatisticalDriftCheck


# ─────────────────────────────────────────
# NullCheck
# ─────────────────────────────────────────
class TestNullCheck:
    def test_passes_when_no_nulls(self):
        df = pd.DataFrame({"revenue": [100.0, 200.0, 300.0]})
        result = NullCheck({"revenue": 0.0}).execute(df)
        assert result["passed"] is True

    def test_fails_when_nulls_exceed_threshold(self):
        df = pd.DataFrame({"revenue": [None, None, 300.0]})  # 66% null
        result = NullCheck({"revenue": 0.0}).execute(df)
        assert result["passed"] is False
        assert result["issues"][0]["column"] == "revenue"

    def test_uses_default_threshold_for_unknown_columns(self):
        df = pd.DataFrame({"notes": [None, "ok", "ok", "ok", "ok",
                                     "ok", "ok", "ok", "ok", "ok",
                                     "ok", "ok", "ok", "ok", "ok",
                                     "ok", "ok", "ok", "ok", "ok"]})
        # 1 null / 20 = 5% — right at default threshold, should pass
        result = NullCheck({}).execute(df)
        assert result["passed"] is True


# ─────────────────────────────────────────
# DuplicateCheck
# ─────────────────────────────────────────
class TestDuplicateCheck:
    def test_passes_when_no_duplicates(self):
        df = pd.DataFrame({"order_id": ["A", "B", "C"]})
        result = DuplicateCheck(["order_id"]).execute(df)
        assert result["passed"] is True

    def test_fails_when_duplicates_exist(self):
        df = pd.DataFrame({"order_id": ["A", "A", "C"]})
        result = DuplicateCheck(["order_id"]).execute(df)
        assert result["passed"] is False
        assert result["issues"][0]["duplicate_count"] == 1

    def test_full_row_dedup_when_no_keys(self):
        df = pd.DataFrame({"a": [1, 1, 3], "b": [1, 1, 3]})
        result = DuplicateCheck([]).execute(df)
        assert result["passed"] is False


# ─────────────────────────────────────────
# StatisticalDriftCheck
# ─────────────────────────────────────────
class TestStatisticalDriftCheck:
    def test_passes_when_no_drift(self):
        np.random.seed(42)
        df = pd.DataFrame({"revenue": np.random.normal(5000, 100, 1000)})
        result = StatisticalDriftCheck({"revenue": {"mean": 5000, "std": 100}}).execute(df)
        assert result["passed"] is True

    def test_fails_when_mean_drifts_significantly(self):
        # Mean is 2000 but baseline expects 5000 std=100 — huge Z-score
        df = pd.DataFrame({"revenue": [2000.0] * 100})
        result = StatisticalDriftCheck({"revenue": {"mean": 5000, "std": 100}}).execute(df)
        assert result["passed"] is False
        assert result["issues"][0]["z_score"] > 3

    def test_ignores_columns_not_in_config(self):
        df = pd.DataFrame({"mystery_col": [1, 2, 3, 4, 5]})
        result = StatisticalDriftCheck({}).execute(df)
        assert result["passed"] is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
