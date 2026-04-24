"""
=============================================================
Data Quality Checks
Null · Duplicate · Statistical Drift
=============================================================
"""

import pandas as pd
import numpy as np
from abc import ABC, abstractmethod


class BaseCheck(ABC):
    @abstractmethod
    def execute(self, df: pd.DataFrame) -> dict:
        pass


# ─────────────────────────────────────────
# CHECK 1: Null / Missing Values
# ─────────────────────────────────────────
class NullCheck(BaseCheck):
    """
    Raises issue when the null % in any column exceeds the threshold.
    Default threshold: 5% per column.
    """

    def __init__(self, thresholds: dict):
        self.thresholds = thresholds  # e.g. {"revenue": 0.0, "customer_id": 0.0}
        self.default_threshold = 0.05

    def execute(self, df: pd.DataFrame) -> dict:
        issues = []
        for col in df.columns:
            null_pct = df[col].isna().mean()
            threshold = self.thresholds.get(col, self.default_threshold)
            if null_pct > threshold:
                issues.append({
                    "check": "NullCheck",
                    "column": col,
                    "severity": "CRITICAL" if null_pct > 0.3 else "WARNING",
                    "detail": f"{null_pct:.1%} nulls found (max allowed: {threshold:.1%})",
                    "null_pct": round(null_pct, 4),
                    "threshold": threshold,
                })

        return {
            "check": "NullCheck",
            "passed": len(issues) == 0,
            "issues": issues,
        }


# ─────────────────────────────────────────
# CHECK 2: Duplicate Records
# ─────────────────────────────────────────
class DuplicateCheck(BaseCheck):
    """
    Detects duplicate rows based on key columns.
    If no keys provided, checks full-row duplicates.
    """

    def __init__(self, key_columns: list):
        self.key_columns = key_columns

    def execute(self, df: pd.DataFrame) -> dict:
        issues = []
        cols = self.key_columns if self.key_columns else df.columns.tolist()
        # Only check columns that exist in this df
        cols = [c for c in cols if c in df.columns]

        dup_count = df.duplicated(subset=cols).sum()
        dup_pct = dup_count / len(df) if len(df) > 0 else 0

        if dup_count > 0:
            issues.append({
                "check": "DuplicateCheck",
                "column": str(cols),
                "severity": "CRITICAL" if dup_pct > 0.1 else "WARNING",
                "detail": f"{dup_count} duplicate rows ({dup_pct:.1%} of total)",
                "duplicate_count": int(dup_count),
                "duplicate_pct": round(dup_pct, 4),
            })

        return {
            "check": "DuplicateCheck",
            "passed": len(issues) == 0,
            "issues": issues,
        }


# ─────────────────────────────────────────
# CHECK 3: Statistical Drift (Data Drift)
# ─────────────────────────────────────────
class StatisticalDriftCheck(BaseCheck):
    """
    Detects distribution drift in numeric columns using Z-score method.
    Compares current batch mean/std against expected baseline.

    In production: baselines come from a metadata store updated weekly.
    Here: baselines are passed via config or auto-computed from first run.
    """

    def __init__(self, thresholds: dict):
        # e.g. {"revenue": {"mean": 5000, "std": 1200}, "quantity": {"mean": 50, "std": 15}}
        self.baselines = thresholds
        self.z_threshold = 3.0  # flag if mean deviates > 3 std from baseline

    def execute(self, df: pd.DataFrame) -> dict:
        issues = []
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()

        for col in numeric_cols:
            if col not in self.baselines:
                continue

            baseline = self.baselines[col]
            base_mean = baseline.get("mean")
            base_std = baseline.get("std")

            if base_std is None or base_std == 0:
                continue

            current_mean = df[col].dropna().mean()
            z_score = abs((current_mean - base_mean) / base_std)

            if z_score > self.z_threshold:
                issues.append({
                    "check": "StatisticalDriftCheck",
                    "column": col,
                    "severity": "CRITICAL" if z_score > 5 else "WARNING",
                    "detail": (
                        f"Mean drift detected: current={current_mean:.2f}, "
                        f"baseline={base_mean:.2f}, Z-score={z_score:.2f}"
                    ),
                    "current_mean": round(float(current_mean), 4),
                    "baseline_mean": base_mean,
                    "z_score": round(float(z_score), 4),
                })

        return {
            "check": "StatisticalDriftCheck",
            "passed": len(issues) == 0,
            "issues": issues,
        }
