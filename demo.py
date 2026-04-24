"""
=============================================================
Demo: Sales KPI Pipeline Monitor
Simulates the exact scenario from JSB Distribuidora & Very Tecnologia:
monitoring sell-out, revenue and stock data for commercial dashboards.

This demo shows:
  - A healthy pipeline run
  - A degraded run (nulls + duplicates + drift injected)
  - Reliability score over time
=============================================================
"""

import os
import sys
import pandas as pd
import numpy as np

# allow running from project root
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.monitor import DataPipelineMonitor
from src.alerting import AlertManager
from src.metadata_store import MetadataStore
from src.report import ReportGenerator

# ─────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────
CONFIG = {
    "null_thresholds": {
        "revenue":     0.00,   # revenue must NEVER be null
        "customer_id": 0.00,   # PK — zero tolerance
        "quantity":    0.02,
        "product_sku": 0.01,
    },
    "duplicate_keys": ["order_id"],
    "drift_thresholds": {
        "revenue":  {"mean": 5_000.0, "std": 1_200.0},
        "quantity": {"mean": 50.0,    "std": 15.0},
    },
}

# Alerts: disabled by default in demo (set env vars to enable)
ALERT_CONFIG = {
    "telegram": {
        "enabled": bool(os.getenv("TELEGRAM_BOT_TOKEN")),
        "bot_token": os.getenv("TELEGRAM_BOT_TOKEN"),
        "chat_id": os.getenv("TELEGRAM_CHAT_ID"),
    },
    "email": {
        "enabled": bool(os.getenv("EMAIL_SENDER")),
        "sender": os.getenv("EMAIL_SENDER"),
        "password": os.getenv("EMAIL_PASSWORD"),
        "recipients": [os.getenv("EMAIL_TO", "")],
    },
}


def make_healthy_batch(n=5000) -> pd.DataFrame:
    """Simulate a clean sales batch — as expected in production."""
    np.random.seed(42)
    return pd.DataFrame({
        "order_id":    [f"ORD-{i:06d}" for i in range(n)],
        "customer_id": [f"CLI-{np.random.randint(1, 2000):05d}" for _ in range(n)],
        "product_sku": [f"SKU-{np.random.randint(100, 999)}" for _ in range(n)],
        "quantity":    np.random.normal(50, 15, n).clip(1).astype(int),
        "revenue":     np.random.normal(5000, 1200, n).clip(100).round(2),
        "region":      np.random.choice(["Nordeste", "Sudeste", "Sul", "Centro-Oeste"], n),
        "loaded_at":   pd.Timestamp("2024-11-01"),
    })


def make_degraded_batch(n=5000) -> pd.DataFrame:
    """Simulate a batch with injected issues — what we detect and alert on."""
    np.random.seed(99)
    df = make_healthy_batch(n)

    # Inject nulls in revenue (8% — above 0% threshold)
    null_idx = np.random.choice(df.index, size=int(n * 0.08), replace=False)
    df.loc[null_idx, "revenue"] = np.nan

    # Inject duplicates on order_id (5%)
    dup_idx = np.random.choice(df.index, size=int(n * 0.05), replace=False)
    df.loc[dup_idx, "order_id"] = "ORD-000001"

    # Inject drift: revenue mean drops by 40% (fraud / pricing bug simulation)
    df["revenue"] = df["revenue"] * 0.6

    return df


def run_demo():
    reporter = ReportGenerator()
    alert_mgr = AlertManager(ALERT_CONFIG)
    store = MetadataStore("data/pipeline_metadata.db")
    monitor = DataPipelineMonitor(CONFIG, alert_mgr)

    print("\n" + "═" * 60)
    print("  DATA PIPELINE HEALTH MONITOR — Demo")
    print("  Simulating: Sales KPI Pipeline (JSB / Very Tecnologia context)")
    print("═" * 60)

    # ── RUN 1: Healthy ────────────────────────────────────────
    print("\n[1/2] Running HEALTHY batch...")
    df_ok = make_healthy_batch()
    result_ok = monitor.run(df_ok, pipeline_name="sales_kpi_pipeline", table_name="fct_orders")
    reporter.print_summary(result_ok)
    store.save_run(result_ok)

    # ── RUN 2: Degraded ───────────────────────────────────────
    print("[2/2] Running DEGRADED batch (injected issues)...")
    df_bad = make_degraded_batch()
    result_bad = monitor.run(df_bad, pipeline_name="sales_kpi_pipeline", table_name="fct_orders")
    reporter.print_summary(result_bad)
    store.save_run(result_bad)

    # ── Reliability Score ─────────────────────────────────────
    score = store.get_reliability_score("sales_kpi_pipeline", days=30)
    print(f"  📊 30-day Reliability Score: {score}%")
    print("     (Benchmark: +30% reliability improvement target)\n")


if __name__ == "__main__":
    os.makedirs("data", exist_ok=True)
    run_demo()
