"""
Generate realistic sample data for pipeline monitor demo.
Inclui anomalias intencionais para demonstrar todos os checks.
Run: python generate_sample_data.py
"""
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta

rng = np.random.default_rng(42)
Path("sample_data").mkdir(exist_ok=True)

# ── 1. Commercial KPIs ─────────────────────────────────────────
n = 5000
dates = [datetime.today() - timedelta(hours=int(h)) for h in rng.integers(1, 48, n)]

df = pd.DataFrame({
    "order_id":          [f"ORD-{i:06d}" for i in range(n)],
    "order_date":        dates,
    "updated_at":        dates,
    "customer_id":       rng.integers(1000, 9999, n),
    "channel":           rng.choice(["digital","telesales","field","partner"], n),
    "secondary_channel": [None if rng.random() < 0.4 else "referral" for _ in range(n)],
    "notes":             [None if rng.random() < 0.6 else "ok" for _ in range(n)],
    "revenue":           rng.normal(4500, 1200, n).clip(0),
    "units_sold":        rng.integers(1, 200, n),
    "discount_pct":      rng.uniform(0, 0.30, n),
    "margin":            rng.normal(0.35, 0.08, n).clip(0.05, 0.75),
    "region":            rng.choice(["Norte","Nordeste","Centro-Oeste","Sudeste","Sul"], n),
})

# Inject: ~8% nulls on revenue  → CRITICAL (threshold 20% → adjust to warn at 5%)
null_idx = rng.choice(n, size=int(n * 0.08), replace=False)
df.loc[null_idx, "revenue"] = None

# Inject: ~3% duplicate order_ids → WARNING
dup_idx = rng.choice(n, size=int(n * 0.03), replace=False)
df.loc[dup_idx, "order_id"] = df.loc[dup_idx - 1, "order_id"].values

df.to_csv("sample_data/commercial_kpis.csv", index=False)
null_rate = df["revenue"].isna().mean()
dup_rate  = df.duplicated(subset=["order_id"]).mean()
print(f"✔ commercial_kpis.csv — {len(df):,} rows | revenue nulls: {null_rate:.1%} | order_id dups: {dup_rate:.1%}")

# ── 2. Inventory / Stock ───────────────────────────────────────
n = 8000
warehouses = [f"WH-{i:03d}" for i in range(1, 21)]
skus = [f"SKU-{i:05d}" for i in range(1, 501)]
snap_days  = [int(d) for d in rng.integers(0, 3, n)]
snap_dates = [datetime.today() - timedelta(days=d) for d in snap_days]

inv = pd.DataFrame({
    "sku_id":          rng.choice(skus, n),
    "warehouse_id":    rng.choice(warehouses, n),
    "snapshot_date":   snap_dates,
    "lot_number":      [None if rng.random() < 0.3 else f"LOT-{int(rng.integers(1000,9999))}" for _ in range(n)],
    "supplier_notes":  [None if rng.random() < 0.7 else "ok" for _ in range(n)],
    "qty_on_hand":     rng.integers(0, 5000, n),
    "qty_reserved":    rng.integers(0, 500, n),
    "unit_cost":       rng.uniform(5, 800, n).round(2),
    "lead_time_days":  rng.integers(1, 90, n),
    "reorder_point":   rng.integers(50, 500, n),
    "turnover_rate":   rng.normal(4.5, 1.2, n).clip(0.1),
})

# Inject: 4% nulls on qty_on_hand → WARNING (threshold 2%)
null_inv = rng.choice(n, size=int(n * 0.04), replace=False)
inv.loc[null_inv, "qty_on_hand"] = None

inv.to_csv("sample_data/inventory_stock.csv", index=False)
print(f"✔ inventory_stock.csv   — {len(inv):,} rows | qty_on_hand nulls: {inv['qty_on_hand'].isna().mean():.1%}")

# ── 3. Strategic Indicators ────────────────────────────────────
n = 500
periods = [datetime.today().replace(day=1) - timedelta(days=30 * i) for i in range(12)]
loaded_hrs = [int(h) for h in rng.integers(1, 8, n)]

kpis = pd.DataFrame({
    "indicator_id":      [f"KPI-{i:04d}" for i in range(n)],
    "indicator_name":    rng.choice(["Net Revenue","Gross Margin","NPS Score","OTIF Rate","EBITDA %"], n),
    "reference_period":  rng.choice(periods, n),
    "loaded_at":         [datetime.today() - timedelta(hours=h) for h in loaded_hrs],
    "business_unit":     rng.choice(["Commercial","Operations","Finance","Logistics"], n),
    "actual_value":      rng.normal(100, 25, n).clip(0),
    "target_value":      rng.normal(110, 20, n).clip(0),
    "variance_pct":      rng.normal(0, 0.08, n),
    "ytd_actual":        rng.normal(950, 120, n).clip(0),
    "ytd_target":        rng.normal(1000, 100, n).clip(0),
    "confidence_score":  rng.uniform(0.70, 1.0, n).round(3),
})

# Inject: 6% nulls on actual_value → CRITICAL (exec pipeline, threshold 5%)
null_kpi = rng.choice(n, size=int(n * 0.06), replace=False)
kpis.loc[null_kpi, "actual_value"] = None

kpis.to_csv("sample_data/strategic_indicators.csv", index=False)
print(f"✔ strategic_indicators.csv — {len(kpis):,} rows | actual_value nulls: {kpis['actual_value'].isna().mean():.1%}")
print("\nAll sample data generated in sample_data/")
