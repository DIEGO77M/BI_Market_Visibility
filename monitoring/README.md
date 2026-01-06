# 🔍 Monitoring

**Zero-coupling drift detection** for schema and quality changes.

---

## Design Principle

```
Pipeline (notebooks/)              Monitoring (this folder)
        │                                   │
        ▼                                   ▼
   Writes Delta  ──────────────────▶  Reads Delta History
                                      (metadata only, $0 compute)
```

**Key:** Pipeline never fails due to monitoring. Completely decoupled.

---

## Files

| Script | Layer | Detects |
|--------|-------|---------|
| `drift_monitoring_bronze.py` | Bronze | Schema changes (new/removed columns) |
| `silver_drift_monitoring.py` | Silver | Quality drift, volume anomalies |

**Gold validation** is in `notebooks/gold/validation/` (part of pipeline).

---

## How It Works

```python
# Zero-compute: reads Delta transaction logs only
history = spark.sql("DESCRIBE HISTORY bronze_price_audit")
current_schema = spark.table(table).schema
# Compare schemas across versions → Generate alerts
```

---

## Severity Levels

| Level | Condition | Example |
|-------|-----------|---------|
| 🚨 HIGH | Critical column removed | `Product_SKU` missing |
| ⚠️ MEDIUM | Non-critical change | New column added |
| ℹ️ LOW | Minor drift | Slight volume change |

---

## Run

```bash
# Manually in Databricks
# Open drift_monitoring_bronze.py → Run All

# Or schedule after ingestion (see .databricks/workflows/)
```

---

See [Architecture Docs](../docs/) for design decisions.
