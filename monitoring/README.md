# 🔍 Monitoring - Schema & Quality Drift Detection

This directory contains **operational monitoring** scripts for detecting schema changes and data quality drift across the Medallion Architecture layers. The monitoring is **completely decoupled** from the data pipeline—pipelines never fail due to monitoring issues.

---

## Why Monitoring Exists

### Business Problem

In enterprise data pipelines, source systems change without notice:
- Upstream teams add/remove columns
- Data types change silently
- Data quality degrades over time
- Volume patterns shift unexpectedly

**Without monitoring:** These changes propagate downstream, breaking dashboards and KPIs silently.

**With monitoring:** Proactive detection allows intervention before business impact.

---

## Architecture: Zero-Coupling Design

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         ZERO-COUPLING PRINCIPLE                                  │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│   DATA PIPELINE (notebooks/)              MONITORING (monitoring/)              │
│   ┌──────────────────────────┐           ┌──────────────────────────┐          │
│   │                          │           │                          │          │
│   │  Bronze → Silver → Gold  │           │  Drift Detection         │          │
│   │                          │           │                          │          │
│   │  Writes to Delta Lake    │           │  Reads Delta History     │          │
│   │  (normal operation)      │           │  (metadata only)         │          │
│   │                          │           │                          │          │
│   └────────────┬─────────────┘           └────────────┬─────────────┘          │
│                │                                      │                         │
│                ▼                                      ▼                         │
│   ┌──────────────────────────────────────────────────────────────────┐         │
│   │                      DELTA LAKE STORAGE                          │         │
│   │                                                                  │         │
│   │  _delta_log/           ◄─────────────── Monitoring reads here   │         │
│   │  • Transaction history                  (zero-compute)          │         │
│   │  • Schema metadata                                               │         │
│   │  • Version snapshots                                             │         │
│   └──────────────────────────────────────────────────────────────────┘         │
│                                                                                  │
│   KEY BENEFITS:                                                                 │
│   ✅ Pipeline never fails due to monitoring issues                             │
│   ✅ Monitoring reads metadata only (free)                                     │
│   ✅ Single source of truth (Delta History)                                    │
│   ✅ No custom logging required in pipelines                                   │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### Why Not Embed Monitoring in Pipelines?

| Approach | Pros | Cons | Decision |
|----------|------|------|----------|
| Monitoring inside notebooks | Single execution | Pipeline fails if monitoring fails, tight coupling | ❌ Rejected |
| **Separate monitoring scripts** | Decoupled, independent failure | Separate scheduling needed | ✅ Selected |

**Trade-off:** Requires separate scheduling but ensures pipeline resilience.

---

## Directory Structure

```
monitoring/
├── drift_monitoring_bronze.py    # Bronze layer schema drift detection
├── silver_drift_monitoring.py    # Silver layer quality + volume drift
└── README.md                     # This file
```

### Why No Gold Drift Monitoring?

**Gold layer validation is implemented differently:**

| Layer | Monitoring Approach | Location |
|-------|---------------------|----------|
| Bronze | Schema drift detection | `monitoring/drift_monitoring_bronze.py` |
| Silver | Quality + volume drift | `monitoring/silver_drift_monitoring.py` |
| Gold | **Post-execution validation** | `notebooks/gold/validation/gold_validation.py` |

**Rationale for Gold:**
- Gold layer has strict contracts (Star Schema)
- Validation is part of the pipeline (not separate monitoring)
- Referential integrity checks run after each Gold execution
- Failures should block downstream (intentional tight coupling for quality)

---

## Bronze Layer Monitoring

### File: `drift_monitoring_bronze.py`

**Purpose:** Detect schema changes in source data before they propagate downstream.

**Schedule:** Daily at 3:05 AM (5 minutes after Bronze ingestion)

**Runtime:** ~2-5 minutes (metadata operations only)

### What It Monitors

| Table | Critical Columns | Alert Behavior |
|-------|------------------|----------------|
| `bronze_price_audit` | PDV_Code, Product_SKU, Audit_Date, Observed_Price | Alert on new OR removed columns |
| `bronze_sell_in` | Year, Product, PDV, Quantity, Amount | Alert on new OR removed columns |
| `bronze_master_pdv` | PDV_Code, PDV_Name | Alert on new columns only (dimension can evolve) |
| `bronze_master_products` | Product_SKU, Product_Name | Alert on new columns only |

### Detection Logic

```python
# Compare current schema vs previous version
current_columns = set(spark.table(table).columns)
previous_columns = get_previous_schema_from_delta_history(table)

new_columns = current_columns - previous_columns
removed_columns = previous_columns - current_columns

# Classify severity
if removed_columns & critical_columns:
    severity = "HIGH"
elif removed_columns:
    severity = "MEDIUM"
else:
    severity = "LOW"
```

### Severity Classification

| Severity | Condition | Action Required |
|----------|-----------|-----------------|
| **HIGH** 🚨 | Critical column removed | URGENT: Verify source change, update Silver validation, notify consumers |
| **MEDIUM** ⚠️ | Non-critical column removed | WARNING: Check Silver dependencies, verify intentional change |
| **LOW** ℹ️ | New columns added | INFO: Document in data dictionary, consider Silver extension |

### Output Table

```sql
-- Check recent alerts
SELECT 
    table_name,
    severity,
    new_columns,
    removed_columns,
    detected_timestamp
FROM workspace.default.bronze_schema_alerts
WHERE detected_timestamp >= current_date() - INTERVAL 7 DAYS
ORDER BY severity DESC, detected_timestamp DESC;
```

---

## Silver Layer Monitoring

### File: `silver_drift_monitoring.py`

**Purpose:** Detect quality degradation and volume anomalies in cleaned data.

**Schedule:** After each Silver write (post-write hook)

**Runtime:** ~1 minute (metadata-only, serverless-friendly)

### What It Monitors

| Drift Type | Detection Method | Example Alert |
|------------|------------------|---------------|
| **Schema Drift** | Column comparison vs baseline | "New column `promotion_flag` detected" |
| **Quality Drift** | Null rate / invalid rate changes | "Null rate increased from 2% to 15%" |
| **Volume Drift** | Row count vs historical average | "Row count dropped 60% vs 7-day average" |

### Severity Classification

| Severity | Condition | Action Required |
|----------|-----------|-----------------|
| **HIGH** 🚨 | Critical column missing, type change, >50% row count drop | URGENT: Review Silver logic, check upstream, notify consumers |
| **MEDIUM** ⚠️ | New column, moderate null/invalid increase, moderate volume change | WARNING: Review data quality, update documentation |
| **LOW** ℹ️ | Minor changes, expected drift | INFO: Monitor, document |

### Output Table

```sql
-- Check drift history
SELECT * FROM workspace.default.silver_drift_history
ORDER BY timestamp DESC
LIMIT 50;
```

---

## How to Run

### Manual Execution (Interactive)

```python
# In Databricks workspace:
# 1. Navigate to monitoring/drift_monitoring_bronze.py
# 2. Click "Run All"
# 3. Review console output for detected drifts
```

### Scheduled Execution (Production)

Add to Databricks Workflow:

```yaml
# .databricks/workflows/monitoring_pipeline.yml
resources:
  jobs:
    monitoring_daily:
      name: "Schema & Quality Monitoring"
      schedule:
        quartz_cron_expression: "0 5 3 * * ?"  # 3:05 AM daily
        timezone_id: "America/Bogota"
      
      tasks:
        - task_key: "bronze_drift"
          notebook_task:
            notebook_path: "monitoring/drift_monitoring_bronze.py"
        
        - task_key: "silver_drift"
          notebook_task:
            notebook_path: "monitoring/silver_drift_monitoring.py"
          depends_on:
            - task_key: "bronze_drift"
```

---

## Configuration

### Adjust Critical Columns

Edit `drift_monitoring_bronze.py`:

```python
BRONZE_TABLES_CONFIG = {
    "bronze_price_audit": {
        "critical_columns": [
            "PDV_Code", 
            "Product_SKU", 
            "Audit_Date", 
            "Observed_Price",
            # Add new critical columns here
        ],
        "alert_on_new": True,
        "alert_on_removed": True
    },
    # ... other tables
}
```

### Adjust Thresholds

Edit `silver_drift_monitoring.py`:

```python
DRIFT_THRESHOLDS = {
    "null_rate_increase": 0.10,      # Alert if null rate increases by 10%
    "volume_drop_pct": 0.50,         # Alert if volume drops by 50%
    "volume_spike_pct": 2.00,        # Alert if volume increases by 200%
}
```

---

## Design Decisions

### Why Delta History Over Custom Audit Tables?

| Approach | Storage | Compute | Maintenance |
|----------|---------|---------|-------------|
| Custom audit tables | Additional storage | Write operations | Manual cleanup |
| **Delta History** | Already exists | Zero (metadata read) | Automatic |

**Decision:** Delta History provides schema snapshots automatically. No need to duplicate.

### Why Zero-Compute Validation?

Uses `DESCRIBE HISTORY` + time travel instead of full table scans:

```python
# Zero-compute approach
history = spark.sql(f"DESCRIBE HISTORY {table}")
schema_at_version = spark.read.option("versionAsOf", version).table(table).schema

# Avoided: Full table scan
# df = spark.table(table).count()  # ❌ Expensive
```

**Benefits:**
- ✅ $0 compute cost (metadata-only)
- ✅ Milliseconds execution
- ✅ Serverless-friendly

### Why Non-Blocking Architecture?

Bronze philosophy: **"Load first, validate later"**

- Data ingestion should never fail due to monitoring
- Drift detection is observability, not validation
- Monitoring failure doesn't stop pipeline
- Clean separation of concerns

---

## Operational Queries

### Tables Needing Attention

```sql
SELECT 
    table_name,
    COUNT(*) as unresolved_alerts,
    MAX(severity) as highest_severity
FROM workspace.default.bronze_schema_alerts
WHERE notified = false
GROUP BY table_name
HAVING COUNT(*) > 0;
```

### Schema Evolution Timeline

```sql
SELECT 
    table_name,
    DATE(detected_timestamp) as date,
    severity,
    new_columns,
    removed_columns
FROM workspace.default.bronze_schema_alerts
ORDER BY detected_timestamp DESC;
```

---

## Related Documentation

- [BRONZE_ARCHITECTURE_DECISIONS.md](../docs/BRONZE_ARCHITECTURE_DECISIONS.md) - Bronze layer design
- [SILVER_ARCHITECTURE_DECISIONS.md](../docs/SILVER_ARCHITECTURE_DECISIONS.md) - Silver layer design
- [GOLD_ARCHITECTURE_DECISIONS.md](../docs/GOLD_ARCHITECTURE_DECISIONS.md) - Gold validation approach
- [notebooks/gold/validation/gold_validation.py](../notebooks/gold/validation/gold_validation.py) - Gold layer validation
