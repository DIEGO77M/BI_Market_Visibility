# Bronze Layer - Schema Drift Monitoring Guide

**Author:** Diego Mayorga  
**Date:** December 31, 2025  
**Project:** BI Market Visibility  
**Layer:** Bronze (Raw Data Ingestion)

---

## Overview

Schema drift monitoring for Bronze layer detects structural changes in source data without blocking ingestion pipeline. This decoupled approach ensures operational resilience while providing proactive visibility into schema evolution.

---

## Architecture

```
┌─────────────────────┐
│  Source Systems     │
│  (CSV/Excel files)  │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐      
│ Bronze Ingestion    │──────▶ NO schema logging code
│ (3:00 AM daily)     │        (clean, focused on ingestion)
└──────────┬──────────┘      
           │
           │ SUCCESS
           ▼
┌─────────────────────┐
│ Delta Lake Storage  │
│ (automatic tracking)│
│ - Transaction logs  │
│ - Schema metadata   │
└──────────┬──────────┘
           │
           │ Reads Delta History (zero-compute)
           ▼
┌─────────────────────┐      ┌──────────────────────┐
│ Drift Monitoring    │─────▶│ bronze_schema_alerts │
│ (3:05 AM daily)     │      │ (only table created) │
└─────────────────────┘      └──────────────────────┘
```

---

## Tables Created

### `bronze_schema_alerts`
The ONLY table created by monitoring. Logs detected schema drifts with severity classification.

| Column | Type | Description |
|--------|------|-------------|
| `alert_id` | STRING | Unique alert identifier |
| `table_name` | STRING | Affected table |
| `batch_id` | STRING | Batch that triggered drift |
| `new_columns` | ARRAY<STRING> | Columns added |
| `removed_columns` | ARRAY<STRING> | Columns removed |
| `detected_timestamp` | TIMESTAMP | When drift was detected |
| `alert_type` | STRING | NEW_COLUMNS / REMOVED_COLUMNS |
| `severity` | STRING | HIGH / MEDIUM / LOW |
| `critical_columns_affected` | BOOLEAN | Whether critical columns removed |
| `action_required` | STRING | Recommended next steps |
| `notified` | BOOLEAN | Whether alert was sent (future) |
| `layer` | STRING | "bronze" |

**Usage:** Operational dashboard queries this for trend analysis and incident tracking.

---

## Severity Classification
version_current` | BIGINT | Current Delta version |
| `version_previous` | BIGINT | Previous Delta version compared |
| `new_columns` | ARRAY<STRING> | Columns added |
| `removed_columns` | ARRAY<STRING> | Columns removed |
| `detected_timestamp` | TIMESTAMP | When drift was detected |
| `alert_type` | STRING | NEW_COLUMNS / REMOVED_COLUMNS |
| `severity` | STRING | HIGH / MEDIUM / LOW |
| `critical_columns_affected` | BOOLEAN | Whether critical columns removed |
| `action_required` | STRING | Recommended next steps |
| `layer` | STRING | "bronze" |
| `notified` | BOOLEAN | Whether alert was sent (future) |

**Usage:** Operational dashboard queries this for trend analysis and incident tracking.yaml
Job Name: Bronze_Pipeline_Daily
Schedule: Daily at 3:00 AM (America/Bogota)

Tasks:
  1. bronze_ingestion (3:00 AM)
     - Ingests data + logs schema snapshots
     - Timeout: 60 minutes
  
  2. drift_monitoring_bronze (3:05 AM)
     - Depends on: bronze_ingestion SUCCESS
     - Compares schemas, generates alerts
     - Timeout: 10 minutes
```

---

## Key Design Decisions

### 1. Why Decoupled Monitoring?
Zero-Coupling Architecture?

**Option A (Implemented): Delta History as Source**
- ✅ Bronze doesn't know monitoring exists
- ✅ Delta History is single source of truth
- ✅ No custom logging code in Bronze
- ✅ Self-healing (monitoring can backfill from logs)

**Option B (Rejected): Bronze logs to audit table**
- ❌ Adds code to Bronze that isn't core responsibility
- ❌ Tight coupling between pipeline and monitoring
- ❌ Duplicates data Delta already has
- ❌ If Bronze logging fails, monitoring breaks

---

### 2. Why Delta History Instead of Custom Tables?

**What Delta History Provides Natively:**
- Schema snapshots (via `DESCRIBE TABLE` at specific version)
- Transaction metadata (timestamp, user, operation)
- Zero-compute access (metadata-only queries)
- Automatic retention (Delta transaction log lifecycle)

**No `bronze_schema_audit` table needed!** Delta transaction logs already store this.

**Monitoring reads directly:**
```python
# Current schema
spark.sql("DESCRIBE TABLE bronze_master_pdv")

# Previous schema
spark.read.format("delta").option("versionAsOf", prev_version).table("bronze_master_pdv")
```

---

### 3. Why 
**Problem:** If drift detection ran INSIDE Bronze ingestion:
- ❌ Schema issues could block data loading
- ❌ Violates "load-first" Bronze philosophy
- ❌ Increases ingestion latency

**Solution:** Separate job running AFTER ingestion:
- ✅ Data always lands (non-blocking)
- ✅ Drift detection failure doesn't stop pipeline
- ✅ Clean separation of concerns

---

### 4. Why 5-Minute Delay?

Allows Bronze ingestion to complete and commit metadata before monitoring reads Delta History. Prevents race conditions.

---

### 5. Why Not Real-Time Streaming Detection?

- Bronze is batch-oriented (daily file drops)
- Schema changes are infrequent (weeks/months between)
- Daily monitoring provides sufficient SLA
- Reduces operational complexity

---

## Operational Queries

### Check Recent Drifts

```sql
SELECT 
    table_name,
    severity,
    new_columns,
    removed_columns,
    detected_timestamp
FROM workspace.default.bronze_schema_alerts
WHERE detected_timestamp >= current_date() - INTERVAL 7 DAYS
  AND layer = 'bronze'
ORDER BY severity DESC, detected_timestamp DESC;
```

### Delta History (Source of Truth)

```sql
-- View schema evolution timeline
DESCRIBE HISTORY workspace.default.bronze_master_pdv;

-- Get schema at specific version
DESCRIBE TABLE workspace.default.bronze_master_pdv VERSION AS OF 5;
```

### Tables Needing Attention

```sql
SELECT 
    table_name,
    COUNT(*) as unresolved_alerts,
    MAX(severity) as highest_severity
FROM workspace.default.bronze_schema_alerts
WHERE notified = false
  AND layer = 'bronze'
GROUP BY table_name
HAVING COUNT(*) > 0
ORDER BY highest_severity DESC;
```

---

## Future Enhancements

### Phase 2: Notification Integration

```python
# Slack webhook (stored in Databricks Secret Scope)
if severity == "HIGH":
    webhook_url = dbutils.secrets.get("monitoring", "slack_webhook")
    requests.post(webhook_url, json={
        "text": f"🚨 Critical schema drift in {table_name}",
        "blocks": [...]
    })
```

### Phase 3: Power BI Dashboard

- Drift frequency trends
- Tables by stability score
- Alert resolution time metrics
- Schema evolution heatmap

---

## Troubleshooting

### Issue: No drift detected when expected

**Symptom:** Schema changed but monitoring shows "No drift detected"

**Solution:**
1. Verify Bronze ingestion completed successfully (creates new Delta version)
2. Check Delta History: `DESCRIBE HISTORY workspace.default.bronze_master_pdv`
3. Ensure previous version exists (drift needs 2+ versions to compare)
4. Confirm business columns changed (metadata/partition columns are excluded)

---

### Issue: Schema extraction failed

**Symptom:** "Error extracting schema" in monitoring logs

**Solutiotable exists: `SHOW TABLES IN workspace.default LIKE 'bronze_*'`
2. Verify permissions: user must have `SELECT` on Bronze tables
3. Check Unity Catalog connectivity
3. Review `_metadata_` column filtering (shouldn't be included in business columns)

---

## Best Practices

1. **Review Alerts Weekly:** Even LOW seveCRITICAL_COLUMNS` config as business evolves
3. **Archive Old Alerts:** Retention policy (e.g., 1 year) for compliance
4. **Coordinate with Source Owners:** Proactive communication reduces surprises
5. **Test Monitoring Independently:** Don't rely on Bronze runs to validate monitoring

---

## Testing Monitoring

### Simulate Schema Change (Safe Test)

```python
# In a test notebook, create a test table with evolving schema

# Version 1
df_v1 = spark.createDataFrame([
    ("A001", "Product A", 100)
], ["Product_SKU", "Product_Name", "Stock"])

df_v1.write.format("delta").mode("overwrite").saveAsTable("workspace.default.test_drift_table")

# Version 2: Add new column (LOW severity expected)
df_v2 = spark.createDataFrame([
    ("A001", "Product A", 100, "Category_A")
], ["Product_SKU", "Product_Name", "Stock", "Category"])

df_v2.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("workspace.default.test_drift_table")

# Run monitoring on test_drift_table
# Expected: LOW severity alert for new "Category" column
```
4. **Coordinate with Source Owners:** Proactive communication reduces surprises

---

**Related Documentation:**
- [Bronze Architecture Decisions](../BRONZE_ARCHITECTURE_DECISIONS.md)
- [Bronze Ingestion Notebook](../../notebooks/01_bronze_ingestion.py)
- [Monitoring Notebook](../../monitoring/drift_monitoring_bronze.py)

**Author:** Diego Mayorga  
**Last Updated:** 2025-12-31
