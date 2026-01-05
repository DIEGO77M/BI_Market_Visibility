# 🔍 Monitoring - Schema & Quality Drift Detection

**Purpose:** Operational monitoring for schema evolution across Bronze, Silver, and Gold layers of the Medallion Architecture.

---


## 📁 Structure

```
monitoring/
├── drift_monitoring_bronze.py    # Bronze layer schema drift detection
├── silver_drift_monitoring.py    # Silver layer drift monitoring (schema, quality, volume)
├── drift_monitoring_gold.py      # [Future] Gold layer monitoring
└── README.md                     # This file
```

**Notebooks:**
- [../notebooks/silver_drift_monitoring.ipynb](../notebooks/silver_drift_monitoring.ipynb) (Databricks-ready, executable)

---

## 🎯 What is Schema Drift Monitoring?

Schema drift occurs when the structure of source data changes unexpectedly:
- ✅ New columns added (e.g., `"Promotion_Flag"` appears in Price_Audit)
- ✅ Columns removed (e.g., `"Auditor_Notes"` no longer in source)
- ✅ Columns renamed (e.g., `"Observed_Price"` → `"Current_Price"`)

**This monitoring detects these changes proactively without blocking data ingestion.**

---

## 🏗️ Architecture

### Zero-Coupling Design (Opción A)

```
┌─────────────────────────────────────────────────────────────────┐
│                      DATA PIPELINE (notebooks/)                 │
│                                                                 │
│  Bronze Ingestion → Silver Standardization → Gold Analytics    │
│         ↓                    ↓                      ↓           │
│    Writes Delta         Writes Delta          Writes Delta     │
│         ↓                    ↓                      ↓           │
└─────────┬───────────────────────────────────────────────────────┘
          │
          │ Delta History (_delta_log/) automatically tracks schemas
          ↓
┌─────────────────────────────────────────────────────────────────┐
│                    DELTA LAKE (Storage)                         │
│  - Transaction logs with schema metadata (automatic)            │
│  - DESCRIBE HISTORY provides version history                   │
│  - No custom audit tables needed                               │
└─────────┬───────────────────────────────────────────────────────┘
          │
          │ Reads Delta History (zero-compute, read-only)
          ↓
┌─────────────────────────────────────────────────────────────────┐
│                  MONITORING (monitoring/)                       │
│                                                                 │
│  Drift Detection → Compare Delta Versions → Generate Alerts    │
│        ↓                                                        │
│   Delta Tables (bronze_schema_alerts, silver_drift_history)    │
└─────────────────────────────────────────────────────────────────┘
```

**Key Principle:** 
- ✅ **Zero Coupling:** Pipelines don't know monitoring exists
- ✅ **Single Source of Truth:** Delta History is authoritative
- ✅ **No Custom Logging:** Delta automatically tracks schema changes

---


## 📊 Silver Layer Monitoring

### Script: `silver_drift_monitoring.py`

**Schedule:** After each Silver write (post-write hook, see Silver notebook)
**Runtime:** ~1 minute (metadata-only, serverless-friendly)
**Cluster:** Serverless (same as Silver pipeline)

### What It Monitors

| Table | Drift Types | Alert Behavior |
|-------|------------|----------------|
| All Silver tables | Schema drift (new/missing/type changes), Quality drift (null/invalid rates), Volume drift (row count, key cardinality) | Alert on HIGH/MEDIUM/LOW severity, log in audit table |

### Severity Classification

| Severity | Condition | Action Required |
|----------|-----------|-----------------|
| **HIGH** 🚨 | Critical column missing, type change, >50% row count drop | URGENT: Review Silver logic, check upstream, notify consumers |
| **MEDIUM** ⚠️ | New column, moderate null/invalid increase, moderate volume change | WARNING: Review data quality, update documentation |
| **LOW** ℹ️ | Minor changes, expected drift | INFO: Monitor, document |

### Output Table

**`silver_drift_history`** - Drift events with severity and details
```sql
SELECT * FROM workspace.default.silver_drift_history
ORDER BY timestamp DESC;
```

---

## 📊 Bronze Layer Monitoring

### Notebook: `drift_monitoring_bronze.py`

**Schedule:** Daily at 3:05 AM (5 minutes after Bronze ingestion)  
**Runtime:** ~2-5 minutes (metadata operations only)  
**Cluster:** Serverless (same as Bronze ingestion)

### What It Monitors

| Table | Critical Columns | Alert Behavior |
|-------|------------------|----------------|
| `bronze_price_audit` | PDV_Code, Product_SKU, Audit_Date, Observed_Price | Alert on new OR removed columns |
| `bronze_sell_in` | Year, Product, PDV, Quantity, Amount | Alert on new OR removed columns |
| `bronze_master_pdv` | PDV_Code, PDV_Name | Alert on new columns only (dimension can evolve) |
| `bronze_master_products` | Product_SKU, Product_Name | Alert on new columns only |

### Severity Classification

| Severity | Condition | Action Required |
|----------|-----------|-----------------|
| **HIGH** 🚨 | Critical column removed | URGENT: Verify source change, update Silver validation, notify consumers |
| **MEDIUM** ⚠️ | Non-critical column removed | WARNING: Check Silver dependencies, verify intentional change |
| **LOW** ℹ️ | New columns added | INFO: Document in data dictionary, consider Silver extension |

### Output Tables

**`bronze_schema_alerts`** - Drift alerts with severity (only table created by monitoring)
```sql
SELECT * FROM workspace.default.bronze_schema_alerts
WHERE layer = 'bronze' AND severity IN ('HIGH', 'MEDIUM')
ORDER BY detected_timestamp DESC;
```

---

## 🚀 How to Use

### Run Manually (Interactive)

1. Open Databricks workspace
2. Navigate to `/monitoring/drift_monitoring_bronze.py`
3. Click "Run All"
4. Review console output for detected drifts

### Run as Scheduled Job

**Databricks Workflow Configuration:**

```yaml
Job Name: Bronze_Pipeline_Daily

Tasks:
  1. bronze_ingestion
     Notebook: notebooks/01_bronze_ingestion.py
     Schedule: 3:00 AM daily
     
  2. drift_monitoring_bronze
     Notebook: monitoring/drift_monitoring_bronze.py
     Schedule: Depends on task 1 SUCCESS
     Timeout: 10 minutes
```

**Deploy via Databricks CLI:**
```bash
databricks jobs create --json-file .databricks/workflows/bronze_pipeline.json
```

---

## 📈 Operational Queries

### Check Recent Alerts

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

### Tables Needing Attention

```sql
SELECT 
    table_name,
    COUNT(*) as unresolved_alerts,
    MAX(severity) as highest_severity
FROM workspace.default.bronze_schema_alerts
WHERE notified = false AND layer = 'bronze'
GROUP BY table_name
HAVING COUNT(*) > 0;
```

### Schema Evolution Timeline

```sql
SELECT 
    table_name,
    DATE(snapshot_timestamp) as date,
    column_count,
    COUNT(*) as snapshots_per_day
FROM workspace.default.bronze_schema_audit
WHERE layer = 'bronze'
GROUP BY table_name, DATE(snapshot_timestamp)
ORDER BY date DESC;
```

---

## 🔧 Configuration

### Adjust Lookback Period

Edit `drift_monitoring_bronze.py`:
```python
LOOKBACK_HOURS = 24  # Change to 48, 72, etc.
```

### Modify Critical Columns

Edit `BRONZE_TABLES_CONFIG` dictionary:
```python
"bronze_price_audit": {
    "critical_columns": ["PDV_Code", "Product_SKU", "Audit_Date", "Observed_Price", "NEW_CRITICAL_COL"],
    ...
}
```

### Enable Notifications (Future)

```python
# In drift_monitoring_bronze.py, uncomment:
if severity == "HIGH":
    send_slack_alert(message)
    send_email_alert(recipients)
```

---

## 🎯 Design Decisions

### Why Separate from Data Pipeline?

**Problem if monitoring was inside Bronze notebook:**
- ❌ Schema issues could block data loading
- ❌ Violates "load-first" Bronze philosophy
- ❌ Increases ingestion latency

**SolutiOption A (Zero-Coupling)?

**Rejected Option B (Bronze logs snapshots):**
- ❌ Adds code to Bronze that isn't core responsibility
- ❌ Creates tight coupling between pipeline and monitoring
- ❌ Duplicates data Delta already has
- ❌ If Bronze logging fails, monitoring breaks

**Option A Benefits:**
- ✅ **Zero Coupling:** Bronze doesn't know monitoring exists
- ✅ **Single Source of Truth:** Delta History is authoritative
- ✅ **No Extra Code:** Bronze stays clean (ingestion only)
- ✅ **Self-Contained:** Monitoring reads Delta metadata directly

### Why Delta History Over Custom Tables?

Delta History already provides:
- Schema snapshots (via DESCRIBE TABLE + version)
- Transaction metadata (timestamp, user, operation)
- Zero-compute access (metadata-only)
- Automatic retention (transaction log lifecycle)

**No need for `bronze_schema_audit` table** - Delta does this natively!

### Why Zero-Compute Validation?

Uses `DESCRIBE HISTORY` + time travel instead of full table scans:
- ✅ **Cost:** $0 (metadata-only operation)
- ✅ **Speed:** Milliseconds vs seconds
- ✅ **Serverless-friendly:** No data scanning

### Why Non-Blocking Architecture?

Bronze philosophy: **"Load first, validate later"**
- Data ingestion should never fail due to monitoring
- Drift detection is observability, not validation
- Monitoring failure doesn't stop pipeline
- Clean separation of concernsft_monitoring_bronze_guide.md)
- [Bronze Ingestion Notebook](../notebooks/01_bronze_ingestion.py)

---

## 🔮 Future Enhancements


### Phase 2: Silver & Gold Monitoring

```
monitoring/
├── drift_monitoring_bronze.py    ✅ Implemented
├── silver_drift_monitoring.py    ✅ Implemented
└── drift_monitoring_gold.py      🔜 Planned
```

### Phase 3: Notification Integration

- Slack webhooks for HIGH severity
- Email alerts for critical column removal
- PagerDuty integration for production incidents

### Phase 4: Dashboard

Power BI/SQL dashboard showing:
- Drift frequency trends
- Tables by stability score
- Alert resolution time metrics
- Schema evolution heatmap

---

## 🤝 Contributing

**Adding New Tables to Monitor:**

1. Edit `BRONZE_TABLES_CONFIG` in `drift_monitoring_bronze.py`
2. Define critical columns
3. Set alert preferences
4. Test with manual run


**Adding New Monitoring Layers:**

1. Copy `drift_monitoring_bronze.py` → `silver_drift_monitoring.py` (ya implementado)
2. Actualiza referencias de tablas y lógica de drift según la capa
3. Ajusta reglas de severidad y métricas de calidad
4. Añade llamada post-write en el notebook correspondiente

---

**Author:** Diego Mayorga | diego.mayorgacapera@gmail.com  
**Last Updated:** 2025-12-31  
**Repository:** [github.com/DIEGO77M/BI_Market_Visibility](https://github.com/DIEGO77M/BI_Market_Visibility)
