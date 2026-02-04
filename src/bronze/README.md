# Bronze Layer – Market Visibility Project

> **Purpose:** Raw data ingestion, traceability, and technical validation for robust analytics foundations in Retail/FMCG.

---

## 🚀 Quick Facts

- ⏱️ Full Bronze pipeline runs in: **3–4 minutes**
- 🏗️ Strict Medallion Architecture: Bronze → Silver → Gold
- 📦 100% Delta Lake, Databricks Serverless compatible
- 🔍 Automated quality & drift monitoring (results in Delta tables)
- 🔒 No business logic—only technical validation and traceability

---

## 📊 What is the Bronze Layer?

The Bronze layer is the single source of truth for all raw data ingested into the analytics platform. It ensures:

- **Data lineage**: Every record is traceable to its source file and load event
- **Schema enforcement**: Early detection of structural changes
- **Technical quality**: Nulls, duplicates, schema drift, and ingestion errors are logged and monitored
- **Separation of concerns**: No business rules or enrichment—only raw, auditable data

---

## 🗂️ Structure

| Folder           | Purpose                                 |
|------------------|-----------------------------------------|
| master_products/ | Ingestion & validation of product master|
| master_pdv/      | Ingestion & validation of POS master    |
| price_audit/     | Ingestion & validation of price audits  |
| sell_in/         | Ingestion & validation of sell-in data  |
| notebooks/       | Orchestration notebooks (Databricks)    |
| querys/          | Quality & drift SQL checks              |

---

## 🛠️ Pipeline Flow

1. **Ingest**: Read raw files from `/Volumes/workspace/raw_data/` (CSV/XLSX)
2. **Add Metadata**: Source, load timestamp, batch ID
3. **Write Delta**: Store in Bronze schema, partitioned by load date
4. **Validate**: Run technical quality checks (nulls, schema drift, duplicates)
5. **Monitor**: Log results in Delta tables for downstream alerting

---

## 🧩 Key Scripts & Notebooks

- `ingest_*.py`: Ingestion logic per entity
- `validate_*.py`: Technical validation (pre/post write)
- `monitor_*.py`: Metrics, logging, and drift detection
- `create_*.sql`: DDL for Bronze tables
- `notebooks/`: Orchestration for Databricks Asset Bundles

---

## 📥 Handling Excel (.xlsx) Sources

Some source files (e.g., sell_in.xlsx) are provided in Excel format to simulate real-world scenarios where business data often arrives as .xlsx. In this project:

- **Direct Excel Ingestion:** The pipeline reads .xlsx files natively using Databricks' built-in connectors, demonstrating flexibility for enterprise data sources.
- **CSV Conversion (Recommended for Production):**
    - For production, it is best practice to convert .xlsx to .csv before ingestion for performance and compatibility.
    - Example Databricks code to convert Excel to CSV:

    ```python
    # Databricks: Convert Excel to CSV (simulation)
    import pandas as pd
    df = pd.read_excel('/dbfs/Volumes/workspace/raw_data/sell_in/sell_in.xlsx')
    df.to_csv('/dbfs/Volumes/workspace/raw_data/sell_in/sell_in.csv', index=False)
    ```
- **Upload to Unity Catalog:** All raw files (CSV/XLSX) are uploaded directly to Unity Catalog Volumes for secure, governed access.
- **Simulation Scripts:** In `orchestration/generation/` you will find scripts (e.g., `Gen_Import_Price_Audit.py`, `Gen_Sell_In.py`) that automate the upload and simulation of raw file arrival, mirroring real production ingestion workflows.

> This approach demonstrates both flexibility for business scenarios and readiness for production-grade, automated data pipelines.

---

## 📈 Quality & Monitoring


### Data Validation & Monitoring (Technical Quality)

- **Automated Technical Validation:**
    - Every Bronze ingestion triggers pre- and post-write validation scripts (e.g., `validate_price_audit.py`).
    - Checks include: schema conformity, nulls by column, fully empty rows, technical duplicates, and schema drift.
    - No business rules are applied—focus is on data structure, completeness, and technical consistency.

- **Monitoring & Alerting:**
    - Monitoring scripts (e.g., `monitor_price_audit.py`) run after each load, logging metrics such as row counts, null distributions, and drift events.
    - All results are written to dedicated Delta tables (one per entity), enabling downstream alerting and dashboarding.
    - Ingestion is never stopped by technical quality issues; instead, all anomalies are flagged for review, ensuring full data lineage and auditability.

- **Example: Price Audit Monitoring**
    - The monitoring table for price audit tracks: file name, load timestamp, row count, nulls by column, duplicate counts, and schema drift flags.
    - This structure is replicated for all Bronze entities, providing a unified, queryable view of technical data quality across the platform.

> This approach ensures that even if raw data arrives with structural issues, it is never lost—every anomaly is logged, visible, and traceable, supporting robust data governance and transparency for analytics teams.

---

## 📚 Documentation

- [Project Architecture](../../docs/architecture/)
- [Data Dictionary](../../docs/data_dictionary/)
- [Technical Specs](../../docs/technical_specs/)

---

## 📝 Example: Ingestion Flow (Mermaid)

```mermaid
graph TD;
    A[Raw File Arrives] --> B[Ingest Script]
    B --> C[Add Metadata]
    C --> D[Write to Delta (Bronze)]
    D --> E[Run Quality Checks]
    E --> F[Log Results to Delta]
```

---

## 💡 Why This Matters (for Recruiters)

- **Enterprise-grade**: Follows best practices for scalable, auditable analytics
- **Separation of concerns**: Enables robust, modular pipelines
- **Ready for business logic**: Silver/Gold layers build on a solid, validated foundation
- **Portfolio-ready**: Demonstrates real-world BI architecture, not academic prototypes

---

## License

MIT
