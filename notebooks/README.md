# 📓 Databricks Notebooks

> **Note:** These notebooks are in Databricks `.py` format (not `.ipynb`). Import them directly to Databricks Workspace or view the Python code on GitHub.

## 🎯 Medallion Architecture Implementation

Execute notebooks in the following order:

1. **[01_bronze_ingestion.py](01_bronze_ingestion.py)** ✅ - Raw data ingestion into Delta Lake
2. **[02_silver_standardization.py](02_silver_standardization.py)** ✅ - Data standardization and quality
3. **03_gold_analytics.py** 🚧 - Business aggregations and star schema (Coming soon)

---

## 📊 Notebook Details

### 01_bronze_ingestion.py ✅ COMPLETED

**Purpose:** Ingest raw data from multiple sources with ACID guarantees

**Inputs:**
- CSV files: Master_PDV, Master_Products
- Excel files (24): Price_Audit (partitioned by year_month)
- Excel files (2): Sell-In (partitioned by year)

**Outputs:**
- `workspace.default.bronze_master_pdv` (51 rows)
- `workspace.default.bronze_master_products` (201 rows)
- `workspace.default.bronze_price_audit` (1,200+ rows, partitioned)
- `workspace.default.bronze_sell_in` (400+ rows, partitioned)

**Key Features:**
- Unity Catalog managed tables
- Delta Lake with column mapping
- File-by-file Excel processing (low memory)
- Audit columns (ingestion_timestamp, source_file)
- Dynamic partition overwrite
- Metrics from Delta History (no count operations)

**Runtime:** ~2-4 minutes (Serverless optimized)

**Technical Highlights:**
- ✅ Solved DBFS public access issue → Unity Catalog Volumes
- ✅ CSV delimiter detection (semicolon vs comma)
- ✅ Excel reading without spark-excel → pandas + openpyxl
- ✅ Special characters in columns → Column Mapping enabled
- ✅ 3x performance improvement with optimizations

---

### 02_silver_standardization.py ✅ COMPLETED

**Purpose:** Standardize and validate Bronze data with business rules

**Inputs:**
- `workspace.default.bronze_master_pdv`
- `workspace.default.bronze_master_products`
- `workspace.default.bronze_price_audit`
- `workspace.default.bronze_sell_in`

**Outputs:**
- `workspace.default.silver_master_pdv` (deduplicated, standardized)
- `workspace.default.silver_master_products` (price validated)
- `workspace.default.silver_price_audit` (filtered, partitioned by year_month)
- `workspace.default.silver_sell_in` (enriched, partitioned by year)

**Transformations Applied:**
1. **Schema standardization** - snake_case, explicit types
2. **Text normalization** - trim, uppercase
3. **Deduplication** - by business keys (PDV code, Product code)
4. **Domain validations** - prices > 0, no future dates
5. **Derived columns** - unit_price, is_active_sale, year/month
6. **Audit metadata** - processing_date, processing_timestamp

**Architecture Compliance:**
- ✅ Reads ONLY from Bronze Delta tables
- ✅ No cache/persist (Serverless optimized)
- ✅ ONE write action per table
- ✅ No unnecessary count/show operations
- ✅ Validation via Delta History
- ✅ No over-engineering (simple, explicit)

**Runtime:** ~1-2 minutes
- Data type conversions
- Apply business rules
- Data quality checks

**Runtime:** ~15 minutes

---

### 03_gold_analytics.ipynb

**Purpose:** Create business-ready aggregations and dimensional models.

**Inputs:**
- Silver layer Delta tables

**Outputs:**
- Gold layer Delta tables in `data/gold/` directory
- Fact and dimension tables
- Pre-calculated KPIs

**Key Operations:**
- Create star schema
- Build fact tables
- Build dimension tables
- Calculate KPIs
- Optimize for BI queries

**Runtime:** ~20 minutes

---

## Prerequisites

### Databricks Configuration

```python
# Cluster requirements
- Databricks Runtime: 13.3 LTS or higher
- Worker type: Standard_DS3_v2 (or equivalent)
- Min workers: 2
- Max workers: 8
```

### Libraries

All required libraries are listed in `requirements.txt`. Install them in your cluster:

```bash
%pip install -r requirements.txt
```

### Environment Variables

Set the following in your Databricks workspace:

```python
# Path configurations
BRONZE_PATH = "/mnt/bronze/"
SILVER_PATH = "/mnt/silver/"
GOLD_PATH = "/mnt/gold/"
```

---

## Running the Notebooks

### Option 1: Manual Execution

1. Import notebooks to Databricks workspace
2. Attach to appropriate cluster
3. Run cells sequentially
4. Verify outputs in each layer

### Option 2: Job Scheduling

Create a Databricks job with the following tasks:

```json
{
  "tasks": [
    {
      "task_key": "bronze_ingestion",
      "notebook_path": "/notebooks/01_bronze_ingestion"
    },
    {
      "task_key": "silver_transformation",
      "depends_on": [{"task_key": "bronze_ingestion"}],
      "notebook_path": "/notebooks/02_silver_transformation"
    },
    {
      "task_key": "gold_analytics",
      "depends_on": [{"task_key": "silver_transformation"}],
      "notebook_path": "/notebooks/03_gold_analytics"
    }
  ]
}
```

---

## Best Practices

1. **Always test on sample data first** before running on full dataset
2. **Use notebook parameters** for flexible execution
3. **Enable Delta table optimization** regularly
4. **Document assumptions** in markdown cells
5. **Log key metrics** for monitoring

---

## Troubleshooting

### Common Issues

**Issue:** OutOfMemoryError
- **Solution:** Increase cluster size or partition data better

**Issue:** Delta table not found
- **Solution:** Verify paths and ensure previous layer executed successfully

**Issue:** Schema mismatch
- **Solution:** Check schema evolution settings, use mergeSchema option

**Issue:** Slow performance
- **Solution:** Enable caching, optimize partitions, use Z-ordering

---

## Monitoring

Monitor notebook execution through:

1. **Databricks Job Runs:** View history and logs
2. **Delta Table History:** Check versions and operations
3. **Data Quality Reports:** Review validation results
4. **Performance Metrics:** Track runtime trends

---

## Development Guidelines

When modifying notebooks:

1. Create a feature branch
2. Test changes thoroughly
3. Document changes in markdown cells
4. Update this README if needed
5. Create pull request for review

---

## Version Control

Notebooks are version controlled in Git. To sync:

```bash
# Export from Databricks
databricks workspace export /notebooks/01_bronze_ingestion.ipynb ./notebooks/

# Import to Databricks  
databricks workspace import ./notebooks/01_bronze_ingestion.ipynb /notebooks/
```

---

## Contact

For questions or issues with notebooks, contact [Your Email]
