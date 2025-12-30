# Databricks Notebooks

## Execution Order

Execute notebooks in the following order:

1. **01_bronze_ingestion.ipynb** - Ingest raw data into Bronze layer
2. **02_silver_transformation.ipynb** - Clean and validate data in Silver layer  
3. **03_gold_analytics.ipynb** - Create business-ready datasets in Gold layer

## Notebook Descriptions

### 01_bronze_ingestion.ipynb

**Purpose:** Ingest raw data from source systems with minimal processing.

**Inputs:**
- Raw data files from `data/raw/` directory
- Supported formats: CSV, JSON, Parquet, Excel

**Outputs:**
- Delta tables in `data/bronze/` directory
- Ingestion metadata and logs

**Key Operations:**
- Read source files
- Add ingestion timestamp
- Write to Delta format
- Handle schema inference

**Runtime:** ~10 minutes

---

### 02_silver_transformation.ipynb

**Purpose:** Clean, validate, and standardize data for analytics.

**Inputs:**
- Bronze layer Delta tables

**Outputs:**
- Cleaned Delta tables in `data/silver/` directory
- Data quality reports
- Validation logs

**Key Operations:**
- Remove duplicates
- Handle null values
- Standardize formats
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
