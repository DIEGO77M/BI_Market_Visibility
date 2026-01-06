# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Orchestrator & Configuration
# MAGIC 
# MAGIC **Purpose:** Central configuration and orchestration for the modular Gold Layer architecture. This notebook does NOT process data directly - it defines shared configuration and provides execution options for independent Gold jobs.
# MAGIC 
# MAGIC **Author:** Diego Mayorga  
# MAGIC **Date:** 2026-01-06  
# MAGIC **Project:** BI Market Visibility Analysis
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ## Pipeline Execution
# MAGIC 
# MAGIC **To run the complete Gold Layer pipeline:**
# MAGIC 1. Set `RUN_PIPELINE = True` in the Auto-Execution section, OR
# MAGIC 2. Use the widget at the top: select `yes` and run all cells
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ## Architectural Philosophy
# MAGIC 
# MAGIC The Gold Layer is designed as a **decoupled, anti-saturation architecture** optimized for:
# MAGIC - Databricks Serverless stability
# MAGIC - Daily operational reliability
# MAGIC - Independent failure recovery
# MAGIC - Power BI / Streamlit direct consumption
# MAGIC 
# MAGIC ### Design Principles
# MAGIC 
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────────────────────┐
# MAGIC │                        GOLD LAYER ARCHITECTURE                               │
# MAGIC │                     (Decoupled Anti-Saturation Design)                       │
# MAGIC ├─────────────────────────────────────────────────────────────────────────────┤
# MAGIC │                                                                              │
# MAGIC │  ┌─────────────┐   ┌─────────────┐   ┌─────────────┐                        │
# MAGIC │  │  DIM_DATE   │   │ DIM_PRODUCT │   │   DIM_PDV   │    ← INDEPENDENT JOBS  │
# MAGIC │  │   (static)  │   │  (SCD-2)    │   │   (SCD-2)   │                        │
# MAGIC │  └──────┬──────┘   └──────┬──────┘   └──────┬──────┘                        │
# MAGIC │         │                 │                 │                               │
# MAGIC │  ═══════╪═════════════════╪═════════════════╪════════════════════           │
# MAGIC │         │                 │                 │                               │
# MAGIC │  ┌──────▼─────────────────▼─────────────────▼──────┐                        │
# MAGIC │  │              FACT TABLES (Append-Only)           │   ← INDEPENDENT JOBS  │
# MAGIC │  │  ┌───────────────┐ ┌───────────────┐ ┌─────────┐│                        │
# MAGIC │  │  │FACT_SELL_IN   │ │FACT_PRICE_AUDIT│ │FACT_STOCK│                       │
# MAGIC │  │  │ date×prod×pdv │ │ date×prod×pdv │ │ date×p×p││                        │
# MAGIC │  │  └───────────────┘ └───────────────┘ └─────────┘│                        │
# MAGIC │  └──────────────────────┬──────────────────────────┘                        │
# MAGIC │                         │                                                    │
# MAGIC │  ═══════════════════════╪═══════════════════════════════════════            │
# MAGIC │                         │                                                    │
# MAGIC │  ┌──────────────────────▼──────────────────────────┐                        │
# MAGIC │  │            KPI TABLES (Denormalized)             │   ← INDEPENDENT JOBS  │
# MAGIC │  │  ┌───────────────────┐ ┌───────────────────┐    │                        │
# MAGIC │  │  │KPI_MARKET_VISIBILITY│ │ KPI_MARKET_SHARE │    │                        │
# MAGIC │  │  │   (daily ops)     │ │  (weekly trends) │    │                        │
# MAGIC │  │  └───────────────────┘ └───────────────────┘    │                        │
# MAGIC │  └─────────────────────────────────────────────────┘                        │
# MAGIC │                                                                              │
# MAGIC │  ═══════════════════════════════════════════════════════════════            │
# MAGIC │                                                                              │
# MAGIC │  ┌─────────────────────────────────────────────────┐                        │
# MAGIC │  │          VALIDATION JOB (Read-Only)              │   ← POST-EXECUTION    │
# MAGIC │  │       gold_validation.py - Data Quality          │                        │
# MAGIC │  └─────────────────────────────────────────────────┘                        │
# MAGIC │                                                                              │
# MAGIC └─────────────────────────────────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC 
# MAGIC ### Anti-Saturation Guarantees
# MAGIC 
# MAGIC | Risk | Mitigation |
# MAGIC |------|------------|
# MAGIC | Monolithic notebooks | Each table = independent notebook |
# MAGIC | Heavy window functions | Pre-aggregated KPIs, no runtime computation |
# MAGIC | Full refresh | Incremental by partition where possible |
# MAGIC | Multiple writes | One write per notebook |
# MAGIC | Runtime KPI calculation | Pre-materialized in KPI tables |
# MAGIC | Cascading failures | Jobs fail independently |
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ## Job Execution Strategy
# MAGIC 
# MAGIC **Recommended Databricks Workflow:**
# MAGIC 
# MAGIC ```yaml
# MAGIC Daily_Gold_Pipeline:
# MAGIC   # Phase 1: Dimensions (can run in parallel)
# MAGIC   - name: gold_dim_date
# MAGIC     depends_on: []
# MAGIC   - name: gold_dim_product  
# MAGIC     depends_on: []
# MAGIC   - name: gold_dim_pdv
# MAGIC     depends_on: []
# MAGIC     
# MAGIC   # Phase 2: Facts (depends on dimensions)
# MAGIC   - name: gold_fact_sell_in
# MAGIC     depends_on: [gold_dim_date, gold_dim_product, gold_dim_pdv]
# MAGIC   - name: gold_fact_price_audit
# MAGIC     depends_on: [gold_dim_date, gold_dim_product, gold_dim_pdv]
# MAGIC   - name: gold_fact_stock
# MAGIC     depends_on: [gold_fact_sell_in]  # Derived from sell-in
# MAGIC     
# MAGIC   # Phase 3: KPIs (depends on facts)
# MAGIC   - name: gold_kpi_market_visibility
# MAGIC     depends_on: [gold_fact_sell_in, gold_fact_price_audit, gold_fact_stock]
# MAGIC   - name: gold_kpi_market_share
# MAGIC     depends_on: [gold_kpi_market_visibility]
# MAGIC     
# MAGIC   # Phase 4: Validation (read-only, post-execution)
# MAGIC   - name: gold_validation
# MAGIC     depends_on: [gold_kpi_market_share]
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Widget Configuration & Imports

# COMMAND ----------

# Databricks Widgets for interactive execution control
try:
    dbutils.widgets.dropdown("run_pipeline", "no", ["yes", "no"], "Execute Pipeline")
    dbutils.widgets.dropdown("stop_on_error", "yes", ["yes", "no"], "Stop on Error")
    dbutils.widgets.dropdown("run_validation", "yes", ["yes", "no"], "Run Validation")
    dbutils.widgets.text("timeout_seconds", "3600", "Timeout (seconds)")
except:
    # Fallback for local/test environments without dbutils
    pass

# Get widget values (with fallbacks for local testing)
def get_widget(name, default):
    try:
        return dbutils.widgets.get(name)
    except:
        return default

WIDGET_RUN_PIPELINE = get_widget("run_pipeline", "no") == "yes"
WIDGET_STOP_ON_ERROR = get_widget("stop_on_error", "yes") == "yes"
WIDGET_RUN_VALIDATION = get_widget("run_validation", "yes") == "yes"
WIDGET_TIMEOUT = int(get_widget("timeout_seconds", "3600"))

print(f"🔧 Widget Configuration:")
print(f"   - run_pipeline: {WIDGET_RUN_PIPELINE}")
print(f"   - stop_on_error: {WIDGET_STOP_ON_ERROR}")
print(f"   - run_validation: {WIDGET_RUN_VALIDATION}")
print(f"   - timeout_seconds: {WIDGET_TIMEOUT}")

# COMMAND ----------

from pyspark.sql.functions import (
    col, lit, when, coalesce, current_date, current_timestamp,
    year, month, quarter, weekofyear, dayofweek, dayofmonth,
    date_format, to_date, datediff, add_months, last_day,
    sum as spark_sum, avg as spark_avg, count as spark_count,
    max as spark_max, min as spark_min, first, last,
    row_number, rank, dense_rank, lead, lag,
    round as spark_round, abs as spark_abs,
    concat, concat_ws, sha2, md5,
    expr, broadcast
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StringType, IntegerType, LongType, DoubleType, 
    DecimalType, DateType, TimestampType, BooleanType
)
from datetime import datetime, timedelta

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Global Configuration

# COMMAND ----------

# =============================================================================
# UNITY CATALOG CONFIGURATION
# =============================================================================

CATALOG = "workspace"
SCHEMA = "default"

# -----------------------------------------------------------------------------
# SILVER LAYER (INPUT - READ ONLY)
# -----------------------------------------------------------------------------
SILVER_TABLES = {
    "master_pdv": f"{CATALOG}.{SCHEMA}.silver_master_pdv",
    "master_products": f"{CATALOG}.{SCHEMA}.silver_master_products",
    "price_audit": f"{CATALOG}.{SCHEMA}.silver_price_audit",
    "sell_in": f"{CATALOG}.{SCHEMA}.silver_sell_in"
}

# -----------------------------------------------------------------------------
# GOLD LAYER (OUTPUT - WRITE)
# -----------------------------------------------------------------------------
GOLD_TABLES = {
    # Dimensions
    "dim_date": f"{CATALOG}.{SCHEMA}.gold_dim_date",
    "dim_product": f"{CATALOG}.{SCHEMA}.gold_dim_product",
    "dim_pdv": f"{CATALOG}.{SCHEMA}.gold_dim_pdv",
    
    # Facts
    "fact_sell_in": f"{CATALOG}.{SCHEMA}.gold_fact_sell_in",
    "fact_price_audit": f"{CATALOG}.{SCHEMA}.gold_fact_price_audit",
    "fact_stock": f"{CATALOG}.{SCHEMA}.gold_fact_stock",
    
    # KPIs
    "kpi_market_visibility": f"{CATALOG}.{SCHEMA}.gold_kpi_market_visibility_daily",
    "kpi_market_share": f"{CATALOG}.{SCHEMA}.gold_kpi_market_share"
}

# -----------------------------------------------------------------------------
# BUSINESS CONFIGURATION
# -----------------------------------------------------------------------------
BUSINESS_CONFIG = {
    # Date dimension range
    "date_start": "2022-01-01",
    "date_end": "2026-12-31",
    
    # Stock calculation assumptions
    "default_lead_time_days": 7,
    "stock_out_threshold_days": 0,
    "overstock_threshold_days": 45,
    
    # Price competitiveness thresholds
    "price_variance_tolerance": 0.05,  # 5% tolerance
    "price_index_baseline": 100.0,
    
    # Market share calculation
    "market_share_lookback_days": 30,
    
    # Lost sales estimation
    "avg_daily_sales_lookback": 14
}

# -----------------------------------------------------------------------------
# SERVERLESS OPTIMIZATION FLAGS
# -----------------------------------------------------------------------------
SERVERLESS_CONFIG = {
    "use_broadcast_joins": True,
    "max_broadcast_size_mb": 10,
    "partition_by_date": True,
    "coalesce_small_files": True,
    "target_file_size_mb": 128
}

print("✅ Gold Layer Configuration Loaded")
print(f"   Catalog: {CATALOG}")
print(f"   Schema: {SCHEMA}")
print(f"   Silver Tables: {len(SILVER_TABLES)}")
print(f"   Gold Tables: {len(GOLD_TABLES)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Shared Utility Functions

# COMMAND ----------

# =============================================================================
# SURROGATE KEY GENERATION
# =============================================================================

def generate_surrogate_key(*cols):
    """
    Generate a deterministic surrogate key from business key columns.
    Uses SHA-256 hash truncated to 16 characters for uniqueness.
    
    Args:
        *cols: Column names to include in the key
        
    Returns:
        Column expression generating the surrogate key
    """
    return sha2(concat_ws("||", *[col(c).cast(StringType()) for c in cols]), 256).substr(1, 16)


def generate_date_sk(date_col):
    """
    Generate integer surrogate key from date in YYYYMMDD format.
    
    Args:
        date_col: Column containing date
        
    Returns:
        Column expression with integer date key
    """
    return date_format(col(date_col), "yyyyMMdd").cast(IntegerType())


# =============================================================================
# SCD TYPE 2 UTILITIES
# =============================================================================

def apply_scd2_merge(spark, target_table, source_df, business_keys, attribute_cols, sk_col):
    """
    Apply SCD Type 2 merge logic for slowly changing dimensions.
    
    Strategy: MERGE with update-insert pattern
    - New records: Insert with is_current=True
    - Changed records: Close old (valid_to, is_current=False), insert new version
    - Unchanged records: No action
    
    Args:
        spark: SparkSession
        target_table: Target Delta table name
        source_df: Source DataFrame with new/updated records
        business_keys: List of business key column names
        attribute_cols: List of attribute columns to track changes
        sk_col: Surrogate key column name
    """
    from delta.tables import DeltaTable
    
    # Check if target exists
    if not spark._jsparkSession.catalog().tableExists(target_table):
        # Initial load - insert all with SCD2 metadata
        initial_df = source_df \
            .withColumn("valid_from", current_date()) \
            .withColumn("valid_to", lit("9999-12-31").cast(DateType())) \
            .withColumn("is_current", lit(True))
        
        initial_df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("delta.columnMapping.mode", "name") \
            .saveAsTable(target_table)
        return
    
    # Build change detection condition
    change_conditions = [f"target.{c} <> source.{c}" for c in attribute_cols]
    change_condition = " OR ".join(change_conditions)
    
    # Build merge key condition
    merge_key_condition = " AND ".join([f"target.{k} = source.{k}" for k in business_keys])
    
    delta_table = DeltaTable.forName(spark, target_table)
    
    # Prepare source with SCD2 columns
    source_with_scd2 = source_df \
        .withColumn("valid_from", current_date()) \
        .withColumn("valid_to", lit("9999-12-31").cast(DateType())) \
        .withColumn("is_current", lit(True))
    
    # MERGE operation
    delta_table.alias("target").merge(
        source_with_scd2.alias("source"),
        f"{merge_key_condition} AND target.is_current = true"
    ).whenMatchedUpdate(
        condition=change_condition,
        set={
            "valid_to": "current_date()",
            "is_current": "false"
        }
    ).whenNotMatchedInsertAll().execute()
    
    # Insert new versions for changed records
    # This requires a second pass - handled by the dimension notebooks


# =============================================================================
# WRITE UTILITIES (ONE WRITE PER TABLE)
# =============================================================================

def write_gold_table(df, table_name, partition_cols=None, mode="overwrite"):
    """
    Write DataFrame to Gold Delta table with Serverless optimizations.
    
    Single write action per call - critical for Serverless stability.
    
    Args:
        df: DataFrame to write
        table_name: Full table name (catalog.schema.table)
        partition_cols: Optional list of partition columns
        mode: Write mode (overwrite or append)
    """
    writer = df.write \
        .format("delta") \
        .mode(mode) \
        .option("delta.columnMapping.mode", "name")
    
    if partition_cols:
        writer = writer \
            .option("partitionOverwriteMode", "dynamic") \
            .partitionBy(*partition_cols)
    
    writer.saveAsTable(table_name)
    
    print(f"✅ Written to {table_name} (mode={mode})")


def write_gold_table_incremental(df, table_name, merge_keys, partition_col=None):
    """
    Write DataFrame using incremental MERGE for facts.
    
    Args:
        df: DataFrame with new/updated records
        table_name: Full table name
        merge_keys: List of columns forming the unique key
        partition_col: Optional partition column for optimization
    """
    from delta.tables import DeltaTable
    
    # Check if table exists
    if not spark._jsparkSession.catalog().tableExists(table_name):
        # Initial load
        writer = df.write.format("delta").mode("overwrite")
        if partition_col:
            writer = writer.partitionBy(partition_col)
        writer.saveAsTable(table_name)
        print(f"✅ Initial load to {table_name}")
        return
    
    # Incremental MERGE
    delta_table = DeltaTable.forName(spark, table_name)
    merge_condition = " AND ".join([f"target.{k} = source.{k}" for k in merge_keys])
    
    delta_table.alias("target").merge(
        df.alias("source"),
        merge_condition
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    
    print(f"✅ Incremental merge to {table_name}")


# =============================================================================
# VALIDATION UTILITIES (ZERO-COMPUTE)
# =============================================================================

def get_table_metrics(spark, table_name):
    """
    Get table metrics using Delta History (zero compute cost).
    
    Args:
        spark: SparkSession
        table_name: Full table name
        
    Returns:
        Dictionary with metrics
    """
    try:
        history = spark.sql(f"DESCRIBE HISTORY {table_name} LIMIT 1").first()
        metrics = history["operationMetrics"] or {}
        return {
            "table": table_name,
            "operation": history["operation"],
            "timestamp": history["timestamp"],
            "rows": metrics.get("numOutputRows", metrics.get("numTargetRowsInserted", "N/A")),
            "files": metrics.get("numFiles", "N/A")
        }
    except Exception as e:
        return {"table": table_name, "error": str(e)}


def validate_gold_layer(spark):
    """
    Validate all Gold tables using Delta metadata (no DataFrame scans).
    """
    print("=" * 60)
    print("GOLD LAYER VALIDATION (Zero-Compute)")
    print("=" * 60)
    
    for name, table in GOLD_TABLES.items():
        metrics = get_table_metrics(spark, table)
        if "error" in metrics:
            print(f"❌ {name}: {metrics['error']}")
        else:
            print(f"✅ {name}: {metrics['rows']} rows, {metrics['files']} files")
    
    print("=" * 60)


def get_conf_safe(key: str, default: str = "N/A") -> str:
    """
    Safely get Spark configuration value with fallback default.
    Prevents CONFIG_NOT_AVAILABLE errors in Databricks.
    
    Args:
        key: Configuration key name
        default: Default value if key not found
        
    Returns:
        Configuration value or default
    """
    try:
        return spark.conf.get(key)
    except:
        return default

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Automated Pipeline Execution
# MAGIC 
# MAGIC This section implements the **automatic Gold Layer pipeline** with:
# MAGIC - Phase-based execution (Dimensions → Facts → KPIs → Validation)
# MAGIC - Error handling and status tracking
# MAGIC - Execution time metrics
# MAGIC - Rollback-safe design (each notebook is atomic)

# COMMAND ----------

from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# Pipeline Configuration
PIPELINE_CONFIG = {
    "timeout_seconds": 3600,  # 1 hour max per notebook
    "run_validation": True,
    "stop_on_error": True,
    "parallel_dimensions": False  # Set True if cluster allows parallelism
}

# Pipeline State (Python memory - NOT Spark conf)
# This avoids CONFIG_NOT_AVAILABLE errors in Databricks
PIPELINE_STATE = {
    "status": "NOT_EXECUTED",
    "duration": 0,
    "last_check": datetime.now().isoformat(),
    "notebooks_success": 0,
    "notebooks_failed": 0
}

# Notebook execution phases (dependency order)
PIPELINE_PHASES = {
    "phase_1_dimensions": [
        ("gold_dim_date", "./dimensions/gold_dim_date"),
        ("gold_dim_product", "./dimensions/gold_dim_product"),
        ("gold_dim_pdv", "./dimensions/gold_dim_pdv"),
    ],
    "phase_2_facts": [
        ("gold_fact_sell_in", "./facts/gold_fact_sell_in"),
        ("gold_fact_price_audit", "./facts/gold_fact_price_audit"),
        ("gold_fact_stock", "./facts/gold_fact_stock"),
    ],
    "phase_3_kpis": [
        ("gold_kpi_market_visibility", "./kpis/gold_kpi_market_visibility"),
        ("gold_kpi_market_share", "./kpis/gold_kpi_market_share"),
    ],
    "phase_4_validation": [
        ("gold_validation", "./validation/gold_validation"),
    ]
}

# COMMAND ----------

def run_notebook_safe(notebook_name: str, notebook_path: str, timeout: int = 3600) -> dict:
    """
    Execute a notebook with error handling and timing.
    
    Args:
        notebook_name: Display name for logging
        notebook_path: Relative path to notebook
        timeout: Max execution time in seconds
        
    Returns:
        dict with status, duration, and any error message
    """
    start_time = datetime.now()
    result = {
        "notebook": notebook_name,
        "path": notebook_path,
        "status": "PENDING",
        "duration_seconds": 0,
        "error": None,
        "start_time": start_time.isoformat()
    }
    
    try:
        print(f"🚀 Starting: {notebook_name}")
        dbutils.notebook.run(notebook_path, timeout)
        
        end_time = datetime.now()
        result["status"] = "SUCCESS"
        result["duration_seconds"] = (end_time - start_time).total_seconds()
        print(f"✅ Completed: {notebook_name} ({result['duration_seconds']:.1f}s)")
        
    except Exception as e:
        end_time = datetime.now()
        result["status"] = "FAILED"
        result["duration_seconds"] = (end_time - start_time).total_seconds()
        result["error"] = str(e)
        print(f"❌ Failed: {notebook_name} - {str(e)}")
    
    return result


def run_phase(phase_name: str, notebooks: list, parallel: bool = False) -> list:
    """
    Execute a pipeline phase (group of notebooks).
    
    Args:
        phase_name: Name for logging
        notebooks: List of (name, path) tuples
        parallel: If True, run notebooks concurrently
        
    Returns:
        List of execution results
    """
    print("\n" + "=" * 60)
    print(f"📦 PHASE: {phase_name.upper()}")
    print("=" * 60)
    
    results = []
    timeout = PIPELINE_CONFIG["timeout_seconds"]
    
    if parallel and len(notebooks) > 1:
        # Parallel execution (use with caution on Serverless)
        with ThreadPoolExecutor(max_workers=len(notebooks)) as executor:
            futures = {
                executor.submit(run_notebook_safe, name, path, timeout): name 
                for name, path in notebooks
            }
            for future in as_completed(futures):
                results.append(future.result())
    else:
        # Sequential execution (default - safer for Serverless)
        for name, path in notebooks:
            result = run_notebook_safe(name, path, timeout)
            results.append(result)
            
            # Stop on error if configured
            if result["status"] == "FAILED" and PIPELINE_CONFIG["stop_on_error"]:
                print(f"\n⛔ Pipeline stopped due to error in {name}")
                break
    
    return results


def execute_gold_pipeline() -> dict:
    """
    Execute the complete Gold Layer pipeline with all phases.
    
    Returns:
        dict with pipeline execution summary
    """
    pipeline_start = datetime.now()
    
    print("\n" + "🔷" * 30)
    print("   GOLD LAYER PIPELINE - AUTOMATED EXECUTION")
    print("🔷" * 30)
    print(f"\n⏰ Started at: {pipeline_start.isoformat()}")
    print(f"📋 Configuration: {PIPELINE_CONFIG}")
    
    all_results = {}
    pipeline_failed = False
    
    # Phase 1: Dimensions (can be parallel since no dependencies)
    phase_1_results = run_phase(
        "phase_1_dimensions",
        PIPELINE_PHASES["phase_1_dimensions"],
        parallel=PIPELINE_CONFIG["parallel_dimensions"]
    )
    all_results["phase_1_dimensions"] = phase_1_results
    
    if any(r["status"] == "FAILED" for r in phase_1_results):
        pipeline_failed = True
        if PIPELINE_CONFIG["stop_on_error"]:
            print("\n⛔ Pipeline aborted after Phase 1 failures")
            return generate_pipeline_summary(all_results, pipeline_start, "ABORTED")
    
    # Phase 2: Facts (depend on dimensions)
    phase_2_results = run_phase(
        "phase_2_facts",
        PIPELINE_PHASES["phase_2_facts"],
        parallel=False  # Facts should be sequential for memory management
    )
    all_results["phase_2_facts"] = phase_2_results
    
    if any(r["status"] == "FAILED" for r in phase_2_results):
        pipeline_failed = True
        if PIPELINE_CONFIG["stop_on_error"]:
            print("\n⛔ Pipeline aborted after Phase 2 failures")
            return generate_pipeline_summary(all_results, pipeline_start, "ABORTED")
    
    # Phase 3: KPIs (depend on facts)
    phase_3_results = run_phase(
        "phase_3_kpis",
        PIPELINE_PHASES["phase_3_kpis"],
        parallel=False
    )
    all_results["phase_3_kpis"] = phase_3_results
    
    if any(r["status"] == "FAILED" for r in phase_3_results):
        pipeline_failed = True
        if PIPELINE_CONFIG["stop_on_error"]:
            print("\n⛔ Pipeline aborted after Phase 3 failures")
            return generate_pipeline_summary(all_results, pipeline_start, "ABORTED")
    
    # Phase 4: Validation (optional, read-only)
    if PIPELINE_CONFIG["run_validation"]:
        phase_4_results = run_phase(
            "phase_4_validation",
            PIPELINE_PHASES["phase_4_validation"],
            parallel=False
        )
        all_results["phase_4_validation"] = phase_4_results
    
    # Generate final summary
    final_status = "FAILED" if pipeline_failed else "SUCCESS"
    return generate_pipeline_summary(all_results, pipeline_start, final_status)


def generate_pipeline_summary(results: dict, start_time: datetime, status: str) -> dict:
    """
    Generate pipeline execution summary with metrics.
    """
    end_time = datetime.now()
    total_duration = (end_time - start_time).total_seconds()
    
    # Count results
    total_notebooks = sum(len(phase) for phase in results.values())
    successful = sum(1 for phase in results.values() for r in phase if r["status"] == "SUCCESS")
    failed = sum(1 for phase in results.values() for r in phase if r["status"] == "FAILED")
    
    summary = {
        "status": status,
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat(),
        "total_duration_seconds": total_duration,
        "notebooks_total": total_notebooks,
        "notebooks_success": successful,
        "notebooks_failed": failed,
        "phase_results": results
    }
    
    # Print summary
    print("\n" + "=" * 60)
    print("📊 PIPELINE EXECUTION SUMMARY")
    print("=" * 60)
    print(f"Status: {'✅ ' + status if status == 'SUCCESS' else '❌ ' + status}")
    print(f"Duration: {total_duration:.1f} seconds")
    print(f"Notebooks: {successful}/{total_notebooks} successful")
    
    if failed > 0:
        print(f"\n⚠️ Failed notebooks:")
        for phase_name, phase_results in results.items():
            for r in phase_results:
                if r["status"] == "FAILED":
                    print(f"   - {r['notebook']}: {r['error']}")
    
    print("=" * 60)
    
    return summary

# COMMAND ----------

# MAGIC %md
# MAGIC ### Execute Pipeline
# MAGIC 
# MAGIC Run the cell below to execute the **complete Gold Layer pipeline** automatically.
# MAGIC 
# MAGIC The pipeline will:
# MAGIC 1. Build all **Dimensions** (Date, Product, PDV)
# MAGIC 2. Build all **Facts** (Sell-In, Price Audit, Stock)
# MAGIC 3. Build all **KPIs** (Market Visibility, Market Share)
# MAGIC 4. Run **Validation** checks
# MAGIC 
# MAGIC **Two ways to execute:**
# MAGIC 1. **Widget Mode:** Set `run_pipeline = yes` in widget and run all cells
# MAGIC 2. **Direct Mode:** Set `RUN_PIPELINE = True` below

# COMMAND ----------

# =============================================================================
# 🚀 AUTOMATIC PIPELINE EXECUTION
# =============================================================================

# Update PIPELINE_CONFIG with widget values
PIPELINE_CONFIG["stop_on_error"] = WIDGET_STOP_ON_ERROR
PIPELINE_CONFIG["run_validation"] = WIDGET_RUN_VALIDATION
PIPELINE_CONFIG["timeout_seconds"] = WIDGET_TIMEOUT

# Determine if pipeline should run
# Priority: Widget > Hardcoded flag
RUN_PIPELINE = False  # Set True for hardcoded auto-execution

# Execute if either widget or flag is True
should_run = WIDGET_RUN_PIPELINE or RUN_PIPELINE

# Update last check timestamp
PIPELINE_STATE["last_check"] = datetime.now().isoformat()

if should_run:
    print("\n" + "🤖" * 30)
    print("   AUTOMATIC PIPELINE EXECUTION TRIGGERED")
    print("🤖" * 30)
    print(f"\nTrigger: {'Widget' if WIDGET_RUN_PIPELINE else 'RUN_PIPELINE flag'}")
    print(f"Configuration: {PIPELINE_CONFIG}")
    
    # Execute the complete pipeline
    pipeline_result = execute_gold_pipeline()
    
    # Store result in Python memory (NOT Spark conf)
    PIPELINE_STATE["status"] = pipeline_result["status"]
    PIPELINE_STATE["duration"] = pipeline_result["total_duration_seconds"]
    PIPELINE_STATE["notebooks_success"] = pipeline_result["notebooks_success"]
    PIPELINE_STATE["notebooks_failed"] = pipeline_result["notebooks_failed"]
    
    # Exit with status for CI/CD and Databricks Workflows
    if pipeline_result["status"] != "SUCCESS":
        exit_message = f"FAILED: {pipeline_result['notebooks_failed']}/{pipeline_result['notebooks_total']} notebooks failed"
        print(f"\n❌ {exit_message}")
        dbutils.notebook.exit(exit_message)
    else:
        exit_message = f"SUCCESS: {pipeline_result['notebooks_success']} notebooks completed in {pipeline_result['total_duration_seconds']:.1f}s"
        print(f"\n✅ {exit_message}")
        dbutils.notebook.exit(exit_message)
else:
    print("\n📋 Pipeline not triggered. To execute:")
    print("   1. Set widget 'Execute Pipeline' = 'yes' and Run All, OR")
    print("   2. Set RUN_PIPELINE = True in the code above")
    print(f"\n📊 Current Status:")
    print(f"   - status: {PIPELINE_STATE['status']}")
    print(f"   - duration: {PIPELINE_STATE['duration']}s")
    print(f"   - last_check: {PIPELINE_STATE['last_check']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Manual Execution (Individual Notebooks)
# MAGIC 
# MAGIC If you prefer to run notebooks individually for debugging:

# COMMAND ----------

# Individual notebook execution (for debugging)
# Uncomment any line below to run a specific notebook:

# run_notebook_safe("gold_dim_date", "./dimensions/gold_dim_date")
# run_notebook_safe("gold_dim_product", "./dimensions/gold_dim_product")
# run_notebook_safe("gold_dim_pdv", "./dimensions/gold_dim_pdv")
# run_notebook_safe("gold_fact_sell_in", "./facts/gold_fact_sell_in")
# run_notebook_safe("gold_fact_price_audit", "./facts/gold_fact_price_audit")
# run_notebook_safe("gold_fact_stock", "./facts/gold_fact_stock")
# run_notebook_safe("gold_kpi_market_visibility", "./kpis/gold_kpi_market_visibility")
# run_notebook_safe("gold_kpi_market_share", "./kpis/gold_kpi_market_share")
# run_notebook_safe("gold_validation", "./validation/gold_validation")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Power BI & Streamlit Integration
# MAGIC 
# MAGIC ### Direct Query Tables (Recommended)
# MAGIC 
# MAGIC | Consumer | Table | Purpose |
# MAGIC |----------|-------|---------|
# MAGIC | Power BI | `gold_kpi_market_visibility_daily` | Daily operational dashboard |
# MAGIC | Power BI | `gold_kpi_market_share` | Market share trends |
# MAGIC | Power BI | `gold_dim_*` + `gold_fact_*` | Ad-hoc analysis |
# MAGIC | Streamlit | `gold_kpi_market_visibility_daily` | Real-time alerts |
# MAGIC | Streamlit | `gold_kpi_market_share` | Executive summary |
# MAGIC 
# MAGIC ### DAX Complexity Reduction
# MAGIC 
# MAGIC The KPI tables are **pre-aggregated** to eliminate complex DAX calculations:
# MAGIC 
# MAGIC | Without Gold KPIs | With Gold KPIs |
# MAGIC |-------------------|----------------|
# MAGIC | DAX: `CALCULATE(SUM(Fact[qty]), FILTER(...))` | Direct: `SELECT availability_rate` |
# MAGIC | DAX: `SUMX(...)` with nested filters | Direct: `SELECT lost_sales_estimated` |
# MAGIC | Power BI calculates at query time | Pre-calculated daily |
# MAGIC | Slow dashboards | Fast direct query |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Monitoring & Alerting
# MAGIC 
# MAGIC ### Delta Live Tables Integration (Optional)
# MAGIC 
# MAGIC For production environments, consider wrapping Gold notebooks in DLT pipelines:
# MAGIC 
# MAGIC ```python
# MAGIC # DLT Pipeline (optional enhancement)
# MAGIC @dlt.table(
# MAGIC     name="gold_kpi_market_visibility_daily",
# MAGIC     comment="Daily market visibility KPIs",
# MAGIC     table_properties={"quality": "gold"}
# MAGIC )
# MAGIC @dlt.expect_or_drop("valid_date", "date_sk IS NOT NULL")
# MAGIC def gold_kpi_market_visibility():
# MAGIC     # KPI calculation logic here
# MAGIC     pass
# MAGIC ```
# MAGIC 
# MAGIC ### Recommended Alerts
# MAGIC 
# MAGIC | Alert | Condition | Action |
# MAGIC |-------|-----------|--------|
# MAGIC | Stock-out Risk | `stock_out_flag = true AND channel = 'KEY_ACCOUNT'` | Slack notification |
# MAGIC | Price Out of Market | `price_index < 85 OR price_index > 115` | Email to pricing team |
# MAGIC | Sell-In Anomaly | `sell_in_sell_out_ratio > 2.0` | Dashboard highlight |
# MAGIC | Lost Sales | `lost_sales_estimated > threshold` | Executive report |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC 
# MAGIC ## Notebook Summary
# MAGIC 
# MAGIC This orchestrator provides:
# MAGIC 
# MAGIC 1. **Centralized Configuration** - All table names, thresholds, and settings
# MAGIC 2. **Shared Utilities** - Surrogate key generation, SCD2 helpers, write functions
# MAGIC 3. **Architecture Documentation** - Visual diagram and execution strategy
# MAGIC 4. **Integration Guide** - Power BI and Streamlit consumption patterns
# MAGIC 
# MAGIC **Next Steps:**
# MAGIC - Run dimension notebooks (`gold_dim_*.py`)
# MAGIC - Run fact notebooks (`gold_fact_*.py`)
# MAGIC - Run KPI notebooks (`gold_kpi_*.py`)
# MAGIC - Run validation notebook (`gold_validation.py`)
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC **Author:** Diego Mayorga | diego.mayorgacapera@gmail.com  
# MAGIC **Date:** 2026-01-06  
# MAGIC **Repository:** [github.com/DIEGO77M/BI_Market_Visibility](https://github.com/DIEGO77M/BI_Market_Visibility)
