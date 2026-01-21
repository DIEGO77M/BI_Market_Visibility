"""
SilverProductsValidator: Enterprise-Grade Validation Layer

Implements comprehensive data quality validation for Silver Products Dimension
following Medallion Architecture validation principles.

Platform: Databricks Serverless (Unity Catalog)
Strategy: Fail-fast with detailed quality metrics
Constraints: No transformations, no corrections, no persistence
"""

from typing import Dict, Any, List, Set
from datetime import datetime, date, timedelta
import logging
from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.types import StructType, BooleanType
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("SilverProductsValidator")


class SilverProductsValidator:
    """
    Enterprise-grade validator for workspace.silver.dim_products.
    
    Validates:
    - Schema contract (columns + types)
    - Primary key uniqueness (only extra duplicates counted)
    - Critical null constraints
    - Data integrity (hierarchies, audit fields)
    - Domain validity (allowed values)
    - Temporal consistency (date ranges)
    - Format validation (patterns, UUIDs)
    - Referential integrity (hierarchy consistency)
    """

    def __init__(self, spark: SparkSession, config: Dict[str, Any] = None):
        self.spark = spark
        self.config = config or {}
        self.table_name = self.config.get("table_name", "workspace.silver.dim_products")
        
        # Contractual Schema
        self.expected_schema = {
            "product_code": "string",
            "product_name": "string",
            "brand": "string",
            "segment": "string",
            "subsegment": "string",
            "category": "string",
            "subcategory": "string",
            "product_full_name": "string",
            "product_hierarchy": StructType,
            "ingestion_timestamp": "timestamp",
            "load_date": "date",
            "source_file": "string",
            "bronze_batch_id": "string",
            "silver_processed_at": "timestamp",
            "silver_batch_id": "string"
        }
        
        # Business Rules: Domain values
        self.allowed_segments = self.config.get("allowed_segments", 
            ["culinary", "beverages", "dairy"])
        
        self.allowed_categories = self.config.get("allowed_categories",
            ["ambient_culinary", "beverages", "dairy"])
        
        # Allowed hierarchies: (segment, category, subcategory)
        self.allowed_hierarchies = self.config.get("allowed_hierarchies", [
            ("culinary", "ambient_culinary", "cbr_l4_dehy_seasoning"),
            ("culinary", "ambient_culinary", "cbr_l4_soups"),
            ("culinary", "ambient_culinary", "cbr_l4_sauces"),
            ("beverages", "beverages", "cbr_l4_coffee"),
            ("dairy", "dairy", "cbr_l4_powdered_milk"),
        ])
        self.allowed_hierarchies_set = set(self.allowed_hierarchies)
        
        # Date validation ranges
        self.min_valid_date = self.config.get("min_valid_date", date(2000, 1, 1))
        self.max_valid_date = self.config.get("max_valid_date", date.today() + timedelta(days=1))

    def _read_table(self) -> DataFrame:
        """Reads the Silver table."""
        logger.info(f"Reading table: {self.table_name}")
        return self.spark.table(self.table_name)

    def _validate_schema(self, df: DataFrame) -> DataFrame:
        """
        Validates schema contract: columns existence and data types.
        Fails fast if contract is violated.
        """
        logger.info("Validating schema contract...")
        
        actual_schema = {f.name: f.dataType for f in df.schema.fields}
        missing_columns = []
        type_mismatches = []

        for col, expected_type in self.expected_schema.items():
            if col not in actual_schema:
                missing_columns.append(col)
            elif expected_type == StructType:
                if not isinstance(actual_schema[col], StructType):
                    type_mismatches.append(
                        f"{col}: expected StructType, got {type(actual_schema[col]).__name__}"
                    )
            elif actual_schema[col].typeName() != expected_type:
                type_mismatches.append(
                    f"{col}: expected {expected_type}, got {actual_schema[col].typeName()}"
                )

        if missing_columns:
            raise ValueError(
                f"SCHEMA VIOLATION: Missing columns in {self.table_name}: {missing_columns}"
            )
        
        if type_mismatches:
            raise ValueError(
                f"SCHEMA VIOLATION: Type mismatches in {self.table_name}: {type_mismatches}"
            )
        
        logger.info("Schema contract validation PASSED")
        return df

    def _build_validation_logic(self, df: DataFrame) -> DataFrame:
        """
        Builds comprehensive validation logic into DataFrame lineage.
        Does NOT trigger execution (lazy evaluation).
        """
        logger.info("Building validation logic (lazy)...")
        
        # 1. Primary Key Null Check
        df = df.withColumn(
            "_pk_null_violation",
            F.col("product_code").isNull() | 
            (F.length(F.trim(F.col("product_code"))) == 0) |
            F.col("category").isNull() |
            F.col("load_date").isNull()
        )
        
        # 2. Format Validation: product_code pattern (P + 5 digits)
        df = df.withColumn(
            "_format_violation",
            ~F.col("product_code").rlike(r"^P\d{5}$") |
            (F.length(F.col("silver_batch_id")) != 36)
        )
        
        # 3. Date Range Validation
        df = df.withColumn(
            "_date_range_violation",
            (F.col("load_date") < F.lit(self.min_valid_date)) |
            (F.col("load_date") > F.lit(self.max_valid_date))
        )
        
        # 4. Domain Validation: segment
        df = df.withColumn(
            "_segment_domain_violation",
            ~F.col("segment").isin(self.allowed_segments)
        )
        
        # 5. Domain Validation: category
        df = df.withColumn(
            "_category_domain_violation",
            ~F.col("category").isin(self.allowed_categories)
        )
        
        # 6. Hierarchy Structure Validation
        df = df.withColumn(
            "_hierarchy_violation",
            F.col("product_hierarchy").isNull() |
            F.col("product_hierarchy.segment").isNull() |
            F.col("product_hierarchy.category").isNull() |
            F.col("product_hierarchy.subcategory").isNull()
        )
        
        # 7. Referential Integrity: Hierarchy consistency
        # Build valid combinations as SQL conditions (avoid UDF serialization issues)
        valid_conditions = [
            (F.col("segment") == seg) & 
            (F.col("category") == cat) & 
            (F.col("subcategory") == subcat)
            for seg, cat, subcat in self.allowed_hierarchies
        ]
        
        # Combine all valid conditions with OR
        is_valid_hierarchy = valid_conditions[0]
        for condition in valid_conditions[1:]:
            is_valid_hierarchy = is_valid_hierarchy | condition
        
        df = df.withColumn(
            "_referential_violation",
            ~is_valid_hierarchy
        )
        
        # 8. Temporal Consistency: Audit fields validation
        df = df.withColumn(
            "_audit_violation",
            (F.col("silver_processed_at") < F.col("ingestion_timestamp")) |
            (F.col("load_date") > F.current_date())
        )
        
        # 9. Uniqueness Check (Primary Key: product_code + load_date)
        # Only count EXTRA duplicates (first occurrence is valid)
        window_pk = Window.partitionBy("product_code", "load_date").orderBy("silver_processed_at")
        df = df.withColumn("_row_number", F.row_number().over(window_pk))
        df = df.withColumn("_uniqueness_violation", F.col("_row_number") > 1)
        
        return df

    def run(self) -> Dict[str, Any]:
        """
        Executes the complete validation pipeline.
        Triggers ONE aggregated action for all quality checks.
        """
        logger.info("Starting Enterprise Validation Pipeline...")
        start_time = datetime.now()
        
        try:
            # 1. Read & Schema Check (Lazy)
            df = self._read_table()
            df = self._validate_schema(df)
            
            # 2. Build All Validation Logic (Lazy)
            df_validated = self._build_validation_logic(df)
            
            # 3. Execute Single Aggregated Action
            logger.info("Executing aggregated quality checks...")
            
            metrics = df_validated.select(
                F.count("*").alias("total_records"),
                F.sum(F.col("_pk_null_violation").cast("int")).alias("pk_null_violations"),
                F.sum(F.col("_format_violation").cast("int")).alias("format_violations"),
                F.sum(F.col("_date_range_violation").cast("int")).alias("date_range_violations"),
                F.sum(F.col("_segment_domain_violation").cast("int")).alias("segment_domain_violations"),
                F.sum(F.col("_category_domain_violation").cast("int")).alias("category_domain_violations"),
                F.sum(F.col("_hierarchy_violation").cast("int")).alias("hierarchy_violations"),
                F.sum(F.col("_referential_violation").cast("int")).alias("referential_violations"),
                F.sum(F.col("_audit_violation").cast("int")).alias("audit_violations"),
                F.sum(F.col("_uniqueness_violation").cast("int")).alias("uniqueness_violations")
            ).collect()[0]
            
            # Extract metrics
            total_records = metrics["total_records"]
            pk_null = metrics["pk_null_violations"] or 0
            format_viol = metrics["format_violations"] or 0
            date_range = metrics["date_range_violations"] or 0
            segment_domain = metrics["segment_domain_violations"] or 0
            category_domain = metrics["category_domain_violations"] or 0
            hierarchy = metrics["hierarchy_violations"] or 0
            referential = metrics["referential_violations"] or 0
            audit = metrics["audit_violations"] or 0
            uniqueness = metrics["uniqueness_violations"] or 0
            
            # 4. Volume Check
            if total_records == 0:
                raise ValueError(f"CRITICAL: Table {self.table_name} is EMPTY")
            
            # 5. Build Structured Error Reports
            error_structs = []
            
            if pk_null > 0:
                error_structs.append({
                    "error_code": "NULL_VIOLATION",
                    "rule_name": "not_null_product_code_category_load_date",
                    "metric_value": int(pk_null),
                    "message": f"Found {pk_null} records with NULL product_code, category, or load_date."
                })
            
            if format_viol > 0:
                error_structs.append({
                    "error_code": "FORMAT_VIOLATION",
                    "rule_name": "product_code_pattern_Pddddd_and_uuid_length",
                    "metric_value": int(format_viol),
                    "message": f"Found {format_viol} records with invalid product_code format or silver_batch_id."
                })
            
            if date_range > 0:
                error_structs.append({
                    "error_code": "DATE_RANGE_VIOLATION",
                    "rule_name": "valid_load_date_range",
                    "metric_value": int(date_range),
                    "message": f"Found {date_range} records with load_date outside valid range."
                })
            
            if segment_domain > 0:
                error_structs.append({
                    "error_code": "SEGMENT_DOMAIN_VIOLATION",
                    "rule_name": "segment_in_allowed_values",
                    "metric_value": int(segment_domain),
                    "message": f"Found {segment_domain} records with invalid segment values."
                })
            
            if category_domain > 0:
                error_structs.append({
                    "error_code": "CATEGORY_DOMAIN_VIOLATION",
                    "rule_name": "category_in_allowed_values",
                    "metric_value": int(category_domain),
                    "message": f"Found {category_domain} records with invalid category values."
                })
            
            if hierarchy > 0:
                error_structs.append({
                    "error_code": "HIERARCHY_VIOLATION",
                    "rule_name": "not_null_product_hierarchy_struct",
                    "metric_value": int(hierarchy),
                    "message": f"Found {hierarchy} records with null/invalid hierarchy struct."
                })
            
            if referential > 0:
                error_structs.append({
                    "error_code": "REFERENTIAL_INTEGRITY_VIOLATION",
                    "rule_name": "consistent_segment_category_subcategory",
                    "metric_value": int(referential),
                    "message": f"Found {referential} records with inconsistent hierarchy combinations."
                })
            
            if audit > 0:
                error_structs.append({
                    "error_code": "AUDIT_FIELD_VIOLATION",
                    "rule_name": "audit_fields_validity",
                    "metric_value": int(audit),
                    "message": f"Found {audit} records with audit field violations."
                })
            
            if uniqueness > 0:
                error_structs.append({
                    "error_code": "UNIQUENESS_VIOLATION",
                    "rule_name": "unique_product_code_load_date",
                    "metric_value": int(uniqueness),
                    "message": f"Found {uniqueness} duplicate records (extras only)."
                })
            
            # 6. Fail-Fast on Critical Violations
            if error_structs:
                for err in error_structs:
                    logger.error(
                        f"[VALIDATION_ERROR] error_code={err['error_code']} "
                        f"rule={err['rule_name']} value={err['metric_value']} "
                        f"msg={err['message']}"
                    )
                error_codes = [err['error_code'] for err in error_structs]
                raise ValueError(f"Validation Critical Failure: {error_codes}")
            
            # 7. Calculate Quality Score
            total_violations = sum([
                pk_null, format_viol, date_range, segment_domain, 
                category_domain, hierarchy, referential, audit, uniqueness
            ])
            quality_score = round((1 - (total_violations / total_records)) * 100, 2)
            
            duration = (datetime.now() - start_time).total_seconds()
            
            logger.info(
                f"Validation SUCCESS: {total_records} records validated, "
                f"Quality Score: {quality_score}%"
            )
            
            return {
                "status": "SUCCESS",
                "table": self.table_name,
                "records_validated": total_records,
                "validation_timestamp": datetime.utcnow().isoformat(),
                "duration_seconds": round(duration, 2),
                "quality_metrics": {
                    "data_quality_score": quality_score,
                    "pk_null_violations": pk_null,
                    "format_violations": format_viol,
                    "date_range_violations": date_range,
                    "segment_domain_violations": segment_domain,
                    "category_domain_violations": category_domain,
                    "hierarchy_violations": hierarchy,
                    "referential_violations": referential,
                    "audit_violations": audit,
                    "uniqueness_violations": uniqueness,
                    "total_violations": total_violations
                },
                "error_message": None
            }
        
        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            logger.error(f"Validation pipeline failed: {str(e)}")
            
            # Re-raise to stop orchestration (fail-fast)
            raise e


# ------------------------------------------------------------------------------
# Public Interface
# ------------------------------------------------------------------------------
def run_validation_dim_products(
    spark: SparkSession = None,
    config: Dict[str, Any] = None
) -> Dict[str, Any]:
    """
    Public entrypoint for orchestrators and notebooks.
    
    Args:
        spark: SparkSession instance
        config: Optional configuration dict with keys:
            - table_name: Override default table
            - allowed_segments: List of valid segment values
            - allowed_categories: List of valid category values
            - allowed_hierarchies: List of tuples (segment, category, subcategory)
            - min_valid_date: Minimum acceptable load_date
            - max_valid_date: Maximum acceptable load_date
    
    Returns:
        Dict with validation status and detailed quality metrics
        
    Raises:
        ValueError: If any validation rule is violated (fail-fast)
    """
    if spark is None:
        spark = (
            SparkSession.getActiveSession() 
            or SparkSession.builder.appName("silver_products_validator").getOrCreate()
        )
    
    validator = SilverProductsValidator(spark, config)
    return validator.run()


if __name__ == "__main__":
    try:
        result = run_validation_dim_products()
        print(result)
    except Exception as e:
        print(f"Validation Failed: {e}")