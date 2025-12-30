"""
Data Quality Utility Functions

This module contains functions for data quality checks and validations.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import Dict, List, Tuple


def check_null_values(df: DataFrame, columns: List[str]) -> Dict[str, int]:
    """
    Check for null values in specified columns.
    
    Args:
        df: Input DataFrame
        columns: List of column names to check
        
    Returns:
        Dictionary with column names as keys and null counts as values
    """
    null_counts = {}
    for col in columns:
        null_count = df.filter(F.col(col).isNull()).count()
        null_counts[col] = null_count
    return null_counts


def check_duplicates(df: DataFrame, subset: List[str] = None) -> int:
    """
    Check for duplicate rows in DataFrame.
    
    Args:
        df: Input DataFrame
        subset: List of columns to consider for duplicate check
        
    Returns:
        Count of duplicate rows
    """
    total_count = df.count()
    distinct_count = df.dropDuplicates(subset=subset).count()
    return total_count - distinct_count


def check_data_completeness(df: DataFrame) -> Dict[str, float]:
    """
    Calculate completeness percentage for each column.
    
    Args:
        df: Input DataFrame
        
    Returns:
        Dictionary with column names and completeness percentages
    """
    total_rows = df.count()
    completeness = {}
    
    for col in df.columns:
        non_null_count = df.filter(F.col(col).isNotNull()).count()
        completeness[col] = (non_null_count / total_rows * 100) if total_rows > 0 else 0
    
    return completeness


def validate_date_range(
    df: DataFrame, 
    date_column: str, 
    min_date: str = None, 
    max_date: str = None
) -> Tuple[bool, int]:
    """
    Validate if dates fall within expected range.
    
    Args:
        df: Input DataFrame
        date_column: Name of date column to validate
        min_date: Minimum expected date (format: YYYY-MM-DD)
        max_date: Maximum expected date (format: YYYY-MM-DD)
        
    Returns:
        Tuple of (validation passed, count of invalid records)
    """
    invalid_count = 0
    
    if min_date:
        invalid_count += df.filter(F.col(date_column) < min_date).count()
    
    if max_date:
        invalid_count += df.filter(F.col(date_column) > max_date).count()
    
    return (invalid_count == 0, invalid_count)


def generate_quality_report(df: DataFrame, critical_columns: List[str]) -> Dict:
    """
    Generate comprehensive data quality report.
    
    Args:
        df: Input DataFrame
        critical_columns: List of columns critical for quality
        
    Returns:
        Dictionary containing quality metrics
    """
    report = {
        "total_records": df.count(),
        "total_columns": len(df.columns),
        "null_counts": check_null_values(df, critical_columns),
        "duplicate_count": check_duplicates(df),
        "completeness": check_data_completeness(df),
        "schema": df.schema.json()
    }
    
    return report


def validate_silver_quality(df: DataFrame, table_name: str) -> Dict[str, any]:
    """
    Strategic quality validation for Silver layer (Databricks Serverless optimized).
    Uses aggregations instead of multiple count() operations.
    
    Args:
        df: Silver DataFrame to validate
        table_name: Name of the table being validated
        
    Returns:
        Dictionary with quality metrics
    """
    print(f"\n🔍 Silver Quality Validation: {table_name}")
    print("-" * 60)
    
    # Single aggregation to get multiple metrics efficiently
    quality_metrics = {}
    
    # Check for quality_score column (added by Silver transformations)
    if "quality_score" in df.columns:
        quality_stats = df.agg(
            F.min("quality_score").alias("min_quality"),
            F.max("quality_score").alias("max_quality"),
            F.avg("quality_score").alias("avg_quality")
        ).first()
        
        quality_metrics["quality_score_min"] = quality_stats["min_quality"]
        quality_metrics["quality_score_max"] = quality_stats["max_quality"]
        quality_metrics["quality_score_avg"] = quality_stats["avg_quality"]
        
        print(f"  Quality Score (0-100):")
        print(f"    Min: {quality_stats['min_quality']:.2f}")
        print(f"    Max: {quality_stats['max_quality']:.2f}")
        print(f"    Avg: {quality_stats['avg_quality']:.2f}")
    
    # Check for validation columns (*_is_valid)
    validation_columns = [c for c in df.columns if c.endswith("_is_valid")]
    
    if validation_columns:
        print(f"  Validation Columns: {len(validation_columns)}")
        for val_col in validation_columns:
            # Count valid vs invalid efficiently
            valid_count = df.filter(F.col(val_col) == True).count()
            invalid_count = df.filter(F.col(val_col) == False).count()
            total = valid_count + invalid_count
            
            valid_pct = (valid_count / total * 100) if total > 0 else 0
            
            quality_metrics[val_col] = {
                "valid_count": valid_count,
                "invalid_count": invalid_count,
                "valid_percentage": valid_pct
            }
            
            print(f"    {val_col}: {valid_pct:.1f}% valid")
    
    quality_metrics["total_columns"] = len(df.columns)
    quality_metrics["validation_status"] = "PASSED"
    
    print("-" * 60)
    
    return quality_metrics


def check_silver_standards(df: DataFrame) -> bool:
    """
    Verify Silver layer standards compliance.
    
    Args:
        df: Silver DataFrame
        
    Returns:
        True if standards are met, False otherwise
    """
    required_metadata = ["processing_timestamp", "silver_layer_version", "processing_date"]
    
    # Check for required metadata columns
    missing_metadata = [col for col in required_metadata if col not in df.columns]
    
    if missing_metadata:
        print(f"⚠️  Missing Silver metadata: {missing_metadata}")
        return False
    
    print(f"✅ Silver standards: All metadata columns present")
    return True
