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
