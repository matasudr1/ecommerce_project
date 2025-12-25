"""Utility functions for Spark operations in AWS Glue."""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from typing import Optional, List
import logging

logger = logging.getLogger(__name__)


def get_spark_session(app_name: str = "GlueETL") -> SparkSession:
    """
    Get or create a Spark session.
    
    In AWS Glue, the session is typically created by the GlueContext,
    but for local development, we create our own.
    
    Args:
        app_name: Name for the Spark application
        
    Returns:
        SparkSession instance
    """
    return (SparkSession.builder
            .appName(app_name)
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
            .config("spark.sql.parquet.compression.codec", "snappy")
            .getOrCreate())


def read_csv_with_schema(
    spark: SparkSession,
    path: str,
    schema: Optional[StructType] = None,
    header: bool = True,
    infer_schema: bool = True
) -> DataFrame:
    """
    Read CSV files into a DataFrame.
    
    Args:
        spark: SparkSession
        path: Path to CSV file(s)
        schema: Optional schema to enforce
        header: Whether CSV has header row
        infer_schema: Whether to infer schema if not provided
        
    Returns:
        DataFrame with CSV data
    """
    reader = spark.read.option("header", str(header).lower())
    
    if schema:
        reader = reader.schema(schema)
    else:
        reader = reader.option("inferSchema", str(infer_schema).lower())
    
    return reader.csv(path)


def write_parquet_partitioned(
    df: DataFrame,
    path: str,
    partition_cols: List[str],
    mode: str = "overwrite"
) -> None:
    """
    Write DataFrame to Parquet format with partitioning.
    
    Partitioning is crucial for performance:
    - Allows Athena/Spark to skip irrelevant partitions
    - Enables parallel processing
    - Common partitions: date, year/month, country
    
    Args:
        df: DataFrame to write
        path: Output path (S3 or local)
        partition_cols: Columns to partition by
        mode: Write mode (overwrite, append, etc.)
    """
    (df.write
     .mode(mode)
     .partitionBy(partition_cols)
     .parquet(path))
    
    logger.info(f"Written parquet to {path} with partitions: {partition_cols}")


def add_metadata_columns(df: DataFrame, source_file: str = "unknown") -> DataFrame:
    """
    Add standard metadata columns to a DataFrame.
    
    These columns help with:
    - Data lineage (where did this data come from?)
    - Debugging (when was it processed?)
    - Incremental processing
    
    Args:
        df: Input DataFrame
        source_file: Name/path of source file
        
    Returns:
        DataFrame with metadata columns added
    """
    return (df
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_file", F.lit(source_file))
            .withColumn("_processing_date", F.current_date()))


def deduplicate_by_key(
    df: DataFrame,
    key_columns: List[str],
    order_column: str,
    ascending: bool = False
) -> DataFrame:
    """
    Remove duplicates keeping the most recent record.
    
    This is a common pattern in data engineering:
    - Source systems may send duplicate events
    - We want to keep the latest version of each record
    
    Args:
        df: Input DataFrame with potential duplicates
        key_columns: Columns that define uniqueness
        order_column: Column to determine recency (usually timestamp)
        ascending: If False, keeps the latest (descending order)
        
    Returns:
        Deduplicated DataFrame
    """
    from pyspark.sql.window import Window
    
    window_spec = Window.partitionBy(key_columns).orderBy(
        F.col(order_column).asc() if ascending else F.col(order_column).desc()
    )
    
    return (df
            .withColumn("_row_num", F.row_number().over(window_spec))
            .filter(F.col("_row_num") == 1)
            .drop("_row_num"))


def cast_columns(df: DataFrame, column_types: dict) -> DataFrame:
    """
    Cast multiple columns to specified types.
    
    Args:
        df: Input DataFrame
        column_types: Dictionary mapping column names to types
        
    Returns:
        DataFrame with cast columns
    """
    for col_name, col_type in column_types.items():
        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast(col_type))
    return df


def null_safe_trim(df: DataFrame, columns: List[str]) -> DataFrame:
    """
    Trim whitespace from string columns, handling nulls safely.
    
    Args:
        df: Input DataFrame
        columns: List of column names to trim
        
    Returns:
        DataFrame with trimmed columns
    """
    for col_name in columns:
        if col_name in df.columns:
            df = df.withColumn(col_name, F.trim(F.col(col_name)))
    return df


def validate_not_null(df: DataFrame, columns: List[str]) -> DataFrame:
    """
    Filter out rows with null values in specified columns.
    
    Logs the count of removed rows for monitoring.
    
    Args:
        df: Input DataFrame
        columns: Columns that must not be null
        
    Returns:
        Filtered DataFrame
    """
    original_count = df.count()
    
    condition = F.lit(True)
    for col_name in columns:
        condition = condition & F.col(col_name).isNotNull()
    
    filtered_df = df.filter(condition)
    filtered_count = filtered_df.count()
    
    removed = original_count - filtered_count
    if removed > 0:
        logger.warning(f"Removed {removed} rows with null values in {columns}")
    
    return filtered_df
