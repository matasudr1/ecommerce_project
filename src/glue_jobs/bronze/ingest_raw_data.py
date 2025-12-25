"""
AWS Glue Job: Bronze Layer - Raw Data Ingestion

This script ingests raw CSV data from the landing zone and writes it to the
Bronze layer in Parquet format with minimal transformations.

Bronze Layer Philosophy:
- Store data "as-is" from the source
- Only add metadata (ingestion timestamp, source file)
- Partition by ingestion date for easy reprocessing
- Keep original column names and types
- No business logic or cleaning

Why Bronze?
- Enables reprocessing if business rules change
- Provides data lineage and audit trail
- Supports debugging of downstream issues
- Acts as a "save point" for raw data

Usage in AWS Glue:
    This script is deployed as a Glue job and triggered:
    - On a schedule (e.g., daily at midnight)
    - When new files arrive in S3 (via EventBridge)
    - Manually for backfills
"""

import sys
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import *
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# =============================================================================
# CONFIGURATION
# =============================================================================

# Define the schemas for our source data
# Explicit schemas are better than inference for production jobs:
# - Faster (no need to scan data twice)
# - Predictable (same schema every run)
# - Safer (catches schema changes from source)

CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id", StringType(), False),
    StructField("email", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("country", StringType(), True),
    StructField("city", StringType(), True),
    StructField("address", StringType(), True),
    StructField("created_at", StringType(), True),  # Keep as string, parse in Silver
    StructField("updated_at", StringType(), True),
])

PRODUCTS_SCHEMA = StructType([
    StructField("product_id", StringType(), False),
    StructField("sku", StringType(), True),
    StructField("name", StringType(), True),
    StructField("description", StringType(), True),
    StructField("category", StringType(), True),
    StructField("subcategory", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("price", StringType(), True),  # Keep as string, parse in Silver
    StructField("cost", StringType(), True),
    StructField("stock_quantity", StringType(), True),
    StructField("is_active", StringType(), True),
    StructField("created_at", StringType(), True),
])

ORDERS_SCHEMA = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), True),
    StructField("order_date", StringType(), True),
    StructField("status", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("subtotal", StringType(), True),
    StructField("tax_amount", StringType(), True),
    StructField("shipping_amount", StringType(), True),
    StructField("discount_amount", StringType(), True),
    StructField("total_amount", StringType(), True),
    StructField("currency", StringType(), True),
    StructField("shipping_country", StringType(), True),
    StructField("shipping_city", StringType(), True),
])

ORDER_ITEMS_SCHEMA = StructType([
    StructField("order_item_id", StringType(), False),
    StructField("order_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", StringType(), True),
    StructField("unit_price", StringType(), True),
    StructField("discount_percent", StringType(), True),
    StructField("line_total", StringType(), True),
])

# Map table names to their schemas
SCHEMAS = {
    "customers": CUSTOMERS_SCHEMA,
    "products": PRODUCTS_SCHEMA,
    "orders": ORDERS_SCHEMA,
    "order_items": ORDER_ITEMS_SCHEMA,
}


def add_bronze_metadata(df, source_path: str):
    """
    Add metadata columns to track data lineage.
    
    These columns answer important questions:
    - When was this data ingested? (_ingested_at)
    - Where did it come from? (_source_file)
    - What date partition does it belong to? (_ingestion_date)
    
    Args:
        df: Input DataFrame
        source_path: Path to the source file
        
    Returns:
        DataFrame with metadata columns
    """
    return (df
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_file", F.lit(source_path))
            .withColumn("_ingestion_date", F.current_date()))


def ingest_table(
    glue_context,
    source_bucket: str,
    target_bucket: str,
    table_name: str,
    source_format: str = "csv"
):
    """
    Ingest a single table from raw to bronze layer.
    
    Steps:
    1. Read raw CSV from source location
    2. Apply explicit schema
    3. Add metadata columns
    4. Write to Bronze layer as Parquet
    
    Args:
        glue_context: AWS Glue context
        source_bucket: S3 bucket with raw data
        target_bucket: S3 bucket for bronze data
        table_name: Name of the table to process
        source_format: Format of source files (csv, json, etc.)
    """
    logger.info(f"Starting ingestion for table: {table_name}")
    
    spark = glue_context.spark_session
    
    # Source and target paths
    source_path = f"s3://{source_bucket}/raw/{table_name}/"
    target_path = f"s3://{target_bucket}/bronze/{table_name}/"
    
    # Get the schema for this table
    schema = SCHEMAS.get(table_name)
    if not schema:
        raise ValueError(f"No schema defined for table: {table_name}")
    
    # Read the raw data
    # Using GlueContext for built-in job bookmarks support
    logger.info(f"Reading from: {source_path}")
    
    df = (spark.read
          .format(source_format)
          .option("header", "true")
          .schema(schema)
          .load(source_path))
    
    # Log record count for monitoring
    record_count = df.count()
    logger.info(f"Read {record_count} records from {table_name}")
    
    if record_count == 0:
        logger.warning(f"No records found for {table_name}, skipping")
        return
    
    # Add metadata columns
    df_with_metadata = add_bronze_metadata(df, source_path)
    
    # Write to Bronze layer
    # Partitioned by ingestion date for:
    # - Easy identification of daily loads
    # - Efficient reprocessing of specific dates
    # - Cost optimization (only scan relevant partitions)
    logger.info(f"Writing to: {target_path}")
    
    (df_with_metadata.write
     .mode("append")  # Append to support incremental loads
     .partitionBy("_ingestion_date")
     .parquet(target_path))
    
    logger.info(f"Successfully ingested {table_name}: {record_count} records")


def main():
    """
    Main entry point for the Glue job.
    
    This function:
    1. Parses job arguments
    2. Initializes Glue context
    3. Processes each table
    4. Commits the job
    """
    # Parse arguments passed to the Glue job
    # These are set in the job configuration or at runtime
    args = getResolvedOptions(sys.argv, [
        'JOB_NAME',
        'source_bucket',
        'target_bucket',
        'tables'  # Comma-separated list of tables to process
    ])
    
    # Initialize Glue context
    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args['JOB_NAME'], args)
    
    logger.info(f"Starting job: {args['JOB_NAME']}")
    logger.info(f"Source bucket: {args['source_bucket']}")
    logger.info(f"Target bucket: {args['target_bucket']}")
    
    # Parse table list
    tables = [t.strip() for t in args['tables'].split(',')]
    logger.info(f"Tables to process: {tables}")
    
    # Process each table
    for table_name in tables:
        try:
            ingest_table(
                glue_context=glue_context,
                source_bucket=args['source_bucket'],
                target_bucket=args['target_bucket'],
                table_name=table_name
            )
        except Exception as e:
            logger.error(f"Failed to process {table_name}: {str(e)}")
            raise
    
    # Commit the job
    # This is important for job bookmarks to work correctly
    job.commit()
    logger.info("Job completed successfully")


if __name__ == "__main__":
    main()
