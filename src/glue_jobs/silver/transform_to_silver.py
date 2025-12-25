"""
AWS Glue Job: Silver Layer - Data Transformation and Cleaning

This script transforms data from the Bronze layer to the Silver layer by:
- Cleaning and validating data
- Casting to correct data types
- Handling null values
- Deduplicating records
- Standardizing formats
- Adding derived columns

Silver Layer Philosophy:
- Clean, validated, "single source of truth" data
- Correct data types (dates, numbers, etc.)
- Business-friendly column names
- Deduplicated records
- Null values handled appropriately
- Ready for analysis and reporting

The Silver layer is where most data quality issues are resolved.
"""

import sys
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# =============================================================================
# TRANSFORMATION FUNCTIONS
# =============================================================================

def transform_customers(df):
    """
    Transform customers from Bronze to Silver.
    
    Transformations:
    1. Parse timestamps to proper datetime
    2. Validate and clean email addresses
    3. Standardize country codes
    4. Create derived columns (full_name, email_domain)
    5. Deduplicate by customer_id (keep latest)
    6. Add customer segment based on creation date
    
    Args:
        df: Bronze customer DataFrame
        
    Returns:
        Cleaned Silver customer DataFrame
    """
    logger.info("Transforming customers...")
    
    # Step 1: Parse timestamps
    df = df.withColumn(
        "created_at",
        F.to_timestamp(F.col("created_at"))
    ).withColumn(
        "updated_at", 
        F.to_timestamp(F.col("updated_at"))
    )
    
    # Step 2: Clean and validate emails
    # Extract email domain and validate format
    df = df.withColumn(
        "email",
        F.lower(F.trim(F.col("email")))
    ).withColumn(
        "email_domain",
        F.regexp_extract(F.col("email"), r"@(.+)$", 1)
    ).withColumn(
        "is_valid_email",
        F.col("email").rlike(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
    )
    
    # Step 3: Standardize country codes (uppercase)
    df = df.withColumn(
        "country",
        F.upper(F.trim(F.col("country")))
    )
    
    # Step 4: Create full name
    df = df.withColumn(
        "full_name",
        F.concat_ws(" ", 
                    F.initcap(F.trim(F.col("first_name"))),
                    F.initcap(F.trim(F.col("last_name"))))
    )
    
    # Step 5: Trim string columns
    string_columns = ["first_name", "last_name", "city", "address", "phone"]
    for col_name in string_columns:
        df = df.withColumn(col_name, F.trim(F.col(col_name)))
    
    # Step 6: Deduplicate - keep the most recent record per customer_id
    window = Window.partitionBy("customer_id").orderBy(F.col("_ingested_at").desc())
    df = df.withColumn("_row_num", F.row_number().over(window))
    df = df.filter(F.col("_row_num") == 1).drop("_row_num")
    
    # Step 7: Calculate customer segment based on account age
    df = df.withColumn(
        "account_age_days",
        F.datediff(F.current_date(), F.col("created_at"))
    ).withColumn(
        "segment",
        F.when(F.col("account_age_days") < 30, "new")
         .when(F.col("account_age_days") < 365, "regular")
         .otherwise("established")
    )
    
    # Step 8: Add processing metadata
    df = df.withColumn("_processed_at", F.current_timestamp())
    
    # Step 9: Select final columns in order
    return df.select(
        "customer_id",
        "email",
        "email_domain",
        "is_valid_email",
        "first_name",
        "last_name",
        "full_name",
        "phone",
        "country",
        "city",
        "address",
        "created_at",
        "updated_at",
        "account_age_days",
        "segment",
        "_source_file",
        "_ingested_at",
        "_processed_at"
    )


def transform_products(df):
    """
    Transform products from Bronze to Silver.
    
    Transformations:
    1. Cast numeric fields to correct types
    2. Calculate profit margin
    3. Standardize category values
    4. Handle null descriptions
    5. Parse boolean fields
    
    Args:
        df: Bronze product DataFrame
        
    Returns:
        Cleaned Silver product DataFrame
    """
    logger.info("Transforming products...")
    
    # Step 1: Cast numeric fields
    df = (df
          .withColumn("price", F.col("price").cast(DoubleType()))
          .withColumn("cost", F.col("cost").cast(DoubleType()))
          .withColumn("stock_quantity", F.col("stock_quantity").cast(IntegerType())))
    
    # Step 2: Parse boolean field
    df = df.withColumn(
        "is_active",
        F.when(F.lower(F.col("is_active")).isin("true", "1", "yes"), True)
         .otherwise(False)
    )
    
    # Step 3: Parse timestamp
    df = df.withColumn(
        "created_at",
        F.to_timestamp(F.col("created_at"))
    )
    
    # Step 4: Standardize category (lowercase, trim)
    df = df.withColumn(
        "category",
        F.lower(F.trim(F.col("category")))
    ).withColumn(
        "subcategory",
        F.trim(F.col("subcategory"))
    )
    
    # Step 5: Calculate profit margin percentage
    # margin = (price - cost) / price * 100
    df = df.withColumn(
        "margin_percent",
        F.when(F.col("price") > 0,
               F.round((F.col("price") - F.col("cost")) / F.col("price") * 100, 2))
         .otherwise(0.0)
    )
    
    # Step 6: Handle null descriptions
    df = df.withColumn(
        "description",
        F.coalesce(F.col("description"), F.lit("No description available"))
    )
    
    # Step 7: Clean string fields
    df = (df
          .withColumn("name", F.trim(F.col("name")))
          .withColumn("sku", F.upper(F.trim(F.col("sku"))))
          .withColumn("brand", F.trim(F.col("brand"))))
    
    # Step 8: Deduplicate by product_id
    window = Window.partitionBy("product_id").orderBy(F.col("_ingested_at").desc())
    df = df.withColumn("_row_num", F.row_number().over(window))
    df = df.filter(F.col("_row_num") == 1).drop("_row_num")
    
    # Step 9: Add processing metadata
    df = df.withColumn("_processed_at", F.current_timestamp())
    
    return df.select(
        "product_id",
        "sku",
        "name",
        "description",
        "category",
        "subcategory",
        "brand",
        "price",
        "cost",
        "margin_percent",
        "stock_quantity",
        "is_active",
        "created_at",
        "_source_file",
        "_ingested_at",
        "_processed_at"
    )


def transform_orders(df):
    """
    Transform orders from Bronze to Silver.
    
    Transformations:
    1. Cast numeric fields
    2. Parse order date
    3. Extract date components for analysis
    4. Validate totals
    5. Standardize status values
    
    Args:
        df: Bronze orders DataFrame
        
    Returns:
        Cleaned Silver orders DataFrame
    """
    logger.info("Transforming orders...")
    
    # Step 1: Cast numeric fields
    numeric_columns = ["subtotal", "tax_amount", "shipping_amount", 
                       "discount_amount", "total_amount"]
    for col_name in numeric_columns:
        df = df.withColumn(col_name, F.col(col_name).cast(DoubleType()))
    
    # Step 2: Parse order date
    df = df.withColumn(
        "order_date",
        F.to_timestamp(F.col("order_date"))
    )
    
    # Step 3: Extract date components for easier analysis
    df = (df
          .withColumn("order_year", F.year(F.col("order_date")))
          .withColumn("order_month", F.month(F.col("order_date")))
          .withColumn("order_day", F.dayofmonth(F.col("order_date")))
          .withColumn("order_day_of_week", F.dayofweek(F.col("order_date")))
          .withColumn("order_week", F.weekofyear(F.col("order_date"))))
    
    # Step 4: Standardize status (lowercase)
    df = df.withColumn(
        "status",
        F.lower(F.trim(F.col("status")))
    )
    
    # Step 5: Standardize payment method
    df = df.withColumn(
        "payment_method",
        F.lower(F.trim(F.col("payment_method")))
    )
    
    # Step 6: Validate total amount
    # Flag orders where calculated total doesn't match
    df = df.withColumn(
        "calculated_total",
        F.col("subtotal") + F.col("tax_amount") + 
        F.col("shipping_amount") - F.col("discount_amount")
    ).withColumn(
        "is_total_valid",
        F.abs(F.col("total_amount") - F.col("calculated_total")) < 0.01
    )
    
    # Step 7: Standardize country codes
    df = df.withColumn(
        "shipping_country",
        F.upper(F.trim(F.col("shipping_country")))
    )
    
    # Step 8: Deduplicate by order_id
    window = Window.partitionBy("order_id").orderBy(F.col("_ingested_at").desc())
    df = df.withColumn("_row_num", F.row_number().over(window))
    df = df.filter(F.col("_row_num") == 1).drop("_row_num")
    
    # Step 9: Add processing metadata
    df = df.withColumn("_processed_at", F.current_timestamp())
    
    return df.select(
        "order_id",
        "customer_id",
        "order_date",
        "order_year",
        "order_month",
        "order_day",
        "order_day_of_week",
        "order_week",
        "status",
        "payment_method",
        "subtotal",
        "tax_amount",
        "shipping_amount",
        "discount_amount",
        "total_amount",
        "calculated_total",
        "is_total_valid",
        "currency",
        "shipping_country",
        "shipping_city",
        "_source_file",
        "_ingested_at",
        "_processed_at"
    )


def transform_order_items(df):
    """
    Transform order items from Bronze to Silver.
    
    Transformations:
    1. Cast numeric fields
    2. Calculate gross and net amounts
    3. Validate line totals
    
    Args:
        df: Bronze order items DataFrame
        
    Returns:
        Cleaned Silver order items DataFrame
    """
    logger.info("Transforming order items...")
    
    # Step 1: Cast numeric fields
    df = (df
          .withColumn("quantity", F.col("quantity").cast(IntegerType()))
          .withColumn("unit_price", F.col("unit_price").cast(DoubleType()))
          .withColumn("discount_percent", F.col("discount_percent").cast(DoubleType()))
          .withColumn("line_total", F.col("line_total").cast(DoubleType())))
    
    # Step 2: Calculate gross amount (before discount)
    df = df.withColumn(
        "gross_amount",
        F.round(F.col("quantity") * F.col("unit_price"), 2)
    )
    
    # Step 3: Calculate discount amount
    df = df.withColumn(
        "discount_amount",
        F.round(F.col("gross_amount") * F.col("discount_percent") / 100, 2)
    )
    
    # Step 4: Validate line total
    df = df.withColumn(
        "calculated_line_total",
        F.round(F.col("gross_amount") - F.col("discount_amount"), 2)
    ).withColumn(
        "is_line_total_valid",
        F.abs(F.col("line_total") - F.col("calculated_line_total")) < 0.01
    )
    
    # Step 5: Deduplicate by order_item_id
    window = Window.partitionBy("order_item_id").orderBy(F.col("_ingested_at").desc())
    df = df.withColumn("_row_num", F.row_number().over(window))
    df = df.filter(F.col("_row_num") == 1).drop("_row_num")
    
    # Step 6: Add processing metadata
    df = df.withColumn("_processed_at", F.current_timestamp())
    
    return df.select(
        "order_item_id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        "gross_amount",
        "discount_percent",
        "discount_amount",
        "line_total",
        "calculated_line_total",
        "is_line_total_valid",
        "_source_file",
        "_ingested_at",
        "_processed_at"
    )


# Map table names to their transformation functions
TRANSFORMATIONS = {
    "customers": transform_customers,
    "products": transform_products,
    "orders": transform_orders,
    "order_items": transform_order_items,
}


def process_table(
    spark,
    source_bucket: str,
    target_bucket: str,
    table_name: str,
    processing_date: str = None
):
    """
    Process a single table from Bronze to Silver.
    
    Args:
        spark: SparkSession
        source_bucket: S3 bucket with bronze data
        target_bucket: S3 bucket for silver data
        table_name: Name of the table to process
        processing_date: Optional date to process (YYYY-MM-DD)
    """
    logger.info(f"Processing table: {table_name}")
    
    # Source and target paths
    source_path = f"s3://{source_bucket}/bronze/{table_name}/"
    target_path = f"s3://{target_bucket}/silver/{table_name}/"
    
    # Read bronze data
    # If processing_date specified, filter to that partition
    if processing_date:
        source_path = f"{source_path}_ingestion_date={processing_date}/"
    
    logger.info(f"Reading from: {source_path}")
    df = spark.read.parquet(source_path)
    
    record_count = df.count()
    logger.info(f"Read {record_count} records from bronze")
    
    if record_count == 0:
        logger.warning(f"No records to process for {table_name}")
        return
    
    # Get the transformation function for this table
    transform_fn = TRANSFORMATIONS.get(table_name)
    if not transform_fn:
        raise ValueError(f"No transformation defined for table: {table_name}")
    
    # Apply transformations
    df_silver = transform_fn(df)
    
    # Write to Silver layer
    # Partition by processing date for efficient queries
    logger.info(f"Writing to: {target_path}")
    
    (df_silver.write
     .mode("overwrite")
     .parquet(target_path))
    
    silver_count = df_silver.count()
    logger.info(f"Successfully transformed {table_name}: {silver_count} records")


def main():
    """Main entry point for the Silver transformation job."""
    
    args = getResolvedOptions(sys.argv, [
        'JOB_NAME',
        'source_bucket',
        'target_bucket',
        'tables',
        'processing_date'
    ])
    
    # Initialize Glue context
    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args['JOB_NAME'], args)
    
    logger.info(f"Starting Silver transformation job: {args['JOB_NAME']}")
    
    # Parse table list
    tables = [t.strip() for t in args['tables'].split(',')]
    processing_date = args.get('processing_date')
    
    # Process each table
    for table_name in tables:
        try:
            process_table(
                spark=spark,
                source_bucket=args['source_bucket'],
                target_bucket=args['target_bucket'],
                table_name=table_name,
                processing_date=processing_date
            )
        except Exception as e:
            logger.error(f"Failed to process {table_name}: {str(e)}")
            raise
    
    job.commit()
    logger.info("Silver transformation job completed successfully")


if __name__ == "__main__":
    main()
