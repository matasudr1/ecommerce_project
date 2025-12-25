"""
AWS Glue Job: Gold Layer - Dimension Tables

This script creates the dimension tables for our star schema:
- dim_customer: Customer attributes
- dim_product: Product attributes  
- dim_date: Date/time attributes

Dimension Tables Explained:
- Contain descriptive attributes (the "who, what, where, when")
- Have a surrogate key (integer) for joining to fact tables
- Often implement Slowly Changing Dimensions (SCD) patterns
- Are denormalized for query performance

SCD Types:
- Type 1: Overwrite (lose history)
- Type 2: Add new row (keep history) - most common
- Type 3: Add new column (limited history)

This implementation uses Type 1 for simplicity, but includes
the structure needed for Type 2 (effective dates, is_current flag).
"""

import sys
from datetime import datetime, timedelta
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


def generate_surrogate_key(df, key_column: str, natural_key: str):
    """Generate integer surrogate keys based on natural key ordering."""
    window = Window.orderBy(natural_key)
    return df.withColumn(key_column, F.row_number().over(window))


def build_dim_customer(spark, silver_bucket: str, gold_bucket: str):
    """
    Build the customer dimension table.
    
    Customer dimension contains:
    - Customer identifying information
    - Customer segments/classifications
    - Geographic attributes
    
    This is essential for:
    - Analyzing customer behavior
    - Customer segmentation
    - Geographic analysis
    """
    logger.info("Building dim_customer...")
    
    # Read silver layer
    customers = spark.read.parquet(f"s3://{silver_bucket}/silver/customers/")
    orders = spark.read.parquet(f"s3://{silver_bucket}/silver/orders/")
    
    # Calculate customer metrics for segmentation
    customer_metrics = (orders
                        .groupBy("customer_id")
                        .agg(
                            F.count("order_id").alias("total_orders"),
                            F.sum("total_amount").alias("total_spend"),
                            F.min("order_date").alias("first_order_date"),
                            F.max("order_date").alias("last_order_date"),
                            F.avg("total_amount").alias("avg_order_value")
                        ))
    
    # Join customer info with metrics
    dim_customer = (customers
                    .join(customer_metrics, "customer_id", "left")
                    
                    # Calculate customer lifetime value tier
                    .withColumn(
                        "value_tier",
                        F.when(F.col("total_spend") >= 1000, "platinum")
                         .when(F.col("total_spend") >= 500, "gold")
                         .when(F.col("total_spend") >= 100, "silver")
                         .otherwise("bronze")
                    )
                    
                    # Days since last order (for churn analysis)
                    .withColumn(
                        "days_since_last_order",
                        F.datediff(F.current_date(), F.col("last_order_date"))
                    )
                    
                    # Customer status
                    .withColumn(
                        "customer_status",
                        F.when(F.col("total_orders").isNull(), "prospect")
                         .when(F.col("days_since_last_order") > 365, "churned")
                         .when(F.col("days_since_last_order") > 90, "at_risk")
                         .otherwise("active")
                    )
                    
                    # SCD Type 2 columns (for future use)
                    .withColumn("effective_from", F.col("created_at"))
                    .withColumn("effective_to", F.lit(None).cast(TimestampType()))
                    .withColumn("is_current", F.lit(True))
                    
                    .withColumn("_created_at", F.current_timestamp()))
    
    # Generate surrogate key
    dim_customer = generate_surrogate_key(dim_customer, "customer_key", "customer_id")
    
    # Select final columns
    dim_customer_final = dim_customer.select(
        # Keys
        "customer_key",
        "customer_id",
        
        # Attributes
        "email",
        "full_name",
        "first_name",
        "last_name",
        "phone",
        "country",
        "city",
        
        # Derived attributes
        "segment",
        "value_tier",
        "customer_status",
        "is_valid_email",
        
        # Metrics
        F.coalesce("total_orders", F.lit(0)).alias("total_orders"),
        F.coalesce("total_spend", F.lit(0.0)).alias("total_spend"),
        "first_order_date",
        "last_order_date",
        "avg_order_value",
        "days_since_last_order",
        
        # SCD columns
        "effective_from",
        "effective_to",
        "is_current",
        
        # Metadata
        "_created_at"
    )
    
    output_path = f"s3://{gold_bucket}/gold/dim_customer/"
    
    (dim_customer_final.write
     .mode("overwrite")
     .parquet(output_path))
    
    logger.info(f"Created dim_customer with {dim_customer_final.count()} records")


def build_dim_product(spark, silver_bucket: str, gold_bucket: str):
    """
    Build the product dimension table.
    
    Product dimension contains:
    - Product identifying information
    - Category hierarchy
    - Pricing information
    - Performance metrics
    """
    logger.info("Building dim_product...")
    
    products = spark.read.parquet(f"s3://{silver_bucket}/silver/products/")
    
    dim_product = (products
                   # Add price tier classification
                   .withColumn(
                       "price_tier",
                       F.when(F.col("price") >= 500, "premium")
                        .when(F.col("price") >= 100, "mid_range")
                        .when(F.col("price") >= 25, "budget")
                        .otherwise("economy")
                   )
                   
                   # Flag high margin products
                   .withColumn(
                       "is_high_margin",
                       F.col("margin_percent") >= 40
                   )
                   
                   # Stock status
                   .withColumn(
                       "stock_status",
                       F.when(F.col("stock_quantity") == 0, "out_of_stock")
                        .when(F.col("stock_quantity") < 10, "low_stock")
                        .when(F.col("stock_quantity") < 50, "normal")
                        .otherwise("well_stocked")
                   )
                   
                   # SCD columns
                   .withColumn("effective_from", F.col("created_at"))
                   .withColumn("effective_to", F.lit(None).cast(TimestampType()))
                   .withColumn("is_current", F.lit(True))
                   
                   .withColumn("_created_at", F.current_timestamp()))
    
    # Generate surrogate key
    dim_product = generate_surrogate_key(dim_product, "product_key", "product_id")
    
    dim_product_final = dim_product.select(
        # Keys
        "product_key",
        "product_id",
        "sku",
        
        # Attributes
        "name",
        "description",
        "brand",
        
        # Category hierarchy
        "category",
        "subcategory",
        
        # Pricing
        "price",
        "cost",
        "margin_percent",
        "price_tier",
        "is_high_margin",
        
        # Inventory
        "stock_quantity",
        "stock_status",
        "is_active",
        
        # SCD columns
        "effective_from",
        "effective_to",
        "is_current",
        
        # Metadata
        "_created_at"
    )
    
    output_path = f"s3://{gold_bucket}/gold/dim_product/"
    
    (dim_product_final.write
     .mode("overwrite")
     .parquet(output_path))
    
    logger.info(f"Created dim_product with {dim_product_final.count()} records")


def build_dim_date(spark, gold_bucket: str, start_year: int = 2020, end_year: int = 2030):
    """
    Build the date dimension table.
    
    Date dimensions are special:
    - Pre-populated with all dates in a range
    - Include many derived attributes for analysis
    - Date key is usually YYYYMMDD format
    
    Common uses:
    - Time-based aggregations
    - Holiday/weekend analysis
    - Year-over-year comparisons
    """
    logger.info(f"Building dim_date for years {start_year} to {end_year}...")
    
    # Generate date range
    start_date = datetime(start_year, 1, 1)
    end_date = datetime(end_year, 12, 31)
    num_days = (end_date - start_date).days + 1
    
    # Create date sequence
    date_data = [(start_date + timedelta(days=i),) for i in range(num_days)]
    
    schema = StructType([StructField("date", DateType(), False)])
    dates_df = spark.createDataFrame(date_data, schema)
    
    # Add all date attributes
    dim_date = (dates_df
                # Date key (YYYYMMDD)
                .withColumn(
                    "date_key",
                    (F.year("date") * 10000 + 
                     F.month("date") * 100 + 
                     F.dayofmonth("date")).cast(IntegerType())
                )
                
                # Basic components
                .withColumn("day_of_month", F.dayofmonth("date"))
                .withColumn("day_of_week", F.dayofweek("date"))  # 1=Sunday
                .withColumn("day_of_year", F.dayofyear("date"))
                .withColumn("week_of_year", F.weekofyear("date"))
                .withColumn("month", F.month("date"))
                .withColumn("quarter", F.quarter("date"))
                .withColumn("year", F.year("date"))
                
                # Day names
                .withColumn(
                    "day_name",
                    F.date_format("date", "EEEE")  # Monday, Tuesday, etc.
                )
                .withColumn(
                    "day_name_short",
                    F.date_format("date", "EEE")  # Mon, Tue, etc.
                )
                
                # Month names
                .withColumn(
                    "month_name",
                    F.date_format("date", "MMMM")  # January, February, etc.
                )
                .withColumn(
                    "month_name_short",
                    F.date_format("date", "MMM")  # Jan, Feb, etc.
                )
                
                # Period labels
                .withColumn(
                    "year_month",
                    F.date_format("date", "yyyy-MM")
                )
                .withColumn(
                    "year_quarter",
                    F.concat(F.year("date"), F.lit("-Q"), F.quarter("date"))
                )
                .withColumn(
                    "year_week",
                    F.concat(F.year("date"), F.lit("-W"), 
                             F.lpad(F.weekofyear("date"), 2, "0"))
                )
                
                # Flags
                .withColumn(
                    "is_weekend",
                    F.dayofweek("date").isin(1, 7)  # Sunday=1, Saturday=7
                )
                .withColumn(
                    "is_weekday",
                    ~F.dayofweek("date").isin(1, 7)
                )
                .withColumn(
                    "is_month_start",
                    F.dayofmonth("date") == 1
                )
                .withColumn(
                    "is_month_end",
                    F.dayofmonth("date") == F.dayofmonth(F.last_day("date"))
                )
                .withColumn(
                    "is_quarter_start",
                    (F.month("date").isin(1, 4, 7, 10)) & (F.dayofmonth("date") == 1)
                )
                .withColumn(
                    "is_quarter_end",
                    (F.month("date").isin(3, 6, 9, 12)) & 
                    (F.dayofmonth("date") == F.dayofmonth(F.last_day("date")))
                )
                .withColumn(
                    "is_year_start",
                    (F.month("date") == 1) & (F.dayofmonth("date") == 1)
                )
                .withColumn(
                    "is_year_end",
                    (F.month("date") == 12) & (F.dayofmonth("date") == 31)
                )
                
                # Fiscal periods (assuming fiscal year = calendar year)
                # Adjust these if your business has different fiscal year
                .withColumn("fiscal_year", F.year("date"))
                .withColumn("fiscal_quarter", F.quarter("date"))
                
                # Metadata
                .withColumn("_created_at", F.current_timestamp()))
    
    output_path = f"s3://{gold_bucket}/gold/dim_date/"
    
    (dim_date.write
     .mode("overwrite")
     .parquet(output_path))
    
    logger.info(f"Created dim_date with {dim_date.count()} records")


def main():
    """Main entry point for dimension tables job."""
    
    args = getResolvedOptions(sys.argv, [
        'JOB_NAME',
        'silver_bucket',
        'gold_bucket'
    ])
    
    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args['JOB_NAME'], args)
    
    logger.info("Starting Gold layer - Dimension Tables job")
    
    # Build dimension tables
    # Date dimension first (no dependencies)
    build_dim_date(spark, args['gold_bucket'])
    
    # Product dimension
    build_dim_product(spark, args['silver_bucket'], args['gold_bucket'])
    
    # Customer dimension (depends on orders for metrics)
    build_dim_customer(spark, args['silver_bucket'], args['gold_bucket'])
    
    job.commit()
    logger.info("Gold layer - Dimension Tables job completed")


if __name__ == "__main__":
    main()
