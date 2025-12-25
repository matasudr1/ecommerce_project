"""
AWS Glue Job: Gold Layer - Fact Sales Table

This script creates the central fact table for our star schema.
The fact_sales table contains all the measures (quantitative data)
and foreign keys to dimension tables.

Star Schema Overview:
- Fact tables contain "facts" (measures like revenue, quantity)
- Dimension tables contain descriptive attributes
- Facts reference dimensions via foreign keys
- This design optimizes for analytical queries

Why Star Schema?
- Simple, intuitive structure for analysts
- Excellent query performance
- Easy to understand relationships
- Works well with BI tools
"""

import sys
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


def generate_surrogate_keys(df, key_column: str, partition_column: str = None):
    """
    Generate surrogate keys for a dimension or fact table.
    
    Surrogate keys are artificial keys (integers) that:
    - Are independent of source system keys
    - Never change (unlike natural keys)
    - Are more efficient for joins
    - Support slowly changing dimensions
    
    Args:
        df: Input DataFrame
        key_column: Name for the new surrogate key column
        partition_column: Optional column to partition by for ordering
        
    Returns:
        DataFrame with surrogate key column
    """
    if partition_column:
        window = Window.orderBy(partition_column)
    else:
        window = Window.orderBy(F.monotonically_increasing_id())
    
    return df.withColumn(key_column, F.row_number().over(window))


def create_date_key(date_column):
    """
    Create a date key in YYYYMMDD format.
    
    Date keys link fact tables to the date dimension.
    Format: 20231215 for December 15, 2023
    
    Args:
        date_column: Column containing the date
        
    Returns:
        Column expression for the date key
    """
    return (
        F.year(date_column) * 10000 +
        F.month(date_column) * 100 +
        F.dayofmonth(date_column)
    ).cast(IntegerType())


def build_fact_sales(spark, silver_bucket: str, gold_bucket: str):
    """
    Build the fact_sales table from silver layer data.
    
    The fact_sales table:
    - Grain: One row per order item (most detailed)
    - Measures: quantity, revenue, cost, profit
    - Dimensions: date, customer, product
    
    This is the central table for sales analytics.
    
    Args:
        spark: SparkSession
        silver_bucket: Bucket containing silver layer data
        gold_bucket: Bucket for gold layer output
    """
    logger.info("Building fact_sales table...")
    
    # Read silver layer tables
    orders = spark.read.parquet(f"s3://{silver_bucket}/silver/orders/")
    order_items = spark.read.parquet(f"s3://{silver_bucket}/silver/order_items/")
    products = spark.read.parquet(f"s3://{silver_bucket}/silver/products/")
    customers = spark.read.parquet(f"s3://{silver_bucket}/silver/customers/")
    
    logger.info(f"Loaded orders: {orders.count()}")
    logger.info(f"Loaded order_items: {order_items.count()}")
    
    # First, we need dimension tables with surrogate keys
    # In a full implementation, these would be pre-built
    # For now, we'll create simple lookups
    
    # Customer dimension lookup
    dim_customer = (customers
                    .select("customer_id")
                    .distinct())
    dim_customer = generate_surrogate_keys(dim_customer, "customer_key", "customer_id")
    
    # Product dimension lookup
    dim_product = (products
                   .select("product_id", "cost")
                   .distinct())
    dim_product = generate_surrogate_keys(dim_product, "product_key", "product_id")
    
    # Join order_items with orders to get order-level info
    fact_base = (order_items
                 .join(orders, "order_id", "inner")
                 .join(dim_customer, "customer_id", "inner")
                 .join(dim_product, "product_id", "inner"))
    
    # Calculate measures
    fact_sales = (fact_base
                  # Create date key from order date
                  .withColumn("date_key", create_date_key(F.col("order_date")))
                  
                  # Measures
                  .withColumn(
                      "gross_revenue",
                      F.round(F.col("quantity") * F.col("unit_price"), 2)
                  )
                  .withColumn(
                      "net_revenue",
                      F.round(F.col("line_total"), 2)
                  )
                  .withColumn(
                      "cost_of_goods",
                      F.round(F.col("quantity") * F.col("cost"), 2)
                  )
                  .withColumn(
                      "profit",
                      F.round(F.col("net_revenue") - F.col("cost_of_goods"), 2)
                  )
                  .withColumn(
                      "profit_margin_pct",
                      F.when(F.col("net_revenue") > 0,
                             F.round(F.col("profit") / F.col("net_revenue") * 100, 2))
                       .otherwise(0.0)
                  )
                  
                  # Allocate order-level amounts to line items proportionally
                  .withColumn(
                      "allocated_tax",
                      F.round(
                          F.col("tax_amount") * 
                          F.col("line_total") / F.col("subtotal"), 2
                      )
                  )
                  .withColumn(
                      "allocated_shipping",
                      F.round(
                          F.col("shipping_amount") * 
                          F.col("line_total") / F.col("subtotal"), 2
                      )
                  ))
    
    # Generate surrogate key for fact table
    fact_sales = generate_surrogate_keys(fact_sales, "sale_key", "order_item_id")
    
    # Select final columns
    fact_sales_final = fact_sales.select(
        # Surrogate key
        "sale_key",
        
        # Foreign keys to dimensions
        "date_key",
        "customer_key",
        "product_key",
        
        # Degenerate dimensions (stored in fact, no separate dim table)
        "order_id",
        "order_item_id",
        
        # Order attributes
        "order_date",
        "status",
        "payment_method",
        "shipping_country",
        
        # Measures
        "quantity",
        "unit_price",
        "discount_percent",
        "gross_revenue",
        "discount_amount",
        "net_revenue",
        "cost_of_goods",
        "profit",
        "profit_margin_pct",
        "allocated_tax",
        "allocated_shipping",
        
        # Metadata
        F.current_timestamp().alias("_created_at")
    )
    
    # Write fact table
    # Partition by date components for query efficiency
    output_path = f"s3://{gold_bucket}/gold/fact_sales/"
    
    (fact_sales_final.write
     .mode("overwrite")
     .partitionBy("date_key")
     .parquet(output_path))
    
    record_count = fact_sales_final.count()
    logger.info(f"Created fact_sales with {record_count} records")
    
    return fact_sales_final


def build_daily_sales_summary(spark, gold_bucket: str):
    """
    Build a daily sales summary aggregate table.
    
    This is a pre-aggregated table for common queries:
    - Daily revenue trends
    - Daily order counts
    - Average order value
    
    Pre-aggregation improves query performance significantly.
    """
    logger.info("Building daily sales summary...")
    
    fact_sales = spark.read.parquet(f"s3://{gold_bucket}/gold/fact_sales/")
    
    daily_summary = (fact_sales
                     .groupBy("date_key", "shipping_country")
                     .agg(
                         F.countDistinct("order_id").alias("total_orders"),
                         F.sum("quantity").alias("total_items_sold"),
                         F.sum("gross_revenue").alias("gross_revenue"),
                         F.sum("net_revenue").alias("net_revenue"),
                         F.sum("profit").alias("total_profit"),
                         F.avg("profit_margin_pct").alias("avg_profit_margin"),
                         F.countDistinct("customer_key").alias("unique_customers")
                     )
                     .withColumn(
                         "avg_order_value",
                         F.round(F.col("net_revenue") / F.col("total_orders"), 2)
                     )
                     .withColumn(
                         "items_per_order",
                         F.round(F.col("total_items_sold") / F.col("total_orders"), 2)
                     )
                     .withColumn("_created_at", F.current_timestamp()))
    
    output_path = f"s3://{gold_bucket}/gold/agg_daily_sales/"
    
    (daily_summary.write
     .mode("overwrite")
     .parquet(output_path))
    
    logger.info(f"Created daily sales summary with {daily_summary.count()} records")


def build_product_performance(spark, silver_bucket: str, gold_bucket: str):
    """
    Build product performance metrics.
    
    Useful for:
    - Identifying top sellers
    - Understanding category performance
    - Inventory planning
    """
    logger.info("Building product performance metrics...")
    
    fact_sales = spark.read.parquet(f"s3://{gold_bucket}/gold/fact_sales/")
    products = spark.read.parquet(f"s3://{silver_bucket}/silver/products/")
    
    product_metrics = (fact_sales
                       .join(products.select(
                           "product_id", "name", "category", 
                           "subcategory", "brand"
                       ), "product_id", "inner")
                       .groupBy(
                           "product_id", "name", "category", 
                           "subcategory", "brand"
                       )
                       .agg(
                           F.sum("quantity").alias("total_quantity_sold"),
                           F.sum("net_revenue").alias("total_revenue"),
                           F.sum("profit").alias("total_profit"),
                           F.countDistinct("order_id").alias("number_of_orders"),
                           F.countDistinct("customer_key").alias("unique_customers"),
                           F.avg("unit_price").alias("avg_selling_price"),
                           F.avg("discount_percent").alias("avg_discount_pct")
                       )
                       .withColumn(
                           "profit_margin_pct",
                           F.when(F.col("total_revenue") > 0,
                                  F.round(F.col("total_profit") / 
                                          F.col("total_revenue") * 100, 2))
                            .otherwise(0.0)
                       )
                       .withColumn("_created_at", F.current_timestamp()))
    
    # Rank products by revenue within category
    window = Window.partitionBy("category").orderBy(F.col("total_revenue").desc())
    product_metrics = product_metrics.withColumn(
        "revenue_rank_in_category",
        F.row_number().over(window)
    )
    
    output_path = f"s3://{gold_bucket}/gold/agg_product_performance/"
    
    (product_metrics.write
     .mode("overwrite")
     .parquet(output_path))
    
    logger.info(f"Created product performance with {product_metrics.count()} records")


def main():
    """Main entry point for the fact sales Gold layer job."""
    
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
    
    logger.info(f"Starting Gold layer - Fact Sales job")
    
    # Build fact table
    build_fact_sales(spark, args['silver_bucket'], args['gold_bucket'])
    
    # Build aggregate tables
    build_daily_sales_summary(spark, args['gold_bucket'])
    build_product_performance(spark, args['silver_bucket'], args['gold_bucket'])
    
    job.commit()
    logger.info("Gold layer - Fact Sales job completed")


if __name__ == "__main__":
    main()
