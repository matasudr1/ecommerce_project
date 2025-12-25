"""
Run ETL Pipeline Locally

This script allows you to run the ETL pipeline on your local machine
without needing AWS Glue. It's useful for:
- Development and testing
- Debugging transformations
- Quick iterations before deploying to AWS

Usage:
    python scripts/run_local.py --layer bronze --input data/raw --output data/output
    python scripts/run_local.py --layer silver --input data/output/bronze --output data/output
    python scripts/run_local.py --layer gold --input data/output/silver --output data/output
    python scripts/run_local.py --all --input data/raw --output data/output
"""

import os
import sys
import logging
import argparse
from pathlib import Path
from datetime import datetime

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_spark_session() -> SparkSession:
    """Create a local Spark session for development."""
    return (SparkSession.builder
            .appName("EcommerceETL-Local")
            .master("local[*]")  # Use all available cores
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
            .config("spark.sql.parquet.compression.codec", "snappy")
            .config("spark.driver.memory", "4g")
            .config("spark.sql.shuffle.partitions", "8")
            .getOrCreate())


def run_bronze_layer(spark: SparkSession, input_path: str, output_path: str):
    """
    Run Bronze layer ingestion locally.
    
    Args:
        spark: SparkSession
        input_path: Path to raw CSV files
        output_path: Path for bronze output
    """
    logger.info("=" * 60)
    logger.info("BRONZE LAYER - Raw Data Ingestion")
    logger.info("=" * 60)
    
    tables = ["customers", "products", "orders", "order_items"]
    
    for table in tables:
        csv_path = os.path.join(input_path, f"{table}.csv")
        
        if not os.path.exists(csv_path):
            logger.warning(f"File not found: {csv_path}, skipping")
            continue
        
        logger.info(f"Processing {table}...")
        
        # Read CSV
        df = spark.read.option("header", "true").csv(csv_path)
        
        # Add metadata columns
        df = (df
              .withColumn("_ingested_at", F.current_timestamp())
              .withColumn("_source_file", F.lit(csv_path))
              .withColumn("_ingestion_date", F.current_date()))
        
        # Write to bronze layer
        bronze_path = os.path.join(output_path, "bronze", table)
        
        (df.write
         .mode("overwrite")
         .partitionBy("_ingestion_date")
         .parquet(bronze_path))
        
        logger.info(f"  ✅ Wrote {df.count()} records to {bronze_path}")
    
    logger.info("Bronze layer complete!")


def run_silver_layer(spark: SparkSession, input_path: str, output_path: str):
    """
    Run Silver layer transformations locally.
    
    This imports and runs the transformation logic from the Glue scripts.
    """
    logger.info("=" * 60)
    logger.info("SILVER LAYER - Data Transformation")
    logger.info("=" * 60)
    
    # Import transformation functions
    from src.glue_jobs.silver.transform_to_silver import (
        transform_customers,
        transform_products,
        transform_orders,
        transform_order_items
    )
    
    transformations = {
        "customers": transform_customers,
        "products": transform_products,
        "orders": transform_orders,
        "order_items": transform_order_items,
    }
    
    for table, transform_fn in transformations.items():
        bronze_path = os.path.join(input_path, "bronze", table)
        
        if not os.path.exists(bronze_path):
            logger.warning(f"Bronze data not found: {bronze_path}, skipping")
            continue
        
        logger.info(f"Transforming {table}...")
        
        # Read bronze data
        df = spark.read.parquet(bronze_path)
        logger.info(f"  Read {df.count()} records from bronze")
        
        # Apply transformation
        df_silver = transform_fn(df)
        
        # Write to silver layer
        silver_path = os.path.join(output_path, "silver", table)
        
        (df_silver.write
         .mode("overwrite")
         .parquet(silver_path))
        
        logger.info(f"  ✅ Wrote {df_silver.count()} records to {silver_path}")
    
    logger.info("Silver layer complete!")


def run_gold_layer(spark: SparkSession, input_path: str, output_path: str):
    """
    Run Gold layer aggregations locally.
    """
    logger.info("=" * 60)
    logger.info("GOLD LAYER - Business Aggregations")
    logger.info("=" * 60)
    
    # Import gold layer functions
    from src.glue_jobs.gold.dim_tables import (
        build_dim_customer,
        build_dim_product,
        build_dim_date
    )
    from src.glue_jobs.gold.fact_sales import (
        build_fact_sales,
        build_daily_sales_summary,
        build_product_performance
    )
    
    silver_path = os.path.join(input_path, "silver")
    gold_path = os.path.join(output_path, "gold")
    
    # Check if silver data exists
    if not os.path.exists(silver_path):
        logger.error(f"Silver layer not found at {silver_path}")
        return
    
    # Build dimension tables
    logger.info("Building dimension tables...")
    
    # Note: The gold functions expect S3 paths, so we need to adapt them
    # For local development, we'll implement simplified versions here
    
    # Dim Customer
    customers = spark.read.parquet(os.path.join(silver_path, "customers"))
    orders = spark.read.parquet(os.path.join(silver_path, "orders"))
    
    # Calculate customer metrics
    customer_metrics = (orders
                        .groupBy("customer_id")
                        .agg(
                            F.count("order_id").alias("total_orders"),
                            F.sum("total_amount").alias("total_spend")
                        ))
    
    dim_customer = (customers
                    .join(customer_metrics, "customer_id", "left")
                    .withColumn("customer_key", F.monotonically_increasing_id() + 1)
                    .withColumn("_created_at", F.current_timestamp()))
    
    dim_customer.write.mode("overwrite").parquet(os.path.join(gold_path, "dim_customer"))
    logger.info(f"  ✅ dim_customer: {dim_customer.count()} records")
    
    # Dim Product
    products = spark.read.parquet(os.path.join(silver_path, "products"))
    dim_product = (products
                   .withColumn("product_key", F.monotonically_increasing_id() + 1)
                   .withColumn("_created_at", F.current_timestamp()))
    
    dim_product.write.mode("overwrite").parquet(os.path.join(gold_path, "dim_product"))
    logger.info(f"  ✅ dim_product: {dim_product.count()} records")
    
    # Fact Sales
    logger.info("Building fact tables...")
    order_items = spark.read.parquet(os.path.join(silver_path, "order_items"))
    
    fact_sales = (order_items
                  .join(orders.select(
                      "order_id", "customer_id", "order_date", 
                      "status", "payment_method", "shipping_country"
                  ), "order_id", "inner")
                  .join(dim_customer.select("customer_id", "customer_key"), "customer_id", "inner")
                  .join(dim_product.select("product_id", "product_key", "cost"), "product_id", "inner")
                  .withColumn("sale_key", F.monotonically_increasing_id() + 1)
                  .withColumn("date_key", 
                              (F.year("order_date") * 10000 + 
                               F.month("order_date") * 100 + 
                               F.dayofmonth("order_date")))
                  .withColumn("gross_revenue", F.col("quantity") * F.col("unit_price"))
                  .withColumn("net_revenue", F.col("line_total"))
                  .withColumn("cost_of_goods", F.col("quantity") * F.col("cost"))
                  .withColumn("profit", F.col("net_revenue") - F.col("cost_of_goods"))
                  .withColumn("_created_at", F.current_timestamp()))
    
    fact_sales.write.mode("overwrite").parquet(os.path.join(gold_path, "fact_sales"))
    logger.info(f"  ✅ fact_sales: {fact_sales.count()} records")
    
    # Daily summary
    daily_summary = (fact_sales
                     .groupBy("date_key", "shipping_country")
                     .agg(
                         F.countDistinct("order_id").alias("total_orders"),
                         F.sum("quantity").alias("total_items"),
                         F.sum("net_revenue").alias("total_revenue"),
                         F.sum("profit").alias("total_profit")
                     )
                     .withColumn("_created_at", F.current_timestamp()))
    
    daily_summary.write.mode("overwrite").parquet(os.path.join(gold_path, "agg_daily_sales"))
    logger.info(f"  ✅ agg_daily_sales: {daily_summary.count()} records")
    
    logger.info("Gold layer complete!")


def main():
    parser = argparse.ArgumentParser(description="Run ETL pipeline locally")
    parser.add_argument(
        "--layer",
        choices=["bronze", "silver", "gold"],
        help="Which layer to run"
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Run all layers in sequence"
    )
    parser.add_argument(
        "--input",
        default="data/raw",
        help="Input directory for raw data"
    )
    parser.add_argument(
        "--output",
        default="data/output",
        help="Output directory for processed data"
    )
    
    args = parser.parse_args()
    
    if not args.layer and not args.all:
        parser.error("Either --layer or --all must be specified")
    
    # Create output directory
    os.makedirs(args.output, exist_ok=True)
    
    # Initialize Spark
    logger.info("Initializing Spark session...")
    spark = get_spark_session()
    
    try:
        if args.all:
            run_bronze_layer(spark, args.input, args.output)
            run_silver_layer(spark, args.output, args.output)
            run_gold_layer(spark, args.output, args.output)
        elif args.layer == "bronze":
            run_bronze_layer(spark, args.input, args.output)
        elif args.layer == "silver":
            run_silver_layer(spark, args.output, args.output)
        elif args.layer == "gold":
            run_gold_layer(spark, args.output, args.output)
        
        logger.info("=" * 60)
        logger.info("Pipeline complete! ✨")
        logger.info("=" * 60)
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
