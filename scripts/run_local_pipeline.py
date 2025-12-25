"""
Local Pipeline Runner

This script runs the entire ETL pipeline locally without AWS.
It simulates what would happen in AWS Glue, but uses local paths.

Usage:
    python scripts/run_local_pipeline.py --all
    python scripts/run_local_pipeline.py --bronze
    python scripts/run_local_pipeline.py --silver
    python scripts/run_local_pipeline.py --gold
"""

import os
import sys
import click
from pathlib import Path
from datetime import datetime

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def get_spark_session():
    """Create a local Spark session."""
    return (SparkSession.builder
            .appName("LocalETLPipeline")
            .master("local[*]")  # Use all available cores
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
            .config("spark.sql.parquet.compression.codec", "snappy")
            .config("spark.driver.memory", "4g")
            .config("spark.sql.shuffle.partitions", "4")
            .getOrCreate())


def run_bronze(spark, base_path: str):
    """
    Run Bronze layer ingestion locally.
    
    Reads CSV from raw/, adds metadata, writes Parquet to bronze/
    """
    print("\n" + "="*60)
    print("🥉 BRONZE LAYER - Raw Data Ingestion")
    print("="*60)
    
    raw_path = os.path.join(base_path, "raw")
    bronze_path = os.path.join(base_path, "bronze")
    
    tables = ["customers", "products", "orders", "order_items"]
    
    for table in tables:
        csv_path = os.path.join(raw_path, f"{table}.csv")
        output_path = os.path.join(bronze_path, table)
        
        if not os.path.exists(csv_path):
            print(f"  ⚠️  {table}.csv not found, skipping...")
            continue
        
        print(f"\n  Processing {table}...")
        
        # Read CSV
        df = spark.read.option("header", "true").csv(csv_path)
        
        # Add metadata columns
        df = (df
              .withColumn("_ingested_at", F.current_timestamp())
              .withColumn("_source_file", F.lit(csv_path))
              .withColumn("_ingestion_date", F.current_date()))
        
        # Write as Parquet
        (df.write
         .mode("overwrite")
         .partitionBy("_ingestion_date")
         .parquet(output_path))
        
        count = df.count()
        print(f"  ✅ {table}: {count} records → {output_path}")
    
    print("\n🥉 Bronze layer complete!")


def run_silver(spark, base_path: str):
    """
    Run Silver layer transformations locally.
    
    Reads from bronze/, cleans and transforms, writes to silver/
    """
    print("\n" + "="*60)
    print("🥈 SILVER LAYER - Data Transformation")
    print("="*60)
    
    bronze_path = os.path.join(base_path, "bronze")
    silver_path = os.path.join(base_path, "silver")
    
    # Import transformation functions
    from src.glue_jobs.silver.transform_to_silver import (
        transform_customers, transform_products, 
        transform_orders, transform_order_items
    )
    
    transformations = {
        "customers": transform_customers,
        "products": transform_products,
        "orders": transform_orders,
        "order_items": transform_order_items,
    }
    
    for table, transform_fn in transformations.items():
        input_path = os.path.join(bronze_path, table)
        output_path = os.path.join(silver_path, table)
        
        if not os.path.exists(input_path):
            print(f"  ⚠️  {table} bronze data not found, skipping...")
            continue
        
        print(f"\n  Transforming {table}...")
        
        # Read bronze data
        df = spark.read.parquet(input_path)
        print(f"    Read {df.count()} records from bronze")
        
        # Apply transformation
        df_transformed = transform_fn(df)
        
        # Write to silver
        (df_transformed.write
         .mode("overwrite")
         .parquet(output_path))
        
        count = df_transformed.count()
        print(f"  ✅ {table}: {count} records → {output_path}")
    
    print("\n🥈 Silver layer complete!")


def run_gold(spark, base_path: str):
    """
    Run Gold layer (star schema) locally.
    
    Reads from silver/, builds dimensions and facts, writes to gold/
    """
    print("\n" + "="*60)
    print("🥇 GOLD LAYER - Star Schema Construction")
    print("="*60)
    
    silver_path = os.path.join(base_path, "silver")
    gold_path = os.path.join(base_path, "gold")
    
    # Check silver data exists
    required_tables = ["customers", "products", "orders", "order_items"]
    for table in required_tables:
        if not os.path.exists(os.path.join(silver_path, table)):
            print(f"  ❌ Silver {table} not found. Run silver layer first.")
            return
    
    # =========================================================================
    # Build Dimension Tables
    # =========================================================================
    print("\n  Building dimension tables...")
    
    # --- dim_date ---
    print("    Building dim_date...")
    from datetime import timedelta
    start_date = datetime(2020, 1, 1)
    end_date = datetime(2030, 12, 31)
    num_days = (end_date - start_date).days + 1
    
    date_data = [(start_date + timedelta(days=i),) for i in range(num_days)]
    from pyspark.sql.types import StructType, StructField, DateType, IntegerType
    
    schema = StructType([StructField("date", DateType(), False)])
    dates_df = spark.createDataFrame(date_data, schema)
    
    dim_date = (dates_df
                .withColumn("date_key", 
                    (F.year("date") * 10000 + F.month("date") * 100 + F.dayofmonth("date")).cast(IntegerType()))
                .withColumn("day_of_month", F.dayofmonth("date"))
                .withColumn("day_of_week", F.dayofweek("date"))
                .withColumn("month", F.month("date"))
                .withColumn("quarter", F.quarter("date"))
                .withColumn("year", F.year("date"))
                .withColumn("day_name", F.date_format("date", "EEEE"))
                .withColumn("month_name", F.date_format("date", "MMMM"))
                .withColumn("is_weekend", F.dayofweek("date").isin(1, 7))
                .withColumn("year_month", F.date_format("date", "yyyy-MM")))
    
    dim_date.write.mode("overwrite").parquet(os.path.join(gold_path, "dim_date"))
    print(f"    ✅ dim_date: {dim_date.count()} records")
    
    # --- dim_customer ---
    print("    Building dim_customer...")
    customers = spark.read.parquet(os.path.join(silver_path, "customers"))
    orders = spark.read.parquet(os.path.join(silver_path, "orders"))
    
    customer_metrics = (orders
                        .groupBy("customer_id")
                        .agg(
                            F.count("order_id").alias("total_orders"),
                            F.sum("total_amount").alias("total_spend"),
                            F.min("order_date").alias("first_order_date"),
                            F.max("order_date").alias("last_order_date")))
    
    from pyspark.sql.window import Window
    window = Window.orderBy("customer_id")
    
    dim_customer = (customers
                    .join(customer_metrics, "customer_id", "left")
                    .withColumn("customer_key", F.row_number().over(window))
                    .withColumn("value_tier",
                        F.when(F.col("total_spend") >= 1000, "platinum")
                         .when(F.col("total_spend") >= 500, "gold")
                         .when(F.col("total_spend") >= 100, "silver")
                         .otherwise("bronze"))
                    .withColumn("_created_at", F.current_timestamp()))
    
    dim_customer.write.mode("overwrite").parquet(os.path.join(gold_path, "dim_customer"))
    print(f"    ✅ dim_customer: {dim_customer.count()} records")
    
    # --- dim_product ---
    print("    Building dim_product...")
    products = spark.read.parquet(os.path.join(silver_path, "products"))
    
    window = Window.orderBy("product_id")
    dim_product = (products
                   .withColumn("product_key", F.row_number().over(window))
                   .withColumn("price_tier",
                       F.when(F.col("price") >= 500, "premium")
                        .when(F.col("price") >= 100, "mid_range")
                        .when(F.col("price") >= 25, "budget")
                        .otherwise("economy"))
                   .withColumn("_created_at", F.current_timestamp()))
    
    dim_product.write.mode("overwrite").parquet(os.path.join(gold_path, "dim_product"))
    print(f"    ✅ dim_product: {dim_product.count()} records")
    
    # =========================================================================
    # Build Fact Table
    # =========================================================================
    print("\n  Building fact_sales...")
    
    order_items = spark.read.parquet(os.path.join(silver_path, "order_items"))
    
    # Get dimension lookups
    customer_lookup = dim_customer.select("customer_id", "customer_key")
    product_lookup = dim_product.select("product_id", "product_key", "cost")
    
    fact_sales = (order_items
                  .join(orders.select("order_id", "customer_id", "order_date", "status", 
                                      "payment_method", "subtotal", "tax_amount", "shipping_amount"), 
                        "order_id", "inner")
                  .join(customer_lookup, "customer_id", "inner")
                  .join(product_lookup, "product_id", "inner")
                  .withColumn("date_key",
                      (F.year("order_date") * 10000 + F.month("order_date") * 100 + 
                       F.dayofmonth("order_date")).cast(IntegerType()))
                  .withColumn("gross_revenue", F.round(F.col("quantity") * F.col("unit_price"), 2))
                  .withColumn("net_revenue", F.col("line_total"))
                  .withColumn("cost_of_goods", F.round(F.col("quantity") * F.col("cost"), 2))
                  .withColumn("profit", F.round(F.col("net_revenue") - F.col("cost_of_goods"), 2))
                  .withColumn("_created_at", F.current_timestamp()))
    
    window = Window.orderBy("order_item_id")
    fact_sales = fact_sales.withColumn("sale_key", F.row_number().over(window))
    
    # Select final columns
    fact_sales_final = fact_sales.select(
        "sale_key", "date_key", "customer_key", "product_key",
        "order_id", "order_item_id", "order_date", "status", "payment_method",
        "quantity", "unit_price", "gross_revenue", "net_revenue",
        "cost_of_goods", "profit", "_created_at"
    )
    
    fact_sales_final.write.mode("overwrite").parquet(os.path.join(gold_path, "fact_sales"))
    print(f"  ✅ fact_sales: {fact_sales_final.count()} records")
    
    print("\n🥇 Gold layer complete!")


def run_validation(spark, base_path: str):
    """Run data quality validation on the pipeline output."""
    print("\n" + "="*60)
    print("✅ DATA QUALITY VALIDATION")
    print("="*60)
    
    gold_path = os.path.join(base_path, "gold")
    
    # Check fact_sales
    fact_sales = spark.read.parquet(os.path.join(gold_path, "fact_sales"))
    dim_customer = spark.read.parquet(os.path.join(gold_path, "dim_customer"))
    dim_product = spark.read.parquet(os.path.join(gold_path, "dim_product"))
    
    print("\n  Record counts:")
    print(f"    fact_sales:   {fact_sales.count():,} rows")
    print(f"    dim_customer: {dim_customer.count():,} rows")
    print(f"    dim_product:  {dim_product.count():,} rows")
    
    # Check for nulls in key columns
    print("\n  Null checks:")
    null_count = fact_sales.filter(F.col("customer_key").isNull()).count()
    print(f"    fact_sales.customer_key nulls: {null_count} {'✅' if null_count == 0 else '❌'}")
    
    null_count = fact_sales.filter(F.col("product_key").isNull()).count()
    print(f"    fact_sales.product_key nulls:  {null_count} {'✅' if null_count == 0 else '❌'}")
    
    # Check referential integrity
    print("\n  Referential integrity:")
    orphan_customers = fact_sales.join(dim_customer, "customer_key", "left_anti").count()
    print(f"    Orphan customer_keys: {orphan_customers} {'✅' if orphan_customers == 0 else '❌'}")
    
    orphan_products = fact_sales.join(dim_product, "product_key", "left_anti").count()
    print(f"    Orphan product_keys:  {orphan_products} {'✅' if orphan_products == 0 else '❌'}")
    
    # Summary stats
    print("\n  Business metrics:")
    total_revenue = fact_sales.agg(F.sum("net_revenue")).collect()[0][0]
    total_profit = fact_sales.agg(F.sum("profit")).collect()[0][0]
    print(f"    Total Revenue: €{total_revenue:,.2f}")
    print(f"    Total Profit:  €{total_profit:,.2f}")
    print(f"    Profit Margin: {(total_profit/total_revenue)*100:.1f}%")
    
    print("\n✅ Validation complete!")


@click.command()
@click.option('--all', 'run_all', is_flag=True, help='Run entire pipeline')
@click.option('--bronze', is_flag=True, help='Run Bronze layer only')
@click.option('--silver', is_flag=True, help='Run Silver layer only')
@click.option('--gold', is_flag=True, help='Run Gold layer only')
@click.option('--validate', is_flag=True, help='Run validation only')
@click.option('--data-path', default='data', help='Base path for data')
def main(run_all, bronze, silver, gold, validate, data_path):
    """
    Run the ETL pipeline locally.
    
    Examples:
        python scripts/run_local_pipeline.py --all
        python scripts/run_local_pipeline.py --bronze --silver
    """
    print("🚀 E-commerce Data Pipeline - Local Runner")
    print(f"📁 Data path: {os.path.abspath(data_path)}")
    
    spark = get_spark_session()
    
    try:
        if run_all:
            run_bronze(spark, data_path)
            run_silver(spark, data_path)
            run_gold(spark, data_path)
            run_validation(spark, data_path)
        else:
            if bronze:
                run_bronze(spark, data_path)
            if silver:
                run_silver(spark, data_path)
            if gold:
                run_gold(spark, data_path)
            if validate:
                run_validation(spark, data_path)
        
        print("\n" + "="*60)
        print("🎉 Pipeline execution complete!")
        print("="*60)
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
