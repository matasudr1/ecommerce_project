"""Tests for data transformation functions."""

import pytest
from pyspark.sql import functions as F


class TestCustomerTransformations:
    """Tests for customer data transformations."""
    
    def test_email_lowercase(self, spark, sample_customers_data):
        """Test that emails are converted to lowercase."""
        from src.glue_jobs.silver.transform_to_silver import transform_customers
        
        # Add a customer with uppercase email
        data = sample_customers_data.copy()
        data[0]["email"] = "JOHN.DOE@EXAMPLE.COM"
        
        # Add required metadata columns
        df = spark.createDataFrame(data)
        df = (df
              .withColumn("_ingested_at", F.current_timestamp())
              .withColumn("_source_file", F.lit("test.csv")))
        
        result = transform_customers(df)
        
        # Check email is lowercase
        emails = [row.email for row in result.collect()]
        assert "john.doe@example.com" in emails
    
    def test_email_domain_extraction(self, spark, sample_customers_data):
        """Test that email domain is correctly extracted."""
        from src.glue_jobs.silver.transform_to_silver import transform_customers
        
        df = spark.createDataFrame(sample_customers_data)
        df = (df
              .withColumn("_ingested_at", F.current_timestamp())
              .withColumn("_source_file", F.lit("test.csv")))
        
        result = transform_customers(df)
        
        # Check email domain extraction
        row = result.filter(F.col("email") == "john.doe@example.com").first()
        assert row.email_domain == "example.com"
    
    def test_full_name_creation(self, spark, sample_customers_data):
        """Test that full name is correctly created."""
        from src.glue_jobs.silver.transform_to_silver import transform_customers
        
        df = spark.createDataFrame(sample_customers_data)
        df = (df
              .withColumn("_ingested_at", F.current_timestamp())
              .withColumn("_source_file", F.lit("test.csv")))
        
        result = transform_customers(df)
        
        # Check full name
        row = result.filter(F.col("customer_id") == "CUST-001").first()
        assert row.full_name == "John Doe"
    
    def test_country_uppercase(self, spark, sample_customers_data):
        """Test that country codes are uppercase."""
        from src.glue_jobs.silver.transform_to_silver import transform_customers
        
        # Add lowercase country
        data = sample_customers_data.copy()
        data[0]["country"] = "de"
        
        df = spark.createDataFrame(data)
        df = (df
              .withColumn("_ingested_at", F.current_timestamp())
              .withColumn("_source_file", F.lit("test.csv")))
        
        result = transform_customers(df)
        
        row = result.filter(F.col("customer_id") == "CUST-001").first()
        assert row.country == "DE"
    
    def test_deduplication(self, spark, sample_customers_data):
        """Test that duplicate customers are removed."""
        from src.glue_jobs.silver.transform_to_silver import transform_customers
        
        # Create duplicate customer
        data = sample_customers_data.copy()
        duplicate = data[0].copy()
        duplicate["email"] = "updated@example.com"  # Different email, same ID
        data.append(duplicate)
        
        df = spark.createDataFrame(data)
        df = (df
              .withColumn("_ingested_at", F.current_timestamp())
              .withColumn("_source_file", F.lit("test.csv")))
        
        result = transform_customers(df)
        
        # Should only have 2 customers (original data had 2 unique customer_ids)
        assert result.count() == 2


class TestProductTransformations:
    """Tests for product data transformations."""
    
    def test_price_casting(self, spark, sample_products_data):
        """Test that price is cast to double."""
        from src.glue_jobs.silver.transform_to_silver import transform_products
        
        df = spark.createDataFrame(sample_products_data)
        df = (df
              .withColumn("_ingested_at", F.current_timestamp())
              .withColumn("_source_file", F.lit("test.csv")))
        
        result = transform_products(df)
        
        # Check price type
        price_type = result.schema["price"].dataType.simpleString()
        assert price_type == "double"
        
        # Check value
        row = result.filter(F.col("product_id") == "PROD-001").first()
        assert row.price == 599.99
    
    def test_margin_calculation(self, spark, sample_products_data):
        """Test that margin percentage is correctly calculated."""
        from src.glue_jobs.silver.transform_to_silver import transform_products
        
        df = spark.createDataFrame(sample_products_data)
        df = (df
              .withColumn("_ingested_at", F.current_timestamp())
              .withColumn("_source_file", F.lit("test.csv")))
        
        result = transform_products(df)
        
        row = result.filter(F.col("product_id") == "PROD-001").first()
        
        # Expected margin: (599.99 - 350.00) / 599.99 * 100 = 41.67%
        expected_margin = round((599.99 - 350.00) / 599.99 * 100, 2)
        assert abs(row.margin_percent - expected_margin) < 0.01
    
    def test_boolean_parsing(self, spark, sample_products_data):
        """Test that is_active is correctly parsed to boolean."""
        from src.glue_jobs.silver.transform_to_silver import transform_products
        
        df = spark.createDataFrame(sample_products_data)
        df = (df
              .withColumn("_ingested_at", F.current_timestamp())
              .withColumn("_source_file", F.lit("test.csv")))
        
        result = transform_products(df)
        
        row = result.filter(F.col("product_id") == "PROD-001").first()
        assert row.is_active == True


class TestOrderTransformations:
    """Tests for order data transformations."""
    
    def test_date_parsing(self, spark, sample_orders_data):
        """Test that order_date is parsed correctly."""
        from src.glue_jobs.silver.transform_to_silver import transform_orders
        
        df = spark.createDataFrame(sample_orders_data)
        df = (df
              .withColumn("_ingested_at", F.current_timestamp())
              .withColumn("_source_file", F.lit("test.csv")))
        
        result = transform_orders(df)
        
        # Check date type
        date_type = result.schema["order_date"].dataType.simpleString()
        assert "timestamp" in date_type
    
    def test_date_components(self, spark, sample_orders_data):
        """Test that date components are correctly extracted."""
        from src.glue_jobs.silver.transform_to_silver import transform_orders
        
        df = spark.createDataFrame(sample_orders_data)
        df = (df
              .withColumn("_ingested_at", F.current_timestamp())
              .withColumn("_source_file", F.lit("test.csv")))
        
        result = transform_orders(df)
        
        row = result.filter(F.col("order_id") == "ORD-001").first()
        assert row.order_year == 2023
        assert row.order_month == 12
        assert row.order_day == 1
    
    def test_status_lowercase(self, spark, sample_orders_data):
        """Test that status is lowercase."""
        from src.glue_jobs.silver.transform_to_silver import transform_orders
        
        data = sample_orders_data.copy()
        data[0]["status"] = "DELIVERED"
        
        df = spark.createDataFrame(data)
        df = (df
              .withColumn("_ingested_at", F.current_timestamp())
              .withColumn("_source_file", F.lit("test.csv")))
        
        result = transform_orders(df)
        
        row = result.filter(F.col("order_id") == "ORD-001").first()
        assert row.status == "delivered"


class TestOrderItemTransformations:
    """Tests for order item data transformations."""
    
    def test_quantity_casting(self, spark, sample_order_items_data):
        """Test that quantity is cast to integer."""
        from src.glue_jobs.silver.transform_to_silver import transform_order_items
        
        df = spark.createDataFrame(sample_order_items_data)
        df = (df
              .withColumn("_ingested_at", F.current_timestamp())
              .withColumn("_source_file", F.lit("test.csv")))
        
        result = transform_order_items(df)
        
        # Check quantity type
        qty_type = result.schema["quantity"].dataType.simpleString()
        assert qty_type == "int"
    
    def test_gross_amount_calculation(self, spark, sample_order_items_data):
        """Test that gross_amount is correctly calculated."""
        from src.glue_jobs.silver.transform_to_silver import transform_order_items
        
        df = spark.createDataFrame(sample_order_items_data)
        df = (df
              .withColumn("_ingested_at", F.current_timestamp())
              .withColumn("_source_file", F.lit("test.csv")))
        
        result = transform_order_items(df)
        
        row = result.filter(F.col("order_item_id") == "ITEM-001").first()
        
        # quantity * unit_price = 1 * 599.99 = 599.99
        assert row.gross_amount == 599.99
