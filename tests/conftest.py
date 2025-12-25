"""
Pytest configuration and shared fixtures.

Fixtures are reusable test components that provide:
- Test data
- Spark sessions
- Mock objects
- Database connections

Use fixtures to avoid repeating setup code in each test.
"""

import os
import sys
import pytest
from datetime import datetime
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))


@pytest.fixture(scope="session")
def spark():
    """
    Create a Spark session for testing.
    
    scope="session" means this fixture is created once per test session,
    not once per test. This is more efficient for Spark.
    """
    from pyspark.sql import SparkSession
    
    spark = (SparkSession.builder
             .appName("TestSession")
             .master("local[2]")
             .config("spark.sql.shuffle.partitions", "2")
             .config("spark.driver.memory", "2g")
             .getOrCreate())
    
    yield spark
    
    # Cleanup after all tests
    spark.stop()


@pytest.fixture
def sample_customers_data():
    """Sample customer data for testing."""
    return [
        {
            "customer_id": "CUST-001",
            "email": "john.doe@example.com",
            "first_name": "John",
            "last_name": "Doe",
            "phone": "+1234567890",
            "country": "DE",
            "city": "Berlin",
            "address": "123 Main St",
            "created_at": "2023-01-15T10:30:00",
            "updated_at": None,
        },
        {
            "customer_id": "CUST-002",
            "email": "jane.smith@example.com",
            "first_name": "Jane",
            "last_name": "Smith",
            "phone": None,
            "country": "FR",
            "city": "Paris",
            "address": "456 Rue Example",
            "created_at": "2023-06-20T15:45:00",
            "updated_at": "2023-07-01T09:00:00",
        },
    ]


@pytest.fixture
def sample_products_data():
    """Sample product data for testing."""
    return [
        {
            "product_id": "PROD-001",
            "sku": "SAM-ELE-0001",
            "name": "Samsung Galaxy Phone",
            "description": "A great smartphone",
            "category": "electronics",
            "subcategory": "Smartphone",
            "brand": "Samsung",
            "price": "599.99",
            "cost": "350.00",
            "stock_quantity": "100",
            "is_active": "true",
            "created_at": "2023-01-01T00:00:00",
        },
        {
            "product_id": "PROD-002",
            "sku": "NIK-CLO-0002",
            "name": "Nike Running Shoes",
            "description": "Comfortable running shoes",
            "category": "sports",
            "subcategory": "Running Shoes",
            "brand": "Nike",
            "price": "129.99",
            "cost": "65.00",
            "stock_quantity": "50",
            "is_active": "true",
            "created_at": "2023-02-15T00:00:00",
        },
    ]


@pytest.fixture
def sample_orders_data():
    """Sample order data for testing."""
    return [
        {
            "order_id": "ORD-001",
            "customer_id": "CUST-001",
            "order_date": "2023-12-01T14:30:00",
            "status": "delivered",
            "payment_method": "credit_card",
            "subtotal": "599.99",
            "tax_amount": "114.00",
            "shipping_amount": "0.00",
            "discount_amount": "0.00",
            "total_amount": "713.99",
            "currency": "EUR",
            "shipping_country": "DE",
            "shipping_city": "Berlin",
        },
        {
            "order_id": "ORD-002",
            "customer_id": "CUST-002",
            "order_date": "2023-12-15T09:15:00",
            "status": "shipped",
            "payment_method": "paypal",
            "subtotal": "129.99",
            "tax_amount": "24.70",
            "shipping_amount": "5.99",
            "discount_amount": "10.00",
            "total_amount": "150.68",
            "currency": "EUR",
            "shipping_country": "FR",
            "shipping_city": "Paris",
        },
    ]


@pytest.fixture
def sample_order_items_data():
    """Sample order items data for testing."""
    return [
        {
            "order_item_id": "ITEM-001",
            "order_id": "ORD-001",
            "product_id": "PROD-001",
            "quantity": "1",
            "unit_price": "599.99",
            "discount_percent": "0",
            "line_total": "599.99",
        },
        {
            "order_item_id": "ITEM-002",
            "order_id": "ORD-002",
            "product_id": "PROD-002",
            "quantity": "1",
            "unit_price": "129.99",
            "discount_percent": "0",
            "line_total": "129.99",
        },
    ]


@pytest.fixture
def customers_df(spark, sample_customers_data):
    """Create a customers DataFrame for testing."""
    return spark.createDataFrame(sample_customers_data)


@pytest.fixture
def products_df(spark, sample_products_data):
    """Create a products DataFrame for testing."""
    return spark.createDataFrame(sample_products_data)


@pytest.fixture
def orders_df(spark, sample_orders_data):
    """Create an orders DataFrame for testing."""
    return spark.createDataFrame(sample_orders_data)


@pytest.fixture
def order_items_df(spark, sample_order_items_data):
    """Create an order items DataFrame for testing."""
    return spark.createDataFrame(sample_order_items_data)


@pytest.fixture
def temp_data_dir(tmp_path):
    """Create a temporary directory structure for data."""
    raw_dir = tmp_path / "raw"
    bronze_dir = tmp_path / "bronze"
    silver_dir = tmp_path / "silver"
    gold_dir = tmp_path / "gold"
    
    raw_dir.mkdir()
    bronze_dir.mkdir()
    silver_dir.mkdir()
    gold_dir.mkdir()
    
    return {
        "root": tmp_path,
        "raw": raw_dir,
        "bronze": bronze_dir,
        "silver": silver_dir,
        "gold": gold_dir,
    }
