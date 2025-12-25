"""Tests for the data generator module."""

import pytest
from datetime import datetime


class TestEcommerceDataGenerator:
    """Tests for the EcommerceDataGenerator class."""
    
    def test_generator_initialization(self):
        """Test that generator initializes with correct defaults."""
        from src.data_generator.generator import EcommerceDataGenerator
        
        generator = EcommerceDataGenerator(
            num_customers=100,
            num_products=50,
            num_orders=200
        )
        
        assert generator.num_customers == 100
        assert generator.num_products == 50
        assert generator.num_orders == 200
        assert len(generator.customers) == 0  # Not yet generated
    
    def test_generate_customers(self):
        """Test customer generation."""
        from src.data_generator.generator import EcommerceDataGenerator
        
        generator = EcommerceDataGenerator(
            num_customers=10,
            num_products=5,
            num_orders=20
        )
        
        generator._generate_customers()
        
        assert len(generator.customers) == 10
        
        # Check customer structure
        customer = generator.customers[0]
        assert "customer_id" in customer
        assert "email" in customer
        assert "first_name" in customer
        assert "last_name" in customer
        assert "country" in customer
        assert "created_at" in customer
        
        # Check customer_id format
        assert customer["customer_id"].startswith("CUST-")
    
    def test_generate_products(self):
        """Test product generation."""
        from src.data_generator.generator import EcommerceDataGenerator
        
        generator = EcommerceDataGenerator(
            num_customers=10,
            num_products=20,
            num_orders=50
        )
        
        generator._generate_products()
        
        assert len(generator.products) == 20
        
        # Check product structure
        product = generator.products[0]
        assert "product_id" in product
        assert "name" in product
        assert "price" in product
        assert "cost" in product
        assert "category" in product
        
        # Check price is reasonable
        assert product["price"] > 0
        assert product["cost"] > 0
        assert product["price"] >= product["cost"]
    
    def test_generate_orders(self):
        """Test order generation with referential integrity."""
        from src.data_generator.generator import EcommerceDataGenerator
        
        generator = EcommerceDataGenerator(
            num_customers=10,
            num_products=20,
            num_orders=50
        )
        
        # Generate customers and products first (required for orders)
        generator._generate_customers()
        generator._generate_products()
        generator._generate_orders()
        
        assert len(generator.orders) == 50
        assert len(generator.order_items) > 0  # At least one item per order
        
        # Check order structure
        order = generator.orders[0]
        assert "order_id" in order
        assert "customer_id" in order
        assert "order_date" in order
        assert "total_amount" in order
        assert "status" in order
        
        # Verify referential integrity - customer_id should exist
        customer_ids = {c["customer_id"] for c in generator.customers}
        for order in generator.orders:
            assert order["customer_id"] in customer_ids
    
    def test_generate_all(self):
        """Test complete data generation."""
        from src.data_generator.generator import EcommerceDataGenerator
        
        generator = EcommerceDataGenerator(
            num_customers=50,
            num_products=30,
            num_orders=100
        )
        
        customers, products, orders, order_items = generator.generate_all()
        
        assert len(customers) == 50
        assert len(products) == 30
        assert len(orders) == 100
        assert len(order_items) > 0
    
    def test_save_to_csv(self, tmp_path):
        """Test saving data to CSV files."""
        from src.data_generator.generator import EcommerceDataGenerator
        
        generator = EcommerceDataGenerator(
            num_customers=5,
            num_products=5,
            num_orders=10
        )
        
        generator.generate_all()
        files = generator.save_to_csv(str(tmp_path))
        
        # Check files were created
        assert "customers" in files
        assert "products" in files
        assert "orders" in files
        assert "order_items" in files
        
        # Check files exist
        import os
        assert os.path.exists(files["customers"])
        assert os.path.exists(files["products"])
        assert os.path.exists(files["orders"])
        assert os.path.exists(files["order_items"])


class TestSchemas:
    """Tests for Pydantic schema validation."""
    
    def test_customer_schema_valid(self):
        """Test valid customer data passes validation."""
        from src.data_generator.schemas import Customer
        
        customer = Customer(
            customer_id="CUST-12345678",
            email="test@example.com",
            first_name="John",
            last_name="Doe",
            country="DE",
            city="Berlin",
            created_at=datetime.now()
        )
        
        assert customer.customer_id == "CUST-12345678"
        assert customer.email == "test@example.com"
    
    def test_product_schema_valid(self):
        """Test valid product data passes validation."""
        from src.data_generator.schemas import Product, ProductCategory
        
        product = Product(
            product_id="PROD-12345678",
            sku="SAM-ELE-0001",
            name="Test Product",
            category=ProductCategory.ELECTRONICS,
            price=99.99,
            cost=50.00,
            stock_quantity=100,
            created_at=datetime.now()
        )
        
        assert product.price == 99.99
        assert product.category == ProductCategory.ELECTRONICS
    
    def test_order_schema_valid(self):
        """Test valid order data passes validation."""
        from src.data_generator.schemas import Order, OrderStatus, PaymentMethod
        
        order = Order(
            order_id="ORD-12345678",
            customer_id="CUST-12345678",
            order_date=datetime.now(),
            status=OrderStatus.DELIVERED,
            payment_method=PaymentMethod.CREDIT_CARD,
            subtotal=100.00,
            tax_amount=19.00,
            shipping_amount=5.00,
            discount_amount=0.00,
            total_amount=124.00,
            shipping_country="DE",
            shipping_city="Berlin"
        )
        
        assert order.status == OrderStatus.DELIVERED
        assert order.total_amount == 124.00
