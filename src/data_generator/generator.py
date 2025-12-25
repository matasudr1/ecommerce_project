"""
E-commerce Data Generator

This module generates realistic sample data for our e-commerce data pipeline.
It creates interconnected datasets that simulate a real e-commerce platform:
- Customers who make purchases
- Products available in a catalog
- Orders placed by customers
- Order items (individual products within orders)

The data has realistic patterns:
- Some customers buy more than others (VIP customers)
- Some products are more popular
- Orders have seasonal patterns
- Prices and quantities follow realistic distributions

Usage:
    python -m src.data_generator.generator --output data/raw --records 10000
"""

import os
import csv
import random
import uuid
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass
import click
from faker import Faker
from tqdm import tqdm

from .schemas import (
    Customer, Product, Order, OrderItem,
    ProductCategory, OrderStatus, PaymentMethod, CustomerSegment
)


# Initialize Faker with multiple locales for realistic international data
fake = Faker(['en_US', 'en_GB', 'de_DE', 'fr_FR', 'es_ES', 'it_IT', 'nl_NL', 'pl_PL'])
Faker.seed(42)  # For reproducibility
random.seed(42)


# =============================================================================
# CONFIGURATION - Product templates for realistic data
# =============================================================================

PRODUCT_TEMPLATES = {
    ProductCategory.ELECTRONICS: {
        "brands": ["Samsung", "Apple", "Sony", "LG", "Philips", "Xiaomi", "Huawei"],
        "products": [
            ("Smartphone", 299, 899),
            ("Laptop", 499, 1999),
            ("Tablet", 199, 999),
            ("Headphones", 29, 349),
            ("Smart Watch", 99, 499),
            ("TV", 299, 2499),
            ("Camera", 199, 1499),
            ("Speaker", 49, 399),
        ],
    },
    ProductCategory.CLOTHING: {
        "brands": ["Nike", "Adidas", "Zara", "H&M", "Levi's", "Uniqlo", "GAP"],
        "products": [
            ("T-Shirt", 15, 59),
            ("Jeans", 39, 129),
            ("Jacket", 59, 249),
            ("Sneakers", 49, 199),
            ("Dress", 29, 149),
            ("Hoodie", 35, 99),
            ("Shorts", 19, 69),
        ],
    },
    ProductCategory.HOME_GARDEN: {
        "brands": ["IKEA", "Bosch", "Dyson", "Philips", "Black+Decker", "Gardena"],
        "products": [
            ("Vacuum Cleaner", 99, 599),
            ("Coffee Machine", 49, 399),
            ("Lamp", 19, 149),
            ("Chair", 49, 299),
            ("Table", 79, 499),
            ("Plant Pot", 9, 49),
            ("Garden Tools Set", 29, 149),
        ],
    },
    ProductCategory.SPORTS: {
        "brands": ["Nike", "Adidas", "Puma", "Under Armour", "Reebok", "Decathlon"],
        "products": [
            ("Running Shoes", 59, 199),
            ("Yoga Mat", 15, 79),
            ("Dumbbells Set", 29, 199),
            ("Bicycle", 199, 999),
            ("Tennis Racket", 29, 199),
            ("Football", 15, 99),
            ("Fitness Tracker", 49, 249),
        ],
    },
    ProductCategory.BOOKS: {
        "brands": ["Penguin", "HarperCollins", "Simon & Schuster", "Macmillan", "Hachette"],
        "products": [
            ("Fiction Novel", 9, 29),
            ("Non-Fiction Book", 12, 39),
            ("Technical Manual", 29, 89),
            ("Children's Book", 7, 24),
            ("Cookbook", 15, 45),
            ("Biography", 14, 34),
        ],
    },
    ProductCategory.TOYS: {
        "brands": ["LEGO", "Hasbro", "Mattel", "Fisher-Price", "Playmobil", "Nintendo"],
        "products": [
            ("Building Blocks Set", 19, 149),
            ("Board Game", 15, 69),
            ("Action Figure", 9, 49),
            ("Doll", 14, 79),
            ("Puzzle", 9, 39),
            ("Remote Control Car", 29, 149),
            ("Video Game", 39, 69),
        ],
    },
    ProductCategory.BEAUTY: {
        "brands": ["L'Oreal", "Nivea", "Dove", "Estée Lauder", "MAC", "Clinique"],
        "products": [
            ("Face Cream", 9, 89),
            ("Shampoo", 5, 29),
            ("Perfume", 29, 149),
            ("Makeup Kit", 19, 99),
            ("Sunscreen", 9, 39),
            ("Hair Dryer", 19, 129),
        ],
    },
    ProductCategory.FOOD: {
        "brands": ["Nestle", "Kraft", "Kellogg's", "Unilever", "PepsiCo", "Organic Valley"],
        "products": [
            ("Coffee Beans 1kg", 9, 29),
            ("Chocolate Box", 7, 39),
            ("Protein Bars Pack", 12, 39),
            ("Olive Oil 1L", 8, 24),
            ("Tea Collection", 9, 29),
            ("Snack Box", 14, 49),
        ],
    },
}

# European countries for our e-commerce store
COUNTRIES = {
    "DE": "Germany",
    "FR": "France", 
    "GB": "United Kingdom",
    "ES": "Spain",
    "IT": "Italy",
    "NL": "Netherlands",
    "PL": "Poland",
    "BE": "Belgium",
    "AT": "Austria",
    "PT": "Portugal",
}


class EcommerceDataGenerator:
    """
    Generates realistic e-commerce data for testing and development.
    
    This generator creates interconnected datasets that maintain referential
    integrity (orders reference valid customers and products).
    
    Attributes:
        num_customers: Number of customer records to generate
        num_products: Number of product records to generate
        num_orders: Number of order records to generate
        start_date: Start date for order generation
        end_date: End date for order generation
    """
    
    def __init__(
        self,
        num_customers: int = 1000,
        num_products: int = 200,
        num_orders: int = 5000,
        start_date: datetime = None,
        end_date: datetime = None
    ):
        self.num_customers = num_customers
        self.num_products = num_products
        self.num_orders = num_orders
        self.start_date = start_date or datetime(2023, 1, 1)
        self.end_date = end_date or datetime(2024, 12, 31)
        
        # Storage for generated data
        self.customers: List[Dict] = []
        self.products: List[Dict] = []
        self.orders: List[Dict] = []
        self.order_items: List[Dict] = []
        
        # Lookup maps for referential integrity
        self._customer_ids: List[str] = []
        self._product_data: Dict[str, Dict] = {}  # product_id -> product details
        
    def generate_all(self) -> Tuple[List[Dict], List[Dict], List[Dict], List[Dict]]:
        """
        Generate all datasets in the correct order.
        
        Returns:
            Tuple of (customers, products, orders, order_items) lists
        """
        print("🏭 Starting data generation...")
        
        # Order matters! Products and customers must exist before orders
        self._generate_customers()
        self._generate_products()
        self._generate_orders()
        
        print(f"✅ Generated {len(self.customers)} customers")
        print(f"✅ Generated {len(self.products)} products")
        print(f"✅ Generated {len(self.orders)} orders")
        print(f"✅ Generated {len(self.order_items)} order items")
        
        return self.customers, self.products, self.orders, self.order_items
    
    def _generate_customers(self) -> None:
        """Generate customer records with realistic distributions."""
        print("👥 Generating customers...")
        
        for i in tqdm(range(self.num_customers), desc="Customers"):
            # Generate a realistic customer profile
            customer_id = f"CUST-{uuid.uuid4().hex[:8].upper()}"
            self._customer_ids.append(customer_id)
            
            # Pick a random country with weighted distribution
            country_code = random.choices(
                list(COUNTRIES.keys()),
                weights=[25, 20, 15, 12, 10, 8, 5, 3, 1, 1],  # Germany most common
                k=1
            )[0]
            
            # Generate creation date (customers created over time)
            days_ago = random.randint(0, (self.end_date - self.start_date).days)
            created_at = self.start_date + timedelta(days=days_ago)
            
            customer = {
                "customer_id": customer_id,
                "email": fake.email(),
                "first_name": fake.first_name(),
                "last_name": fake.last_name(),
                "phone": fake.phone_number() if random.random() > 0.3 else None,
                "country": country_code,
                "city": fake.city(),
                "address": fake.street_address() if random.random() > 0.2 else None,
                "created_at": created_at.isoformat(),
                "updated_at": None,
            }
            
            self.customers.append(customer)
    
    def _generate_products(self) -> None:
        """Generate product catalog with realistic pricing."""
        print("📦 Generating products...")
        
        product_count = 0
        
        for category in tqdm(ProductCategory, desc="Categories"):
            template = PRODUCT_TEMPLATES[category]
            
            # Generate multiple variants of each product type
            for product_name, min_price, max_price in template["products"]:
                # Generate 2-4 variants of each product
                num_variants = random.randint(2, 4)
                
                for variant in range(num_variants):
                    if product_count >= self.num_products:
                        return
                    
                    product_id = f"PROD-{uuid.uuid4().hex[:8].upper()}"
                    brand = random.choice(template["brands"])
                    
                    # Generate realistic price with some variance
                    base_price = random.uniform(min_price, max_price)
                    price = round(base_price, 2)
                    
                    # Cost is typically 40-70% of selling price
                    cost = round(price * random.uniform(0.4, 0.7), 2)
                    
                    product = {
                        "product_id": product_id,
                        "sku": f"{brand[:3].upper()}-{category.value[:3].upper()}-{product_count:04d}",
                        "name": f"{brand} {product_name} {fake.word().title()}",
                        "description": fake.sentence(nb_words=15),
                        "category": category.value,
                        "subcategory": product_name,
                        "brand": brand,
                        "price": price,
                        "cost": cost,
                        "stock_quantity": random.randint(0, 500),
                        "is_active": random.random() > 0.1,  # 90% active
                        "created_at": fake.date_time_between(
                            start_date=self.start_date,
                            end_date=self.end_date
                        ).isoformat(),
                    }
                    
                    self.products.append(product)
                    self._product_data[product_id] = {
                        "price": price,
                        "cost": cost,
                    }
                    product_count += 1
    
    def _generate_orders(self) -> None:
        """
        Generate orders with realistic patterns.
        
        Patterns simulated:
        - Some customers order more frequently (VIP behavior)
        - Seasonal variations (more orders in December)
        - Order values follow a realistic distribution
        - Multiple items per order
        """
        print("🛒 Generating orders...")
        
        # Create customer frequency distribution (some customers buy more)
        # 80/20 rule: 20% of customers generate 80% of orders
        vip_customers = self._customer_ids[:int(len(self._customer_ids) * 0.2)]
        regular_customers = self._customer_ids[int(len(self._customer_ids) * 0.2):]
        
        product_ids = list(self._product_data.keys())
        
        for i in tqdm(range(self.num_orders), desc="Orders"):
            order_id = f"ORD-{uuid.uuid4().hex[:8].upper()}"
            
            # VIP customers more likely to order
            if random.random() < 0.8:  # 80% of orders from VIP
                customer_id = random.choice(vip_customers)
            else:
                customer_id = random.choice(regular_customers)
            
            # Generate order date with seasonal pattern
            order_date = self._generate_order_date()
            
            # Determine order status based on how old the order is
            days_since_order = (self.end_date - order_date).days
            status = self._determine_order_status(days_since_order)
            
            # Generate order items (1-5 items per order)
            num_items = random.choices([1, 2, 3, 4, 5], weights=[40, 30, 15, 10, 5])[0]
            order_products = random.sample(product_ids, min(num_items, len(product_ids)))
            
            subtotal = 0.0
            for j, product_id in enumerate(order_products):
                order_item_id = f"ITEM-{uuid.uuid4().hex[:8].upper()}"
                
                product_price = self._product_data[product_id]["price"]
                quantity = random.choices([1, 2, 3, 4, 5], weights=[50, 25, 15, 7, 3])[0]
                
                # Sometimes apply a discount
                discount_percent = 0.0
                if random.random() < 0.2:  # 20% of items have discount
                    discount_percent = random.choice([5, 10, 15, 20, 25])
                
                line_total = round(quantity * product_price * (1 - discount_percent / 100), 2)
                subtotal += line_total
                
                order_item = {
                    "order_item_id": order_item_id,
                    "order_id": order_id,
                    "product_id": product_id,
                    "quantity": quantity,
                    "unit_price": product_price,
                    "discount_percent": discount_percent,
                    "line_total": line_total,
                }
                self.order_items.append(order_item)
            
            # Calculate order totals
            # Find customer country for shipping
            customer_data = next(c for c in self.customers if c["customer_id"] == customer_id)
            
            tax_rate = 0.21 if customer_data["country"] in ["NL", "BE"] else 0.19
            tax_amount = round(subtotal * tax_rate, 2)
            
            # Shipping based on order value
            shipping_amount = 0.0 if subtotal > 50 else round(random.uniform(3.99, 9.99), 2)
            
            # Order-level discount for large orders
            discount_amount = round(subtotal * 0.05, 2) if subtotal > 200 else 0.0
            
            total_amount = round(subtotal + tax_amount + shipping_amount - discount_amount, 2)
            
            order = {
                "order_id": order_id,
                "customer_id": customer_id,
                "order_date": order_date.isoformat(),
                "status": status.value,
                "payment_method": random.choice(list(PaymentMethod)).value,
                "subtotal": round(subtotal, 2),
                "tax_amount": tax_amount,
                "shipping_amount": shipping_amount,
                "discount_amount": discount_amount,
                "total_amount": total_amount,
                "currency": "EUR",
                "shipping_country": customer_data["country"],
                "shipping_city": customer_data["city"],
            }
            
            self.orders.append(order)
    
    def _generate_order_date(self) -> datetime:
        """
        Generate order date with seasonal patterns.
        
        More orders during:
        - December (holiday shopping)
        - Black Friday period (late November)
        - Back to school (September)
        """
        # Generate random date
        days_range = (self.end_date - self.start_date).days
        random_days = random.randint(0, days_range)
        date = self.start_date + timedelta(days=random_days)
        
        # Apply seasonal weighting (higher chance to keep December dates)
        month_weights = {
            1: 0.7, 2: 0.6, 3: 0.7, 4: 0.8, 5: 0.8, 6: 0.7,
            7: 0.6, 8: 0.7, 9: 0.9, 10: 0.8, 11: 1.0, 12: 1.0
        }
        
        # Rejection sampling for seasonal distribution
        if random.random() > month_weights[date.month]:
            # Try again with bias towards peak months
            if random.random() < 0.4:
                # Shift to peak months - handle day overflow for shorter months
                new_month = random.choice([11, 12])
                # November has 30 days, December has 31
                max_day = 30 if new_month == 11 else 31
                new_day = min(date.day, max_day)
                date = date.replace(month=new_month, day=new_day)
        
        return date
    
    def _determine_order_status(self, days_since_order: int) -> OrderStatus:
        """Determine order status based on age of order."""
        if days_since_order < 1:
            return random.choice([OrderStatus.PENDING, OrderStatus.CONFIRMED])
        elif days_since_order < 3:
            return random.choices(
                [OrderStatus.CONFIRMED, OrderStatus.SHIPPED],
                weights=[0.3, 0.7]
            )[0]
        elif days_since_order < 7:
            return random.choices(
                [OrderStatus.SHIPPED, OrderStatus.DELIVERED],
                weights=[0.2, 0.8]
            )[0]
        else:
            # Older orders are mostly delivered, some cancelled/returned
            return random.choices(
                [OrderStatus.DELIVERED, OrderStatus.CANCELLED, OrderStatus.RETURNED],
                weights=[0.92, 0.05, 0.03]
            )[0]
    
    def save_to_csv(self, output_dir: str) -> Dict[str, str]:
        """
        Save all generated data to CSV files.
        
        Args:
            output_dir: Directory to save CSV files
            
        Returns:
            Dictionary mapping dataset names to file paths
        """
        os.makedirs(output_dir, exist_ok=True)
        
        files = {}
        
        datasets = [
            ("customers", self.customers),
            ("products", self.products),
            ("orders", self.orders),
            ("order_items", self.order_items),
        ]
        
        for name, data in datasets:
            if not data:
                continue
                
            filepath = os.path.join(output_dir, f"{name}.csv")
            
            with open(filepath, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=data[0].keys())
                writer.writeheader()
                writer.writerows(data)
            
            files[name] = filepath
            print(f"📄 Saved {name}.csv ({len(data)} records)")
        
        return files


# =============================================================================
# CLI Interface
# =============================================================================

@click.command()
@click.option('--output', '-o', default='data/raw', help='Output directory for CSV files')
@click.option('--customers', '-c', default=1000, help='Number of customers to generate')
@click.option('--products', '-p', default=200, help='Number of products to generate')
@click.option('--orders', '-r', default=5000, help='Number of orders to generate')
@click.option('--seed', '-s', default=42, help='Random seed for reproducibility')
def main(output: str, customers: int, products: int, orders: int, seed: int):
    """
    Generate sample e-commerce data for the data pipeline.
    
    This script creates realistic interconnected datasets:
    - Customers with contact information
    - Products with pricing and categories
    - Orders with items and totals
    
    Example:
        python -m src.data_generator.generator --output data/raw --orders 10000
    """
    # Set seeds for reproducibility
    random.seed(seed)
    Faker.seed(seed)
    
    generator = EcommerceDataGenerator(
        num_customers=customers,
        num_products=products,
        num_orders=orders,
    )
    
    generator.generate_all()
    files = generator.save_to_csv(output)
    
    print(f"\n✨ Data generation complete!")
    print(f"📁 Files saved to: {os.path.abspath(output)}")


if __name__ == '__main__':
    main()
