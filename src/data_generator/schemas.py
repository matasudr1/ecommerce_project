"""
Data schemas and models for the e-commerce data pipeline.

This module defines the structure of our source data using Pydantic models.
Pydantic helps us:
1. Validate data types automatically
2. Document the expected structure
3. Serialize/deserialize data easily
"""

from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Optional
from pydantic import BaseModel, Field, EmailStr, validator


class OrderStatus(str, Enum):
    """Possible states of an order in our e-commerce system."""
    PENDING = "pending"
    CONFIRMED = "confirmed"
    SHIPPED = "shipped"
    DELIVERED = "delivered"
    CANCELLED = "cancelled"
    RETURNED = "returned"


class PaymentMethod(str, Enum):
    """Supported payment methods."""
    CREDIT_CARD = "credit_card"
    DEBIT_CARD = "debit_card"
    PAYPAL = "paypal"
    BANK_TRANSFER = "bank_transfer"
    CASH_ON_DELIVERY = "cash_on_delivery"


class ProductCategory(str, Enum):
    """Product categories in our e-commerce store."""
    ELECTRONICS = "electronics"
    CLOTHING = "clothing"
    HOME_GARDEN = "home_garden"
    SPORTS = "sports"
    BOOKS = "books"
    TOYS = "toys"
    BEAUTY = "beauty"
    FOOD = "food"


class CustomerSegment(str, Enum):
    """Customer segments based on behavior and value."""
    NEW = "new"
    REGULAR = "regular"
    VIP = "vip"
    INACTIVE = "inactive"


# =============================================================================
# SOURCE DATA MODELS (What we receive from source systems)
# =============================================================================

class Customer(BaseModel):
    """
    Customer record from the source system.
    
    This represents a customer in our e-commerce platform.
    Each customer has a unique ID and contact information.
    """
    customer_id: str = Field(..., description="Unique identifier for the customer")
    email: str = Field(..., description="Customer's email address")
    first_name: str = Field(..., description="Customer's first name")
    last_name: str = Field(..., description="Customer's last name")
    phone: Optional[str] = Field(None, description="Customer's phone number")
    country: str = Field(..., description="Customer's country code (ISO 3166-1)")
    city: str = Field(..., description="Customer's city")
    address: Optional[str] = Field(None, description="Customer's street address")
    created_at: datetime = Field(..., description="When the customer account was created")
    updated_at: Optional[datetime] = Field(None, description="Last update timestamp")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class Product(BaseModel):
    """
    Product record from the catalog system.
    
    Represents an item available for purchase in our store.
    """
    product_id: str = Field(..., description="Unique identifier for the product")
    sku: str = Field(..., description="Stock Keeping Unit - inventory tracking code")
    name: str = Field(..., description="Product display name")
    description: Optional[str] = Field(None, description="Product description")
    category: ProductCategory = Field(..., description="Product category")
    subcategory: Optional[str] = Field(None, description="Product subcategory")
    brand: Optional[str] = Field(None, description="Product brand name")
    price: float = Field(..., ge=0, description="Current selling price")
    cost: float = Field(..., ge=0, description="Cost price for margin calculation")
    stock_quantity: int = Field(..., ge=0, description="Current inventory level")
    is_active: bool = Field(True, description="Whether product is available for sale")
    created_at: datetime = Field(..., description="When product was added to catalog")
    
    @validator('price')
    def price_must_be_greater_than_cost(cls, v, values):
        """Validate that selling price is reasonable compared to cost."""
        if 'cost' in values and v < values['cost'] * 0.5:
            # Allow some loss leaders but flag extreme cases
            pass  # In production, we might log a warning here
        return v


class Order(BaseModel):
    """
    Order record from the order management system.
    
    Represents a customer purchase transaction.
    """
    order_id: str = Field(..., description="Unique identifier for the order")
    customer_id: str = Field(..., description="Reference to the customer")
    order_date: datetime = Field(..., description="When the order was placed")
    status: OrderStatus = Field(..., description="Current order status")
    payment_method: PaymentMethod = Field(..., description="How the customer paid")
    subtotal: float = Field(..., ge=0, description="Sum of item prices before tax/shipping")
    tax_amount: float = Field(..., ge=0, description="Tax charged")
    shipping_amount: float = Field(..., ge=0, description="Shipping cost")
    discount_amount: float = Field(0, ge=0, description="Discounts applied")
    total_amount: float = Field(..., ge=0, description="Final amount charged")
    currency: str = Field("EUR", description="Currency code (ISO 4217)")
    shipping_country: str = Field(..., description="Destination country")
    shipping_city: str = Field(..., description="Destination city")
    
    @validator('total_amount')
    def validate_total(cls, v, values):
        """Validate that total is calculated correctly."""
        expected = (
            values.get('subtotal', 0) + 
            values.get('tax_amount', 0) + 
            values.get('shipping_amount', 0) - 
            values.get('discount_amount', 0)
        )
        # Allow small floating point differences
        if abs(v - expected) > 0.01:
            pass  # In production, log discrepancy
        return v


class OrderItem(BaseModel):
    """
    Order line item - individual product within an order.
    
    An order can have multiple items (one-to-many relationship).
    """
    order_item_id: str = Field(..., description="Unique identifier for this line item")
    order_id: str = Field(..., description="Reference to the parent order")
    product_id: str = Field(..., description="Reference to the product")
    quantity: int = Field(..., ge=1, description="Number of units ordered")
    unit_price: float = Field(..., ge=0, description="Price per unit at time of order")
    discount_percent: float = Field(0, ge=0, le=100, description="Discount applied to this item")
    line_total: float = Field(..., ge=0, description="Total for this line item")
    
    @validator('line_total')
    def validate_line_total(cls, v, values):
        """Validate line total calculation."""
        expected = (
            values.get('quantity', 0) * 
            values.get('unit_price', 0) * 
            (1 - values.get('discount_percent', 0) / 100)
        )
        if abs(v - expected) > 0.01:
            pass  # In production, log discrepancy
        return v


# =============================================================================
# TRANSFORMED DATA MODELS (Silver/Gold layer structures)
# =============================================================================

class CleanedCustomer(BaseModel):
    """
    Cleaned and standardized customer record for the Silver layer.
    
    Additional fields added during transformation:
    - segment: Calculated customer segment
    - is_valid_email: Whether email format is valid
    """
    customer_id: str
    email: str
    email_domain: str  # Extracted from email
    first_name: str
    last_name: str
    full_name: str  # Concatenated name
    phone: Optional[str]
    country: str
    country_name: str  # Full country name
    city: str
    address: Optional[str]
    created_at: datetime
    updated_at: Optional[datetime]
    segment: CustomerSegment
    is_valid_email: bool
    # Metadata
    processed_at: datetime
    source_file: str


class DimCustomer(BaseModel):
    """
    Customer dimension table for the Gold layer star schema.
    
    Slowly Changing Dimension (SCD) Type 2 - we track historical changes.
    """
    customer_key: int  # Surrogate key for the dimension
    customer_id: str   # Natural/business key
    email: str
    full_name: str
    country: str
    city: str
    segment: CustomerSegment
    # SCD Type 2 fields
    effective_from: datetime
    effective_to: Optional[datetime]
    is_current: bool


class DimProduct(BaseModel):
    """Product dimension table for the Gold layer."""
    product_key: int
    product_id: str
    sku: str
    name: str
    category: str
    subcategory: Optional[str]
    brand: Optional[str]
    current_price: float
    cost: float
    margin_percent: float  # Calculated: (price - cost) / price * 100
    is_active: bool


class DimDate(BaseModel):
    """
    Date dimension table - pre-populated with all dates.
    
    This is a standard dimensional modeling pattern.
    Having a date dimension allows for flexible time-based analysis.
    """
    date_key: int  # YYYYMMDD format, e.g., 20231215
    date: datetime
    day_of_week: int  # 1-7 (Monday-Sunday)
    day_of_week_name: str  # Monday, Tuesday, etc.
    day_of_month: int  # 1-31
    day_of_year: int  # 1-366
    week_of_year: int  # 1-53
    month: int  # 1-12
    month_name: str  # January, February, etc.
    quarter: int  # 1-4
    year: int
    is_weekend: bool
    is_holiday: bool  # Would need holiday calendar


class FactSales(BaseModel):
    """
    Sales fact table - the central table in our star schema.
    
    Contains measures (quantitative data) and foreign keys to dimensions.
    Grain: One row per order item (most detailed level).
    """
    sale_key: int  # Surrogate key
    # Foreign keys to dimensions
    date_key: int
    customer_key: int
    product_key: int
    # Degenerate dimension (dimension without a separate table)
    order_id: str
    order_item_id: str
    # Measures (facts)
    quantity: int
    unit_price: float
    discount_amount: float
    tax_amount: float
    shipping_amount: float
    gross_revenue: float  # quantity * unit_price
    net_revenue: float    # gross_revenue - discount_amount
    cost_of_goods: float  # quantity * product cost
    profit: float         # net_revenue - cost_of_goods
