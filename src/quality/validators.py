"""
Data Quality Validators

This module contains data quality checks for our e-commerce data pipeline.
Data quality is crucial because:
- Garbage in = Garbage out
- Bad data leads to bad business decisions
- Early detection prevents downstream failures

We implement checks at multiple levels:
1. Schema validation (correct columns and types)
2. Null checks (required fields present)
3. Range checks (values within expected bounds)
4. Referential integrity (foreign keys exist)
5. Business rules (custom logic)
"""

from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass
from enum import Enum
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
import logging

logger = logging.getLogger(__name__)


class CheckSeverity(Enum):
    """Severity levels for data quality issues."""
    WARNING = "warning"   # Log but continue
    ERROR = "error"       # Fail the job
    INFO = "info"         # Informational only


@dataclass
class QualityCheckResult:
    """Result of a data quality check."""
    check_name: str
    passed: bool
    severity: CheckSeverity
    message: str
    failed_count: int = 0
    total_count: int = 0
    failed_percentage: float = 0.0
    
    def __str__(self):
        status = "✅ PASSED" if self.passed else "❌ FAILED"
        return (f"{status} [{self.severity.value.upper()}] {self.check_name}: "
                f"{self.message} ({self.failed_count}/{self.total_count} = "
                f"{self.failed_percentage:.2f}%)")


class DataQualityValidator:
    """
    Data quality validator for DataFrames.
    
    Usage:
        validator = DataQualityValidator(df)
        results = validator.run_all_checks()
        
        if not validator.all_passed():
            raise DataQualityError("Quality checks failed")
    """
    
    def __init__(self, df: DataFrame, table_name: str = "unknown"):
        self.df = df
        self.table_name = table_name
        self.results: List[QualityCheckResult] = []
        self._total_count = None
    
    @property
    def total_count(self) -> int:
        """Lazily compute and cache total row count."""
        if self._total_count is None:
            self._total_count = self.df.count()
        return self._total_count
    
    def check_not_null(
        self,
        columns: List[str],
        severity: CheckSeverity = CheckSeverity.ERROR
    ) -> List[QualityCheckResult]:
        """
        Check that specified columns have no null values.
        
        Args:
            columns: Column names to check
            severity: How to treat failures
            
        Returns:
            List of check results
        """
        results = []
        
        for col_name in columns:
            if col_name not in self.df.columns:
                results.append(QualityCheckResult(
                    check_name=f"not_null_{col_name}",
                    passed=False,
                    severity=CheckSeverity.ERROR,
                    message=f"Column '{col_name}' does not exist in DataFrame"
                ))
                continue
            
            null_count = self.df.filter(F.col(col_name).isNull()).count()
            passed = null_count == 0
            
            result = QualityCheckResult(
                check_name=f"not_null_{col_name}",
                passed=passed,
                severity=severity,
                message=f"Null check for '{col_name}'",
                failed_count=null_count,
                total_count=self.total_count,
                failed_percentage=(null_count / self.total_count * 100 
                                   if self.total_count > 0 else 0)
            )
            results.append(result)
            self.results.append(result)
        
        return results
    
    def check_unique(
        self,
        columns: List[str],
        severity: CheckSeverity = CheckSeverity.ERROR
    ) -> QualityCheckResult:
        """
        Check that specified columns form a unique key.
        
        Args:
            columns: Columns that should be unique together
            severity: How to treat failures
            
        Returns:
            Check result
        """
        distinct_count = self.df.select(columns).distinct().count()
        duplicate_count = self.total_count - distinct_count
        passed = duplicate_count == 0
        
        result = QualityCheckResult(
            check_name=f"unique_{'+'.join(columns)}",
            passed=passed,
            severity=severity,
            message=f"Uniqueness check for {columns}",
            failed_count=duplicate_count,
            total_count=self.total_count,
            failed_percentage=(duplicate_count / self.total_count * 100 
                               if self.total_count > 0 else 0)
        )
        self.results.append(result)
        return result
    
    def check_values_in_set(
        self,
        column: str,
        valid_values: List,
        severity: CheckSeverity = CheckSeverity.ERROR
    ) -> QualityCheckResult:
        """
        Check that column values are within a set of valid values.
        
        Args:
            column: Column to check
            valid_values: List of allowed values
            severity: How to treat failures
            
        Returns:
            Check result
        """
        invalid_count = self.df.filter(~F.col(column).isin(valid_values)).count()
        passed = invalid_count == 0
        
        result = QualityCheckResult(
            check_name=f"valid_values_{column}",
            passed=passed,
            severity=severity,
            message=f"Values in '{column}' must be one of {valid_values}",
            failed_count=invalid_count,
            total_count=self.total_count,
            failed_percentage=(invalid_count / self.total_count * 100 
                               if self.total_count > 0 else 0)
        )
        self.results.append(result)
        return result
    
    def check_range(
        self,
        column: str,
        min_value: Optional[float] = None,
        max_value: Optional[float] = None,
        severity: CheckSeverity = CheckSeverity.ERROR
    ) -> QualityCheckResult:
        """
        Check that numeric column values fall within a range.
        
        Args:
            column: Column to check
            min_value: Minimum allowed value (inclusive)
            max_value: Maximum allowed value (inclusive)
            severity: How to treat failures
            
        Returns:
            Check result
        """
        condition = F.lit(True)
        
        if min_value is not None:
            condition = condition & (F.col(column) >= min_value)
        if max_value is not None:
            condition = condition & (F.col(column) <= max_value)
        
        invalid_count = self.df.filter(~condition).count()
        passed = invalid_count == 0
        
        range_desc = f"[{min_value}, {max_value}]"
        
        result = QualityCheckResult(
            check_name=f"range_{column}",
            passed=passed,
            severity=severity,
            message=f"Values in '{column}' must be in range {range_desc}",
            failed_count=invalid_count,
            total_count=self.total_count,
            failed_percentage=(invalid_count / self.total_count * 100 
                               if self.total_count > 0 else 0)
        )
        self.results.append(result)
        return result
    
    def check_referential_integrity(
        self,
        column: str,
        reference_df: DataFrame,
        reference_column: str,
        severity: CheckSeverity = CheckSeverity.ERROR
    ) -> QualityCheckResult:
        """
        Check that foreign key values exist in reference table.
        
        Args:
            column: Foreign key column in this DataFrame
            reference_df: Reference DataFrame
            reference_column: Primary key column in reference DataFrame
            severity: How to treat failures
            
        Returns:
            Check result
        """
        # Get distinct foreign key values
        fk_values = self.df.select(column).distinct()
        
        # Get distinct primary key values
        pk_values = reference_df.select(reference_column).distinct()
        
        # Find orphan records (FK values not in PK)
        orphans = fk_values.join(
            pk_values,
            fk_values[column] == pk_values[reference_column],
            "left_anti"
        )
        
        orphan_count = orphans.count()
        passed = orphan_count == 0
        
        result = QualityCheckResult(
            check_name=f"ref_integrity_{column}",
            passed=passed,
            severity=severity,
            message=f"Foreign key '{column}' must exist in reference table",
            failed_count=orphan_count,
            total_count=self.total_count,
            failed_percentage=(orphan_count / self.total_count * 100 
                               if self.total_count > 0 else 0)
        )
        self.results.append(result)
        return result
    
    def check_row_count(
        self,
        min_count: int = 1,
        max_count: Optional[int] = None,
        severity: CheckSeverity = CheckSeverity.ERROR
    ) -> QualityCheckResult:
        """
        Check that DataFrame has expected number of rows.
        
        Args:
            min_count: Minimum expected rows
            max_count: Maximum expected rows (optional)
            severity: How to treat failures
            
        Returns:
            Check result
        """
        count = self.total_count
        passed = count >= min_count
        if max_count is not None:
            passed = passed and count <= max_count
        
        range_desc = f">= {min_count}"
        if max_count is not None:
            range_desc = f"[{min_count}, {max_count}]"
        
        result = QualityCheckResult(
            check_name="row_count",
            passed=passed,
            severity=severity,
            message=f"Row count ({count}) must be {range_desc}",
            failed_count=0 if passed else 1,
            total_count=count
        )
        self.results.append(result)
        return result
    
    def check_freshness(
        self,
        timestamp_column: str,
        max_age_hours: int = 24,
        severity: CheckSeverity = CheckSeverity.WARNING
    ) -> QualityCheckResult:
        """
        Check that data is recent (not stale).
        
        Args:
            timestamp_column: Column containing timestamps
            max_age_hours: Maximum acceptable age in hours
            severity: How to treat failures
            
        Returns:
            Check result
        """
        # Get the most recent timestamp
        max_ts = self.df.agg(F.max(timestamp_column)).collect()[0][0]
        
        if max_ts is None:
            passed = False
            message = "No timestamps found in data"
        else:
            from datetime import datetime, timezone
            now = datetime.now(timezone.utc)
            age_hours = (now - max_ts.replace(tzinfo=timezone.utc)).total_seconds() / 3600
            passed = age_hours <= max_age_hours
            message = f"Most recent data is {age_hours:.1f} hours old (max: {max_age_hours})"
        
        result = QualityCheckResult(
            check_name=f"freshness_{timestamp_column}",
            passed=passed,
            severity=severity,
            message=message,
            total_count=self.total_count
        )
        self.results.append(result)
        return result
    
    def all_passed(self, include_warnings: bool = False) -> bool:
        """
        Check if all quality checks passed.
        
        Args:
            include_warnings: If True, warnings count as failures
            
        Returns:
            True if all checks passed
        """
        for result in self.results:
            if not result.passed:
                if result.severity == CheckSeverity.ERROR:
                    return False
                if include_warnings and result.severity == CheckSeverity.WARNING:
                    return False
        return True
    
    def get_summary(self) -> str:
        """Get a summary of all check results."""
        lines = [f"Data Quality Report for {self.table_name}"]
        lines.append("=" * 50)
        lines.append(f"Total rows: {self.total_count}")
        lines.append("")
        
        passed_count = sum(1 for r in self.results if r.passed)
        failed_count = len(self.results) - passed_count
        
        lines.append(f"Checks passed: {passed_count}/{len(self.results)}")
        lines.append("")
        
        for result in self.results:
            lines.append(str(result))
        
        return "\n".join(lines)
    
    def log_results(self):
        """Log all results using the logging module."""
        logger.info(f"Data Quality Results for {self.table_name}")
        
        for result in self.results:
            if result.passed:
                logger.info(str(result))
            elif result.severity == CheckSeverity.WARNING:
                logger.warning(str(result))
            else:
                logger.error(str(result))


# =============================================================================
# PRE-BUILT VALIDATION SUITES
# =============================================================================

def validate_customers(df: DataFrame) -> DataQualityValidator:
    """Run standard validation suite for customers table."""
    validator = DataQualityValidator(df, "customers")
    
    # Required fields
    validator.check_not_null(["customer_id", "email", "country"])
    
    # Uniqueness
    validator.check_unique(["customer_id"])
    validator.check_unique(["email"], severity=CheckSeverity.WARNING)
    
    # Row count (expect at least some customers)
    validator.check_row_count(min_count=1)
    
    return validator


def validate_products(df: DataFrame) -> DataQualityValidator:
    """Run standard validation suite for products table."""
    validator = DataQualityValidator(df, "products")
    
    # Required fields
    validator.check_not_null(["product_id", "name", "price", "category"])
    
    # Uniqueness
    validator.check_unique(["product_id"])
    validator.check_unique(["sku"], severity=CheckSeverity.WARNING)
    
    # Value ranges
    validator.check_range("price", min_value=0)
    validator.check_range("cost", min_value=0)
    validator.check_range("margin_percent", min_value=-100, max_value=100,
                          severity=CheckSeverity.WARNING)
    
    return validator


def validate_orders(df: DataFrame, customers_df: DataFrame) -> DataQualityValidator:
    """Run standard validation suite for orders table."""
    validator = DataQualityValidator(df, "orders")
    
    # Required fields
    validator.check_not_null(["order_id", "customer_id", "order_date", "total_amount"])
    
    # Uniqueness
    validator.check_unique(["order_id"])
    
    # Value ranges
    validator.check_range("total_amount", min_value=0)
    validator.check_range("subtotal", min_value=0)
    
    # Valid status values
    validator.check_values_in_set(
        "status",
        ["pending", "confirmed", "shipped", "delivered", "cancelled", "returned"]
    )
    
    # Referential integrity
    validator.check_referential_integrity(
        "customer_id", customers_df, "customer_id"
    )
    
    return validator


def validate_order_items(
    df: DataFrame, 
    orders_df: DataFrame,
    products_df: DataFrame
) -> DataQualityValidator:
    """Run standard validation suite for order_items table."""
    validator = DataQualityValidator(df, "order_items")
    
    # Required fields
    validator.check_not_null(["order_item_id", "order_id", "product_id", "quantity"])
    
    # Uniqueness
    validator.check_unique(["order_item_id"])
    
    # Value ranges
    validator.check_range("quantity", min_value=1)
    validator.check_range("unit_price", min_value=0)
    validator.check_range("discount_percent", min_value=0, max_value=100)
    
    # Referential integrity
    validator.check_referential_integrity("order_id", orders_df, "order_id")
    validator.check_referential_integrity("product_id", products_df, "product_id")
    
    return validator
