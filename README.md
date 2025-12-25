# E-commerce Data Pipeline with AWS Glue

A production-ready data engineering project demonstrating ETL pipelines using AWS Glue, PySpark, and Terraform.

## Architecture



<img width="1468" height="1291" alt="image" src="https://github.com/user-attachments/assets/9600ffc9-7714-4ad2-bc9c-9573049cbdc3" />


## Project Structure

```
data_engineering_project_ecommerce/
│
├── data/                          # Sample data for local testing
│   ├── raw/                       # Raw CSV files
│   │   ├── orders.csv
│   │   ├── products.csv
│   │   └── customers.csv
│   └── generated/                 # Output from data generator
│
├── src/                           # Source code
│   ├── data_generator/            # Scripts to generate sample data
│   │   ├── __init__.py
│   │   ├── generator.py           # Main data generation logic
│   │   └── schemas.py             # Data schemas/models
│   │
│   ├── glue_jobs/                 # AWS Glue ETL scripts
│   │   ├── bronze/                # Raw data ingestion
│   │   │   └── ingest_raw_data.py
│   │   ├── silver/                # Data cleaning & transformation
│   │   │   └── transform_to_silver.py
│   │   └── gold/                  # Business aggregations
│   │       ├── fact_sales.py
│   │       └── dim_tables.py
│   │
│   ├── quality/                   # Data quality checks
│   │   ├── __init__.py
│   │   └── validators.py
│   │
│   └── utils/                     # Utility functions
│       ├── __init__.py
│       ├── spark_utils.py
│       └── s3_utils.py
│
├── terraform/                     # Infrastructure as Code
│   ├── main.tf                    # Main Terraform configuration
│   ├── variables.tf               # Input variables
│   ├── outputs.tf                 # Output values
│   ├── s3.tf                      # S3 bucket definitions
│   ├── glue.tf                    # Glue resources
│   ├── iam.tf                     # IAM roles and policies
│   └── terraform.tfvars.example   # Example variable values
│
├── tests/                         # Unit and integration tests
│   ├── __init__.py
│   ├── test_data_generator.py
│   ├── test_transformations.py
│   └── conftest.py                # Pytest fixtures
│
├── config/                        # Configuration files
│   ├── glue_job_config.json       # Glue job parameters
│   └── pipeline_config.yaml       # Pipeline configuration
│
├── scripts/                       # Utility scripts
│   ├── deploy.sh                  # Deployment script
│   ├── run_local.py               # Run ETL locally
│   └── upload_to_s3.py            # Upload data to S3
│
├── requirements.txt               # Python dependencies
├── requirements-dev.txt           # Development dependencies
├── .gitignore                     # Git ignore rules
└── README.md                      # This file
```

## Medallion Architecture Explained

### Bronze Layer (Raw)
- **Purpose**: Store data exactly as received from source systems
- **Transformations**: Minimal - only add metadata (ingestion timestamp, source file)
- **Format**: Parquet, partitioned by ingestion date
- **Use Case**: Data lineage, reprocessing, debugging

### Silver Layer (Cleaned)
- **Purpose**: Cleaned, validated, and standardized data
- **Transformations**: 
  - Data type casting
  - Null handling
  - Deduplication
  - Schema enforcement
- **Format**: Parquet, partitioned by business date
- **Use Case**: Ad-hoc analysis, ML feature engineering

### Gold Layer (Business)
- **Purpose**: Business-ready aggregations and metrics
- **Transformations**:
  - Joins across tables
  - Aggregations (daily, weekly, monthly)
  - KPI calculations
- **Format**: Parquet, optimized for query patterns
- **Use Case**: Dashboards, reports, business decisions

## Getting Started

### Prerequisites
- Python 3.9+
- AWS CLI configured with appropriate credentials
- Terraform 1.0+
- Docker (optional, for local Glue development)

### Local Development

1. **Clone and setup environment**
```bash
git clone <repository-url>
cd data_engineering_project_ecommerce

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

2. **Generate sample data**
```bash
python -m src.data_generator.generator --output data/raw --records 10000
```

3. **Run ETL locally**
```bash
python scripts/run_local.py --layer bronze
python scripts/run_local.py --layer silver
python scripts/run_local.py --layer gold
```

### AWS Deployment

1. **Initialize Terraform**
```bash
cd terraform
terraform init
```

2. **Review and apply infrastructure**
```bash
terraform plan
terraform apply
```

3. **Upload data to S3**
```bash
python scripts/upload_to_s3.py --bucket your-bucket-name --source data/raw
```

4. **Trigger Glue jobs**
```bash
aws glue start-job-run --job-name ecommerce-bronze-ingestion
```

## Data Model

### Source Tables

| Table | Description | Key Fields |
|-------|-------------|------------|
| `orders` | Customer orders | order_id, customer_id, order_date, total_amount |
| `products` | Product catalog | product_id, name, category, price |
| `customers` | Customer information | customer_id, email, country, created_at |
| `order_items` | Order line items | order_id, product_id, quantity, unit_price |

### Gold Layer - Star Schema

```
                    ┌──────────────────┐
                    │   dim_date       │
                    │   ──────────     │
                    │   date_key (PK)  │
                    │   date           │
                    │   day_of_week    │
                    │   month          │
                    │   quarter        │
                    │   year           │
                    └────────┬─────────┘
                             │
┌──────────────────┐         │         ┌──────────────────┐
│   dim_customer   │         │         │   dim_product    │
│   ────────────   │         │         │   ───────────    │
│   customer_key   │◄────────┼────────►│   product_key    │
│   customer_id    │         │         │   product_id     │
│   email          │         │         │   name           │
│   country        │    ┌────┴────┐    │   category       │
│   segment        │    │         │    │   price          │
└──────────────────┘    │  fact_  │    └──────────────────┘
                        │  sales  │
                        │─────────│
                        │sale_key │
                        │date_key │
                        │customer_│
                        │  key    │
                        │product_ │
                        │  key    │
                        │quantity │
                        │revenue  │
                        │discount │
                        └─────────┘
```

## Testing

```bash
# Run all tests
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=src --cov-report=html

# Run specific test file
pytest tests/test_transformations.py -v
```

## Key Metrics Generated

| Metric | Description | Grain |
|--------|-------------|-------|
| `daily_revenue` | Total revenue per day | Daily |
| `orders_per_customer` | Average orders per customer | Customer |
| `top_products` | Best selling products | Product |
| `customer_lifetime_value` | Total spend per customer | Customer |
| `category_performance` | Revenue by category | Category/Month |

## Security Considerations

- S3 buckets are encrypted at rest (SSE-S3)
- Glue jobs use IAM roles with least-privilege access
- No hardcoded credentials - use AWS Secrets Manager
- VPC endpoints for private connectivity (optional)

## Learning Resources

- [AWS Glue Documentation](https://docs.aws.amazon.com/glue/)
- [PySpark Guide](https://spark.apache.org/docs/latest/api/python/)
- [Terraform AWS Provider](https://registry.terraform.io/providers/hashicorp/aws/latest/docs)
- [Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)
## Local Development Notes

**Windows users:** PySpark requires Hadoop binaries (`winutils.exe`). The ETL code is designed for AWS Glue (Linux-based) where this isn't needed. To run locally on Windows, either:
- Set up WSL (Windows Subsystem for Linux)
- Download winutils.exe and set `HADOOP_HOME`

The data generator works on all platforms without additional setup.
## License

MIT License - feel free to use this project for learning and development.
