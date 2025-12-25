# 🎓 E-commerce Data Pipeline - Learning Agenda

## How This Course Works

We'll go through this project **exactly how a real data engineer would build it** - from the first "I need to solve this problem" thought to the final deployed solution.

Each section builds on the previous one. No jumping ahead!

---

## 📋 Learning Path

### **Module 1: Understanding the Problem (30 min)**
> *"Before writing any code, understand what you're building and why"*

- [ ] 1.1 What is a Data Pipeline? (Real-world analogy)
- [ ] 1.2 What business problem are we solving?
- [ ] 1.3 Who will use this data? (Analysts, dashboards, ML)
- [ ] 1.4 What questions should the data answer?
- [ ] 1.5 Defining success criteria

---

### **Module 2: Designing the Architecture (45 min)**
> *"Plan before you code - architecture decisions are expensive to change"*

- [ ] 2.1 What is a Data Lake vs Data Warehouse?
- [ ] 2.2 The Medallion Architecture (Bronze/Silver/Gold) explained
  - Why three layers?
  - What happens in each layer?
  - Real-world examples
- [ ] 2.3 Choosing file formats (CSV vs Parquet vs Delta)
- [ ] 2.4 Partitioning strategy (why and how)
- [ ] 2.5 Drawing our architecture diagram
- [ ] 2.6 Choosing AWS services (Why Glue? Why S3?)

---

### **Module 3: Project Setup & Structure (30 min)**
> *"A well-organized project is easier to maintain and scale"*

- [ ] 3.1 Creating the folder structure
- [ ] 3.2 Python project best practices
- [ ] 3.3 Understanding `requirements.txt`
- [ ] 3.4 The purpose of `__init__.py` files
- [ ] 3.5 Configuration management (YAML, JSON)
- [ ] 3.6 Why `.gitignore` matters

---

### **Module 4: Data Modeling (45 min)**
> *"Know your data before you transform it"*

- [ ] 4.1 Understanding source data (customers, orders, products)
- [ ] 4.2 Entity-Relationship thinking
- [ ] 4.3 What is Pydantic and why use it for schemas?
- [ ] 4.4 Data types matter (strings, dates, decimals)
- [ ] 4.5 Handling relationships (foreign keys)
- [ ] 4.6 Designing the Star Schema (facts & dimensions)

---

### **Module 5: Creating Sample Data (30 min)**
> *"You can't build a pipeline without data to test it"*

- [ ] 5.1 Why generate fake data?
- [ ] 5.2 Using the Faker library
- [ ] 5.3 Making data realistic (distributions, patterns)
- [ ] 5.4 Maintaining referential integrity
- [ ] 5.5 Running the generator: `generator.py` walkthrough

---

### **Module 6: PySpark Fundamentals (1 hour)**
> *"The engine that powers our transformations"*

- [ ] 6.1 What is Spark? (Distributed computing explained simply)
- [ ] 6.2 SparkSession - your entry point
- [ ] 6.3 DataFrames vs SQL tables
- [ ] 6.4 Common operations: select, filter, join, groupBy
- [ ] 6.5 Transformations vs Actions (lazy evaluation)
- [ ] 6.6 Reading and writing data (CSV, Parquet)
- [ ] 6.7 Hands-on: Run some Spark code locally

---

### **Module 7: Building the Bronze Layer (45 min)**
> *"Capture raw data exactly as it arrives"*

- [ ] 7.1 Bronze layer philosophy
- [ ] 7.2 Why keep raw data unchanged?
- [ ] 7.3 Adding metadata columns (lineage)
- [ ] 7.4 Partitioning by ingestion date
- [ ] 7.5 Code walkthrough: `ingest_raw_data.py`
- [ ] 7.6 AWS Glue job structure explained

---

### **Module 8: Building the Silver Layer (1 hour)**
> *"Clean, validate, and standardize"*

- [ ] 8.1 Silver layer philosophy
- [ ] 8.2 Common data quality issues
- [ ] 8.3 Type casting (strings to dates, numbers)
- [ ] 8.4 Handling null values
- [ ] 8.5 Deduplication techniques
- [ ] 8.6 Data validation rules
- [ ] 8.7 Code walkthrough: `transform_to_silver.py`

---

### **Module 9: Building the Gold Layer (1 hour)**
> *"Business-ready data for analytics"*

- [ ] 9.1 Gold layer philosophy
- [ ] 9.2 Dimensional Modeling 101
  - Fact tables (measures)
  - Dimension tables (attributes)
  - Surrogate keys
- [ ] 9.3 Building dimension tables
- [ ] 9.4 Building fact tables
- [ ] 9.5 Pre-aggregated tables (why and when)
- [ ] 9.6 Code walkthrough: `dim_tables.py` and `fact_sales.py`

---

### **Module 10: Data Quality (45 min)**
> *"Bad data = bad decisions"*

- [ ] 10.1 Why data quality matters
- [ ] 10.2 Types of data quality checks
  - Completeness (nulls)
  - Uniqueness (duplicates)
  - Validity (allowed values)
  - Consistency (referential integrity)
- [ ] 10.3 Building a quality framework
- [ ] 10.4 Code walkthrough: `validators.py`
- [ ] 10.5 When to fail vs warn

---

### **Module 11: Testing Your Pipeline (45 min)**
> *"If it's not tested, it's broken"*

- [ ] 11.1 Why test data pipelines?
- [ ] 11.2 Types of tests (unit, integration)
- [ ] 11.3 pytest basics
- [ ] 11.4 Fixtures - reusable test data
- [ ] 11.5 Testing Spark transformations
- [ ] 11.6 Running tests locally

---

### **Module 12: Infrastructure as Code - Terraform (1 hour)**
> *"Define your cloud resources in code"*

- [ ] 12.1 What is Infrastructure as Code?
- [ ] 12.2 Why Terraform? (vs CloudFormation, Pulumi)
- [ ] 12.3 Terraform basics
  - Providers
  - Resources
  - Variables
  - Outputs
- [ ] 12.4 The Terraform workflow: init → plan → apply
- [ ] 12.5 S3 buckets in Terraform
- [ ] 12.6 IAM roles and policies explained
- [ ] 12.7 Glue jobs and workflows in Terraform
- [ ] 12.8 Code walkthrough: all `.tf` files

---

### **Module 13: Deployment & Running the Pipeline (30 min)**
> *"Ship it!"*

- [ ] 13.1 Deploying with Terraform
- [ ] 13.2 Uploading data to S3
- [ ] 13.3 Triggering Glue jobs
- [ ] 13.4 Monitoring job runs
- [ ] 13.5 Querying data with Athena
- [ ] 13.6 Common errors and debugging

---

### **Module 14: Next Steps & Best Practices (30 min)**
> *"Level up your skills"*

- [ ] 14.1 CI/CD for data pipelines
- [ ] 14.2 Monitoring and alerting
- [ ] 14.3 Cost optimization
- [ ] 14.4 Incremental processing (job bookmarks)
- [ ] 14.5 Real-time streaming (next level)
- [ ] 14.6 Resources for continued learning

---

## ⏱️ Estimated Total Time: ~10 hours

## 🚀 Ready to Start?

**Tell me: "Let's start with Module 1"** and we'll begin!

Or jump to any specific module if you already know some topics.

---

## Quick Reference: File → Module Mapping

| File | Covered In |
|------|------------|
| `src/data_generator/schemas.py` | Module 4 |
| `src/data_generator/generator.py` | Module 5 |
| `src/glue_jobs/bronze/*.py` | Module 7 |
| `src/glue_jobs/silver/*.py` | Module 8 |
| `src/glue_jobs/gold/*.py` | Module 9 |
| `src/quality/validators.py` | Module 10 |
| `tests/*.py` | Module 11 |
| `terraform/*.tf` | Module 12 |
