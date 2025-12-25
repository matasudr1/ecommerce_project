# =============================================================================
# S3 BUCKETS
# =============================================================================
#
# S3 (Simple Storage Service) is AWS's object storage.
# We use it as our Data Lake - storing all data at every stage.
#
# Bucket Organization (following data lake best practices):
# s3://bucket-name/
# ├── raw/              # Landing zone for incoming data (as-is)
# ├── bronze/           # Raw data in Parquet format
# ├── silver/           # Cleaned and validated data
# ├── gold/             # Business-ready aggregations
# └── scripts/          # Glue job scripts
#
# =============================================================================

# -----------------------------------------------------------------------------
# Main Data Lake Bucket
# -----------------------------------------------------------------------------

resource "aws_s3_bucket" "data_lake" {
  # Bucket names must be globally unique across ALL AWS accounts
  bucket = local.bucket_name
  
  # prevent accidental deletion (set to false to allow destroy)
  force_destroy = var.environment != "prod"
  
  tags = {
    Name = "Data Lake - ${var.project_name}"
    Type = "data-lake"
  }
}

# Enable versioning - keep history of all changes
# Critical for data recovery and audit trails
resource "aws_s3_bucket_versioning" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  
  versioning_configuration {
    status = "Enabled"
  }
}

# Server-side encryption - encrypt data at rest
# AWS manages the keys (SSE-S3), simplest option
resource "aws_s3_bucket_server_side_encryption_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"  # SSE-S3 encryption
    }
    bucket_key_enabled = true  # Reduces API costs
  }
}

# Block all public access - critical for data security!
resource "aws_s3_bucket_public_access_block" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Lifecycle rules - automatically manage data lifecycle
resource "aws_s3_bucket_lifecycle_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  
  # Rule 1: Move old raw data to cheaper storage
  rule {
    id     = "raw-data-lifecycle"
    status = "Enabled"
    
    filter {
      prefix = "raw/"
    }
    
    # After 30 days, move to Infrequent Access (cheaper)
    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
    
    # After 90 days, move to Glacier (even cheaper, but slow retrieval)
    transition {
      days          = 90
      storage_class = "GLACIER"
    }
    
    # Delete after retention period
    expiration {
      days = var.data_retention_days
    }
  }
  
  # Rule 2: Clean up incomplete multipart uploads
  rule {
    id     = "cleanup-incomplete-uploads"
    status = "Enabled"
    
    filter {
      prefix = ""
    }
    
    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
  
  # Rule 3: Delete old versions of objects
  rule {
    id     = "delete-old-versions"
    status = "Enabled"
    
    filter {
      prefix = ""
    }
    
    noncurrent_version_expiration {
      noncurrent_days = 30
    }
  }
}

# -----------------------------------------------------------------------------
# Bucket for Glue Scripts
# -----------------------------------------------------------------------------
# Keeping scripts separate is a best practice:
# - Different access patterns than data
# - Different retention requirements
# - Easier to manage permissions

resource "aws_s3_bucket" "scripts" {
  bucket = "${local.name_prefix}-scripts-${data.aws_caller_identity.current.account_id}"
  
  force_destroy = true  # OK to destroy scripts bucket
  
  tags = {
    Name = "Glue Scripts - ${var.project_name}"
    Type = "scripts"
  }
}

resource "aws_s3_bucket_versioning" "scripts" {
  bucket = aws_s3_bucket.scripts.id
  
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "scripts" {
  bucket = aws_s3_bucket.scripts.id
  
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "scripts" {
  bucket = aws_s3_bucket.scripts.id
  
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# -----------------------------------------------------------------------------
# Upload Glue Scripts to S3
# -----------------------------------------------------------------------------
# We upload our Python scripts to S3 so Glue can access them

resource "aws_s3_object" "bronze_script" {
  bucket = aws_s3_bucket.scripts.id
  key    = "glue-jobs/bronze/ingest_raw_data.py"
  source = "${path.module}/../src/glue_jobs/bronze/ingest_raw_data.py"
  etag   = filemd5("${path.module}/../src/glue_jobs/bronze/ingest_raw_data.py")
}

resource "aws_s3_object" "silver_script" {
  bucket = aws_s3_bucket.scripts.id
  key    = "glue-jobs/silver/transform_to_silver.py"
  source = "${path.module}/../src/glue_jobs/silver/transform_to_silver.py"
  etag   = filemd5("${path.module}/../src/glue_jobs/silver/transform_to_silver.py")
}

resource "aws_s3_object" "gold_fact_script" {
  bucket = aws_s3_bucket.scripts.id
  key    = "glue-jobs/gold/fact_sales.py"
  source = "${path.module}/../src/glue_jobs/gold/fact_sales.py"
  etag   = filemd5("${path.module}/../src/glue_jobs/gold/fact_sales.py")
}

resource "aws_s3_object" "gold_dim_script" {
  bucket = aws_s3_bucket.scripts.id
  key    = "glue-jobs/gold/dim_tables.py"
  source = "${path.module}/../src/glue_jobs/gold/dim_tables.py"
  etag   = filemd5("${path.module}/../src/glue_jobs/gold/dim_tables.py")
}

# -----------------------------------------------------------------------------
# Create folder structure in data lake
# -----------------------------------------------------------------------------
# S3 doesn't really have folders, but we create "folder markers"
# to make navigation easier in the console

resource "aws_s3_object" "folder_raw" {
  bucket  = aws_s3_bucket.data_lake.id
  key     = "raw/"
  content = ""
}

resource "aws_s3_object" "folder_bronze" {
  bucket  = aws_s3_bucket.data_lake.id
  key     = "bronze/"
  content = ""
}

resource "aws_s3_object" "folder_silver" {
  bucket  = aws_s3_bucket.data_lake.id
  key     = "silver/"
  content = ""
}

resource "aws_s3_object" "folder_gold" {
  bucket  = aws_s3_bucket.data_lake.id
  key     = "gold/"
  content = ""
}
