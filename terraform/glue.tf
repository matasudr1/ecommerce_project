# =============================================================================
# AWS GLUE RESOURCES
# =============================================================================
#
# AWS Glue is a serverless ETL (Extract, Transform, Load) service.
#
# Key Components:
# - Data Catalog: Metadata store (like a Hive metastore)
# - Crawlers: Auto-discover schema from data
# - Jobs: Run ETL scripts (PySpark)
# - Triggers: Schedule or event-based job execution
# - Workflows: Orchestrate multiple jobs
#
# Why Glue?
# - Serverless: No infrastructure to manage
# - Integrated: Works with S3, Athena, Redshift, etc.
# - Scalable: Automatically scales workers
# - Cost-effective: Pay only for compute time used
#
# =============================================================================

# -----------------------------------------------------------------------------
# Glue Data Catalog Database
# -----------------------------------------------------------------------------
# The database is a container for tables in the Glue Data Catalog.
# Think of it like a schema in a traditional database.

resource "aws_glue_catalog_database" "ecommerce" {
  name        = local.glue_database
  description = "E-commerce data pipeline - ${var.environment} environment"
  
  # Optional: Data location hint
  location_uri = "s3://${aws_s3_bucket.data_lake.id}/"
}

# -----------------------------------------------------------------------------
# Glue Jobs - Bronze Layer
# -----------------------------------------------------------------------------

resource "aws_glue_job" "bronze_ingestion" {
  name        = "${local.name_prefix}-bronze-ingestion"
  description = "Ingest raw data to Bronze layer"
  role_arn    = aws_iam_role.glue_job_role.arn
  
  # Glue version determines Spark version
  # 4.0 = Spark 3.3, Python 3.10
  glue_version = var.glue_version
  
  # Worker configuration
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_number_of_workers
  
  # Timeout in minutes
  timeout = var.glue_timeout_minutes
  
  # Script location in S3
  command {
    name            = "glueetl"  # Standard Glue ETL job
    script_location = "s3://${aws_s3_bucket.scripts.id}/${aws_s3_object.bronze_script.key}"
    python_version  = "3"
  }
  
  # Job parameters (passed to the script)
  default_arguments = {
    # Standard Glue arguments
    "--job-language"               = "python"
    "--job-bookmark-option"        = var.enable_job_bookmarks ? "job-bookmark-enable" : "job-bookmark-disable"
    "--enable-metrics"             = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-spark-ui"            = "true"
    "--spark-event-logs-path"      = "s3://${aws_s3_bucket.data_lake.id}/spark-logs/"
    
    # Custom arguments for our script
    "--source_bucket"  = aws_s3_bucket.data_lake.id
    "--target_bucket"  = aws_s3_bucket.data_lake.id
    "--tables"         = join(",", var.tables)
    
    # Additional Spark configuration
    "--conf" = "spark.sql.sources.partitionOverwriteMode=dynamic"
  }
  
  # Execution properties
  execution_property {
    max_concurrent_runs = 1  # Only one instance at a time
  }
  
  tags = {
    Layer = "bronze"
  }
}

# -----------------------------------------------------------------------------
# Glue Jobs - Silver Layer
# -----------------------------------------------------------------------------

resource "aws_glue_job" "silver_transformation" {
  name        = "${local.name_prefix}-silver-transformation"
  description = "Transform data from Bronze to Silver layer"
  role_arn    = aws_iam_role.glue_job_role.arn
  
  glue_version      = var.glue_version
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_number_of_workers
  timeout           = var.glue_timeout_minutes
  
  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.scripts.id}/${aws_s3_object.silver_script.key}"
    python_version  = "3"
  }
  
  default_arguments = {
    "--job-language"               = "python"
    "--job-bookmark-option"        = "job-bookmark-disable"  # Silver does full refresh
    "--enable-metrics"             = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-spark-ui"            = "true"
    "--spark-event-logs-path"      = "s3://${aws_s3_bucket.data_lake.id}/spark-logs/"
    
    "--source_bucket"    = aws_s3_bucket.data_lake.id
    "--target_bucket"    = aws_s3_bucket.data_lake.id
    "--tables"           = join(",", var.tables)
    "--processing_date"  = ""  # Will be set at runtime
  }
  
  execution_property {
    max_concurrent_runs = 1
  }
  
  tags = {
    Layer = "silver"
  }
}

# -----------------------------------------------------------------------------
# Glue Jobs - Gold Layer
# -----------------------------------------------------------------------------

resource "aws_glue_job" "gold_fact_sales" {
  name        = "${local.name_prefix}-gold-fact-sales"
  description = "Build fact_sales table in Gold layer"
  role_arn    = aws_iam_role.glue_job_role.arn
  
  glue_version      = var.glue_version
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_number_of_workers
  timeout           = var.glue_timeout_minutes
  
  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.scripts.id}/${aws_s3_object.gold_fact_script.key}"
    python_version  = "3"
  }
  
  default_arguments = {
    "--job-language"               = "python"
    "--job-bookmark-option"        = "job-bookmark-disable"
    "--enable-metrics"             = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    
    "--silver_bucket" = aws_s3_bucket.data_lake.id
    "--gold_bucket"   = aws_s3_bucket.data_lake.id
  }
  
  execution_property {
    max_concurrent_runs = 1
  }
  
  tags = {
    Layer = "gold"
  }
}

resource "aws_glue_job" "gold_dimensions" {
  name        = "${local.name_prefix}-gold-dimensions"
  description = "Build dimension tables in Gold layer"
  role_arn    = aws_iam_role.glue_job_role.arn
  
  glue_version      = var.glue_version
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_number_of_workers
  timeout           = var.glue_timeout_minutes
  
  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.scripts.id}/${aws_s3_object.gold_dim_script.key}"
    python_version  = "3"
  }
  
  default_arguments = {
    "--job-language"               = "python"
    "--job-bookmark-option"        = "job-bookmark-disable"
    "--enable-metrics"             = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    
    "--silver_bucket" = aws_s3_bucket.data_lake.id
    "--gold_bucket"   = aws_s3_bucket.data_lake.id
  }
  
  execution_property {
    max_concurrent_runs = 1
  }
  
  tags = {
    Layer = "gold"
  }
}

# -----------------------------------------------------------------------------
# Glue Workflow - Orchestrate the Pipeline
# -----------------------------------------------------------------------------
# A workflow coordinates multiple jobs to run in sequence

resource "aws_glue_workflow" "etl_pipeline" {
  name        = "${local.name_prefix}-etl-pipeline"
  description = "Main ETL pipeline: Bronze -> Silver -> Gold"
  
  # Maximum concurrent runs
  max_concurrent_runs = 1
}

# -----------------------------------------------------------------------------
# Glue Triggers - Define job execution order
# -----------------------------------------------------------------------------

# Trigger 1: Start the pipeline (on-demand or scheduled)
resource "aws_glue_trigger" "start_pipeline" {
  name          = "${local.name_prefix}-start-pipeline"
  type          = "ON_DEMAND"  # Manual trigger; change to SCHEDULED for auto
  workflow_name = aws_glue_workflow.etl_pipeline.name
  
  actions {
    job_name = aws_glue_job.bronze_ingestion.name
  }
}

# Trigger 2: After Bronze completes, start Silver
resource "aws_glue_trigger" "bronze_to_silver" {
  name          = "${local.name_prefix}-bronze-to-silver"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.etl_pipeline.name
  
  predicate {
    conditions {
      job_name = aws_glue_job.bronze_ingestion.name
      state    = "SUCCEEDED"
    }
  }
  
  actions {
    job_name = aws_glue_job.silver_transformation.name
  }
}

# Trigger 3: After Silver completes, start Gold (dimensions first)
resource "aws_glue_trigger" "silver_to_gold_dims" {
  name          = "${local.name_prefix}-silver-to-gold-dims"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.etl_pipeline.name
  
  predicate {
    conditions {
      job_name = aws_glue_job.silver_transformation.name
      state    = "SUCCEEDED"
    }
  }
  
  actions {
    job_name = aws_glue_job.gold_dimensions.name
  }
}

# Trigger 4: After dimensions, build fact table
resource "aws_glue_trigger" "gold_dims_to_facts" {
  name          = "${local.name_prefix}-gold-dims-to-facts"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.etl_pipeline.name
  
  predicate {
    conditions {
      job_name = aws_glue_job.gold_dimensions.name
      state    = "SUCCEEDED"
    }
  }
  
  actions {
    job_name = aws_glue_job.gold_fact_sales.name
  }
}

# -----------------------------------------------------------------------------
# Glue Crawlers (Optional)
# -----------------------------------------------------------------------------
# Crawlers automatically discover schema from data.
# They're useful but can be expensive if run frequently.

resource "aws_glue_crawler" "silver_crawler" {
  count = var.enable_crawler ? 1 : 0
  
  name          = "${local.name_prefix}-silver-crawler"
  role          = aws_iam_role.glue_crawler_role[0].arn
  database_name = aws_glue_catalog_database.ecommerce.name
  
  s3_target {
    path = "s3://${aws_s3_bucket.data_lake.id}/silver/"
  }
  
  # Schedule (cron expression)
  schedule = var.crawler_schedule
  
  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }
  
  configuration = jsonencode({
    Version = 1.0
    Grouping = {
      TableGroupingPolicy = "CombineCompatibleSchemas"
    }
  })
}

resource "aws_glue_crawler" "gold_crawler" {
  count = var.enable_crawler ? 1 : 0
  
  name          = "${local.name_prefix}-gold-crawler"
  role          = aws_iam_role.glue_crawler_role[0].arn
  database_name = aws_glue_catalog_database.ecommerce.name
  
  s3_target {
    path = "s3://${aws_s3_bucket.data_lake.id}/gold/"
  }
  
  schedule = var.crawler_schedule
  
  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }
}
