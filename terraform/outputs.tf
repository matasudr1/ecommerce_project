# =============================================================================
# TERRAFORM OUTPUTS
# =============================================================================
#
# Outputs are values that Terraform displays after `terraform apply`.
# They're useful for:
# - Showing important resource identifiers
# - Passing values to other Terraform configurations
# - Integration with CI/CD pipelines
#
# Access outputs with: terraform output <output_name>
# Get all outputs as JSON: terraform output -json
#
# =============================================================================

# -----------------------------------------------------------------------------
# S3 Buckets
# -----------------------------------------------------------------------------

output "data_lake_bucket_name" {
  description = "Name of the data lake S3 bucket"
  value       = aws_s3_bucket.data_lake.id
}

output "data_lake_bucket_arn" {
  description = "ARN of the data lake S3 bucket"
  value       = aws_s3_bucket.data_lake.arn
}

output "scripts_bucket_name" {
  description = "Name of the scripts S3 bucket"
  value       = aws_s3_bucket.scripts.id
}

# Data locations
output "s3_paths" {
  description = "S3 paths for each data layer"
  value = {
    raw    = "s3://${aws_s3_bucket.data_lake.id}/raw/"
    bronze = "s3://${aws_s3_bucket.data_lake.id}/bronze/"
    silver = "s3://${aws_s3_bucket.data_lake.id}/silver/"
    gold   = "s3://${aws_s3_bucket.data_lake.id}/gold/"
  }
}

# -----------------------------------------------------------------------------
# Glue Resources
# -----------------------------------------------------------------------------

output "glue_database_name" {
  description = "Name of the Glue Data Catalog database"
  value       = aws_glue_catalog_database.ecommerce.name
}

output "glue_jobs" {
  description = "Map of Glue job names"
  value = {
    bronze_ingestion      = aws_glue_job.bronze_ingestion.name
    silver_transformation = aws_glue_job.silver_transformation.name
    gold_dimensions       = aws_glue_job.gold_dimensions.name
    gold_fact_sales       = aws_glue_job.gold_fact_sales.name
  }
}

output "glue_workflow_name" {
  description = "Name of the main ETL workflow"
  value       = aws_glue_workflow.etl_pipeline.name
}

# -----------------------------------------------------------------------------
# IAM
# -----------------------------------------------------------------------------

output "glue_role_arn" {
  description = "ARN of the Glue job IAM role"
  value       = aws_iam_role.glue_job_role.arn
}

# -----------------------------------------------------------------------------
# Useful Commands
# -----------------------------------------------------------------------------

output "useful_commands" {
  description = "Helpful AWS CLI commands for this deployment"
  value = {
    upload_data = "aws s3 cp data/raw/ s3://${aws_s3_bucket.data_lake.id}/raw/ --recursive"
    
    start_workflow = "aws glue start-workflow-run --name ${aws_glue_workflow.etl_pipeline.name}"
    
    start_bronze_job = "aws glue start-job-run --job-name ${aws_glue_job.bronze_ingestion.name}"
    
    check_job_status = "aws glue get-job-runs --job-name ${aws_glue_job.bronze_ingestion.name} --max-results 1"
    
    query_with_athena = "Use Athena console to query database: ${aws_glue_catalog_database.ecommerce.name}"
  }
}

# -----------------------------------------------------------------------------
# Environment Info
# -----------------------------------------------------------------------------

output "environment_info" {
  description = "Information about this deployment"
  value = {
    project     = var.project_name
    environment = var.environment
    region      = var.aws_region
    account_id  = data.aws_caller_identity.current.account_id
  }
}
