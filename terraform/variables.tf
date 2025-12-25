# =============================================================================
# INPUT VARIABLES
# =============================================================================
#
# Variables make your Terraform code reusable and configurable.
# Values can be set via:
# 1. terraform.tfvars file (most common)
# 2. Command line: terraform apply -var="environment=prod"
# 3. Environment variables: TF_VAR_environment=prod
#
# Variable types:
# - string: Text
# - number: Integers and floats
# - bool: true/false
# - list: Ordered collection [a, b, c]
# - map: Key-value pairs {key = value}
# - object: Complex structures
#
# =============================================================================

# -----------------------------------------------------------------------------
# Required Variables (no default, must be provided)
# -----------------------------------------------------------------------------

variable "project_name" {
  description = "Name of the project, used in resource naming"
  type        = string
  
  validation {
    condition     = length(var.project_name) > 0 && length(var.project_name) <= 20
    error_message = "Project name must be between 1 and 20 characters."
  }
}

variable "owner" {
  description = "Owner of the resources (email or team name)"
  type        = string
}

# -----------------------------------------------------------------------------
# Optional Variables (have defaults)
# -----------------------------------------------------------------------------

variable "aws_region" {
  description = "AWS region to deploy resources"
  type        = string
  default     = "eu-west-1"  # Ireland - good for European e-commerce
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "dev"
  
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be one of: dev, staging, prod."
  }
}

# -----------------------------------------------------------------------------
# Glue Job Configuration
# -----------------------------------------------------------------------------

variable "glue_version" {
  description = "AWS Glue version for jobs"
  type        = string
  default     = "4.0"  # Latest version with Spark 3.3
}

variable "glue_worker_type" {
  description = "Type of Glue worker to use"
  type        = string
  default     = "G.1X"  # 1 DPU = 4 vCPU, 16 GB RAM
  
  # Worker type options:
  # - Standard: Legacy, don't use
  # - G.1X: 1 DPU (4 vCPU, 16 GB)
  # - G.2X: 2 DPU (8 vCPU, 32 GB)
  # - G.025X: 0.25 DPU (for small jobs)
}

variable "glue_number_of_workers" {
  description = "Number of Glue workers for each job"
  type        = number
  default     = 2  # Minimum for distributed processing
  
  validation {
    condition     = var.glue_number_of_workers >= 2
    error_message = "Minimum number of workers is 2."
  }
}

variable "glue_timeout_minutes" {
  description = "Timeout for Glue jobs in minutes"
  type        = number
  default     = 60
}

# -----------------------------------------------------------------------------
# Data Pipeline Configuration
# -----------------------------------------------------------------------------

variable "tables" {
  description = "List of tables to process in the pipeline"
  type        = list(string)
  default     = ["customers", "products", "orders", "order_items"]
}

variable "enable_job_bookmarks" {
  description = "Enable Glue job bookmarks for incremental processing"
  type        = bool
  default     = true
}

variable "data_retention_days" {
  description = "Number of days to retain data in S3 (for lifecycle rules)"
  type        = number
  default     = 365
}

# -----------------------------------------------------------------------------
# Cost Control
# -----------------------------------------------------------------------------

variable "enable_crawler" {
  description = "Enable Glue Crawlers (can be expensive if run frequently)"
  type        = bool
  default     = false  # Disable by default in dev
}

variable "crawler_schedule" {
  description = "Schedule for Glue Crawlers (cron expression)"
  type        = string
  default     = "cron(0 6 * * ? *)"  # Daily at 6 AM UTC
}
