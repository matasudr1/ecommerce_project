# =============================================================================
# TERRAFORM CONFIGURATION
# =============================================================================
# 
# What is Terraform?
# ------------------
# Terraform is an "Infrastructure as Code" (IaC) tool. Instead of clicking
# around in the AWS Console to create resources, you write code that describes
# what you want, and Terraform creates it for you.
#
# Why use Terraform?
# - Reproducibility: Create the same infrastructure every time
# - Version control: Track changes to infrastructure in Git
# - Documentation: Code IS documentation of what exists
# - Automation: No manual clicking, reduces human error
# - Multi-environment: Same code for dev, staging, prod
#
# How it works:
# 1. You write .tf files describing desired resources
# 2. Run `terraform init` to download providers
# 3. Run `terraform plan` to see what will be created
# 4. Run `terraform apply` to create the resources
# 5. Run `terraform destroy` to tear everything down
#
# =============================================================================

# Specify required Terraform version and providers
terraform {
  required_version = ">= 1.0.0"
  
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"  # Use AWS provider version 5.x
    }
  }
  
  # Backend configuration (where Terraform stores state)
  # Uncomment this for production use with remote state
  # backend "s3" {
  #   bucket = "your-terraform-state-bucket"
  #   key    = "ecommerce-pipeline/terraform.tfstate"
  #   region = "eu-west-1"
  # }
}

# Configure the AWS Provider
# Credentials are taken from:
# 1. Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
# 2. Shared credentials file (~/.aws/credentials)
# 3. IAM role (if running on EC2/ECS/Lambda)
provider "aws" {
  region = var.aws_region
  
  # Default tags applied to ALL resources
  # This is great for cost tracking and organization
  default_tags {
    tags = {
      Project     = var.project_name
      Environment = var.environment
      ManagedBy   = "Terraform"
      Owner       = var.owner
    }
  }
}

# =============================================================================
# DATA SOURCES
# =============================================================================
# Data sources let us reference existing AWS resources or get information
# from AWS that we don't control (like the current account ID)

# Get current AWS account ID and region
data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

# =============================================================================
# LOCAL VALUES
# =============================================================================
# Locals are like variables, but computed within Terraform.
# Useful for combining values or creating naming conventions.

locals {
  # Consistent naming prefix for all resources
  name_prefix = "${var.project_name}-${var.environment}"
  
  # Common tags (in addition to default_tags)
  common_tags = {
    CostCenter = "data-engineering"
  }
  
  # S3 bucket names (must be globally unique)
  bucket_name = "${local.name_prefix}-data-${data.aws_caller_identity.current.account_id}"
  
  # Glue database name
  glue_database = "${var.project_name}_${var.environment}"
}
