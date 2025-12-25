# =============================================================================
# IAM (Identity and Access Management)
# =============================================================================
#
# IAM controls WHO can do WHAT in AWS.
#
# Key concepts:
# - Principals: Who is making the request (users, services, roles)
# - Actions: What they want to do (s3:GetObject, glue:StartJob)
# - Resources: What they want to do it to (specific S3 bucket, Glue job)
#
# Best Practice: Principle of Least Privilege
# - Give only the permissions needed, nothing more
# - Use specific resource ARNs, not wildcards where possible
#
# For Glue jobs, we create a Role that Glue assumes (becomes) when running.
# This role has permissions to read/write S3 and access Glue resources.
#
# =============================================================================

# -----------------------------------------------------------------------------
# IAM Role for Glue Jobs
# -----------------------------------------------------------------------------

# The IAM Role - an identity that Glue can assume
resource "aws_iam_role" "glue_job_role" {
  name = "${local.name_prefix}-glue-job-role"
  
  # Trust policy - WHO can assume this role
  # This says "AWS Glue service can assume this role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
  
  tags = {
    Name = "Glue Job Role"
  }
}

# -----------------------------------------------------------------------------
# IAM Policies - Define what actions are allowed
# -----------------------------------------------------------------------------

# Policy for S3 access
resource "aws_iam_policy" "glue_s3_access" {
  name        = "${local.name_prefix}-glue-s3-access"
  description = "Allow Glue jobs to read/write to data lake S3 buckets"
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      # Read/Write access to data lake bucket
      {
        Sid    = "DataLakeAccess"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.data_lake.arn,
          "${aws_s3_bucket.data_lake.arn}/*"
        ]
      },
      # Read access to scripts bucket
      {
        Sid    = "ScriptsAccess"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.scripts.arn,
          "${aws_s3_bucket.scripts.arn}/*"
        ]
      }
    ]
  })
}

# Policy for Glue service access
resource "aws_iam_policy" "glue_service_access" {
  name        = "${local.name_prefix}-glue-service-access"
  description = "Allow Glue jobs to access Glue catalog and other services"
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      # Access to Glue Data Catalog (databases, tables)
      {
        Sid    = "GlueCatalogAccess"
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:CreateDatabase",
          "glue:UpdateDatabase",
          "glue:GetTable",
          "glue:GetTables",
          "glue:CreateTable",
          "glue:UpdateTable",
          "glue:DeleteTable",
          "glue:GetPartition",
          "glue:GetPartitions",
          "glue:CreatePartition",
          "glue:BatchCreatePartition",
          "glue:UpdatePartition",
          "glue:DeletePartition"
        ]
        Resource = [
          "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:catalog",
          "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:database/${local.glue_database}",
          "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:table/${local.glue_database}/*"
        ]
      },
      # Job bookmark access (for incremental processing)
      {
        Sid    = "JobBookmarkAccess"
        Effect = "Allow"
        Action = [
          "glue:GetJobBookmark",
          "glue:ResetJobBookmark"
        ]
        Resource = "*"
      }
    ]
  })
}

# Policy for CloudWatch Logs (for job logging)
resource "aws_iam_policy" "glue_cloudwatch_access" {
  name        = "${local.name_prefix}-glue-cloudwatch-access"
  description = "Allow Glue jobs to write logs to CloudWatch"
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "CloudWatchLogsAccess"
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
          "logs:DescribeLogStreams"
        ]
        Resource = [
          "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws-glue/*"
        ]
      }
    ]
  })
}

# -----------------------------------------------------------------------------
# Attach Policies to Role
# -----------------------------------------------------------------------------

# Attach our custom policies
resource "aws_iam_role_policy_attachment" "glue_s3_access" {
  role       = aws_iam_role.glue_job_role.name
  policy_arn = aws_iam_policy.glue_s3_access.arn
}

resource "aws_iam_role_policy_attachment" "glue_service_access" {
  role       = aws_iam_role.glue_job_role.name
  policy_arn = aws_iam_policy.glue_service_access.arn
}

resource "aws_iam_role_policy_attachment" "glue_cloudwatch_access" {
  role       = aws_iam_role.glue_job_role.name
  policy_arn = aws_iam_policy.glue_cloudwatch_access.arn
}

# Attach AWS managed policy for Glue
# This provides baseline permissions that Glue needs
resource "aws_iam_role_policy_attachment" "glue_service_role" {
  role       = aws_iam_role.glue_job_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# -----------------------------------------------------------------------------
# IAM Role for Glue Crawlers (if enabled)
# -----------------------------------------------------------------------------

resource "aws_iam_role" "glue_crawler_role" {
  count = var.enable_crawler ? 1 : 0
  
  name = "${local.name_prefix}-glue-crawler-role"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "crawler_s3_access" {
  count = var.enable_crawler ? 1 : 0
  
  role       = aws_iam_role.glue_crawler_role[0].name
  policy_arn = aws_iam_policy.glue_s3_access.arn
}

resource "aws_iam_role_policy_attachment" "crawler_service_role" {
  count = var.enable_crawler ? 1 : 0
  
  role       = aws_iam_role.glue_crawler_role[0].name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}
