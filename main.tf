terraform {
  required_version = ">= 1.6.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.0"
    }
  }
}

provider "aws" {
  region     = var.aws_region
  access_key = var.aws_access_key_id
  secret_key = var.aws_secret_access_key
}

variable "aws_region" {
  type        = string
  description = "AWS region to deploy resources"
  default     = "us-east-1"
}

variable "project_name" {
  type        = string
  description = "Project name prefix"
  default     = "nyc-taxi"
}

variable "data_bucket" {
  type        = string
  description = "Existing S3 bucket that holds input and will hold outputs"
  default     = "rowdynyc"
}

variable "glue_role_arn" {
  type        = string
  description = "Pre-created Glue service role ARN to use for the job"
}

variable "aws_access_key_id" {
  type        = string
  description = "AWS access key ID (pass via -var or TF_VAR_*)"
  sensitive   = true
}

variable "aws_secret_access_key" {
  type        = string
  description = "AWS secret access key (pass via -var or TF_VAR_*)"
  sensitive   = true
}

variable "run_token" {
  type        = string
  description = "Change this to force a one-time Glue job run via trigger recreation (Terraform-only execution)."
  default     = "init"
}

locals {
  common_tags = {
    Project = var.project_name
    Owner   = "tasker-assignment"
  }

  # Clean S3 folder structure under the existing bucket
  base_prefix      = "nyctaxi/"
  warehouse_prefix = "${local.base_prefix}warehouse/"     # Iceberg warehouse root (only new folder we create)
}

# Create only the warehouse folder to keep structure minimal
resource "aws_s3_object" "warehouse_folder" {
  bucket       = var.data_bucket
  key          = local.warehouse_prefix
  content      = ""
  content_type = "application/x-directory"
}

# Upload the ETL script to S3 for Glue to use
resource "aws_s3_object" "etl_script" {
  bucket = var.data_bucket
  key    = "${local.base_prefix}etl_job.py"
  source = "${path.module}/etl_job.py"
  etag   = filemd5("${path.module}/etl_job.py")
}

# Glue Data Catalog databases for Iceberg tables
resource "aws_glue_catalog_database" "raw_db" {
  name = "nyc_raw"
}

resource "aws_glue_catalog_database" "gold_db" {
  name = "nyc_gold"
}

# IAM role for Glue
data "aws_iam_policy_document" "assume_glue" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

## IAM role/policy are pre-created; supply ARN via variable and skip IAM creation

# CloudWatch Log Group for the job (optional but keeps logs tidy)
resource "aws_cloudwatch_log_group" "glue_log" {
  name              = "/aws/glue/jobs/${var.project_name}"
  retention_in_days = 14
  tags              = local.common_tags
}

# Glue Job definition
resource "aws_glue_job" "etl" {
  name              = "${var.project_name}-etl-${var.aws_region}"
  role_arn          = var.glue_role_arn
  glue_version      = "4.0"
  number_of_workers = 2
  worker_type       = "G.1X"

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${var.data_bucket}/${aws_s3_object.etl_script.key}"
  }

  default_arguments = {
    "--job-language"                  = "python"
    "--enable-glue-datacatalog"       = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--datalake-formats"              = "iceberg"

    # ETL parameters
    "--INPUT_PATH"     = "s3://${var.data_bucket}/${local.base_prefix}yellow_tripdata_*.parquet"
    "--WAREHOUSE_PATH" = "s3://${var.data_bucket}/${local.warehouse_prefix}"
    "--RAW_DB"         = aws_glue_catalog_database.raw_db.name
    "--GOLD_DB"        = aws_glue_catalog_database.gold_db.name
  }

  execution_property {
    max_concurrent_runs = 1
  }

  tags = local.common_tags
}

## Terraform-only execution: create a one-shot ON_DEMAND trigger that starts on creation
## Re-run by changing var.run_token to a new value and re-applying
resource "aws_glue_trigger" "run_once" {
  name               = "${var.project_name}-run-${var.run_token}-${var.aws_region}"
  type               = "SCHEDULED"
  schedule           = "cron(0 0 1 1 ? 2099)" # far-future to avoid recurring runs
  start_on_creation  = true                     # starts immediately on creation

  actions {
    job_name  = aws_glue_job.etl.name
    arguments = {}
  }

  depends_on = [
    aws_glue_job.etl,
    aws_s3_object.etl_script,
    aws_s3_object.warehouse_folder,
  ]

  tags = local.common_tags
}

output "glue_job_name" {
  value       = aws_glue_job.etl.name
  description = "Glue job name"
}

output "script_s3_path" {
  value       = "s3://${var.data_bucket}/${aws_s3_object.etl_script.key}"
  description = "Location of the ETL script"
}


