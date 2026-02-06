# Variables for OMOP CDM Lakehouse Infrastructure

variable "aws_region" {
  description = "AWS region for deployment"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "project_name" {
  description = "Project name for resource naming"
  type        = string
  default     = "healthcare-lakehouse"
}

variable "s3_bucket_name" {
  description = "Name for the main data lake S3 bucket"
  type        = string
  default     = "healthcare-analyzer-lakehouse"
}

variable "glue_database_name" {
  description = "Name for the Glue Data Catalog database"
  type        = string
  default     = "healthcare_lakehouse_db"
}

variable "omop_version" {
  description = "OMOP CDM version"
  type        = string
  default     = "5.4"
}

variable "enable_versioning" {
  description = "Enable S3 versioning"
  type        = bool
  default     = true
}

variable "log_retention_days" {
  description = "CloudWatch log retention in days"
  type        = number
  default     = 30
}

variable "data_lifecycle_days" {
  description = "Days before transitioning data to cheaper storage"
  type = object({
    to_standard_ia = number
    to_glacier     = number
    delete_noncurrent = number
  })
  default = {
    to_standard_ia    = 90
    to_glacier        = 365
    delete_noncurrent = 30
  }
}

variable "tags" {
  description = "Additional tags to apply to resources"
  type        = map(string)
  default     = {}
}
