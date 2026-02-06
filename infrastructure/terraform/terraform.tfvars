# AWS Configuration
aws_region   = "ap-southeast-1"  # Hoặc region bạn muốn
environment  = "dev"

# Project Configuration
project_name       = "omop-cdm-lakehouse"
glue_database_name = "healthcare"

# S3 Bucket (sẽ tự động tạo với suffix unique)
s3_bucket_name = "omop-cdm-lakehouse-904557616804"

# Monitoring
log_retention_days = 7

# Tags
tags = {
  Project     = "OMOP CDM Lakehouse"
  Environment = "dev"
  ManagedBy   = "Terraform"
  Owner       = "taivo"
}
