# SSM Parameters for OMOP CDM Lakehouse
# Configuration parameters stored in AWS Systems Manager Parameter Store

resource "aws_ssm_parameter" "s3_bucket_name" {
  name        = "/${var.project_name}/config/s3-bucket-name"
  description = "Main S3 bucket name for healthcare lakehouse"
  type        = "String"
  value       = aws_s3_bucket.data_lake.id

  tags = var.tags
}

resource "aws_ssm_parameter" "glue_role_arn" {
  name        = "/${var.project_name}/config/glue-role-arn"
  description = "ARN of the Glue service role"
  type        = "String"
  value       = aws_iam_role.glue_role.arn

  tags = var.tags
}

resource "aws_ssm_parameter" "kms_s3_key_id" {
  name        = "/${var.project_name}/config/kms-s3-key-id"
  description = "KMS key ID for S3 encryption"
  type        = "String"
  value       = aws_kms_key.s3_key.id

  tags = var.tags
}

resource "aws_ssm_parameter" "database_name" {
  name        = "/${var.project_name}/config/database-name"
  description = "Glue Data Catalog database name"
  type        = "String"
  value       = aws_glue_catalog_database.healthcare_db.name

  tags = var.tags
}

resource "aws_ssm_parameter" "athena_workgroup" {
  name        = "/${var.project_name}/config/athena-workgroup"
  description = "Athena workgroup name"
  type        = "String"
  value       = aws_athena_workgroup.healthcare.name

  tags = var.tags
}

resource "aws_ssm_parameter" "athena_results_bucket" {
  name        = "/${var.project_name}/config/athena-results-bucket"
  description = "S3 bucket for Athena query results"
  type        = "String"
  value       = aws_s3_bucket.athena_results.id

  tags = var.tags
}

resource "aws_ssm_parameter" "omop_version" {
  name        = "/${var.project_name}/config/omop-version"
  description = "OMOP CDM version"
  type        = "String"
  value       = var.omop_version

  tags = var.tags
}

# Layer paths
resource "aws_ssm_parameter" "raw_path" {
  name        = "/${var.project_name}/paths/raw"
  description = "S3 path for raw layer"
  type        = "String"
  value       = "s3://${aws_s3_bucket.data_lake.id}/raw/"

  tags = var.tags
}

resource "aws_ssm_parameter" "bronze_path" {
  name        = "/${var.project_name}/paths/bronze"
  description = "S3 path for bronze layer"
  type        = "String"
  value       = "s3://${aws_s3_bucket.data_lake.id}/bronze/"

  tags = var.tags
}

resource "aws_ssm_parameter" "silver_path" {
  name        = "/${var.project_name}/paths/silver"
  description = "S3 path for silver layer"
  type        = "String"
  value       = "s3://${aws_s3_bucket.data_lake.id}/silver/"

  tags = var.tags
}

resource "aws_ssm_parameter" "gold_path" {
  name        = "/${var.project_name}/paths/gold"
  description = "S3 path for gold layer"
  type        = "String"
  value       = "s3://${aws_s3_bucket.data_lake.id}/gold/"

  tags = var.tags
}
