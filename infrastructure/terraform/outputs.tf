# Outputs for OMOP CDM Lakehouse Infrastructure

output "s3_bucket_id" {
  description = "ID of the main data lake S3 bucket"
  value       = aws_s3_bucket.data_lake.id
}

output "s3_bucket_arn" {
  description = "ARN of the main data lake S3 bucket"
  value       = aws_s3_bucket.data_lake.arn
}

output "logs_bucket_id" {
  description = "ID of the logs S3 bucket"
  value       = aws_s3_bucket.logs.id
}

output "athena_results_bucket_id" {
  description = "ID of the Athena results S3 bucket"
  value       = aws_s3_bucket.athena_results.id
}

output "glue_role_arn" {
  description = "ARN of the Glue service role"
  value       = aws_iam_role.glue_role.arn
}

output "lake_formation_role_arn" {
  description = "ARN of the Lake Formation admin role"
  value       = aws_iam_role.lake_formation_role.arn
}

output "athena_role_arn" {
  description = "ARN of the Athena query role"
  value       = aws_iam_role.athena_role.arn
}

output "glue_database_name" {
  description = "Name of the Glue database"
  value       = aws_glue_catalog_database.healthcare_db.name
}

output "athena_workgroup_name" {
  description = "Name of the Athena workgroup"
  value       = aws_athena_workgroup.healthcare.name
}

output "kms_s3_key_arn" {
  description = "ARN of the KMS key for S3 encryption"
  value       = aws_kms_key.s3_key.arn
}

output "kms_glue_key_arn" {
  description = "ARN of the KMS key for Glue encryption"
  value       = aws_kms_key.glue_key.arn
}

output "step_function_arn" {
  description = "ARN of the ETL Step Function"
  value       = aws_sfn_state_machine.etl_pipeline.arn
}

output "initial_setup_state_machine_arn" {
  description = "ARN of the Initial Setup Step Function"
  value       = aws_sfn_state_machine.initial_setup.arn
}

output "etl_pipeline_state_machine_arn" {
  description = "ARN of the ETL Pipeline Step Function"
  value       = aws_sfn_state_machine.etl_pipeline.arn
}

output "data_lake_bucket_name" {
  description = "Name of the data lake S3 bucket"
  value       = aws_s3_bucket.data_lake.id
}
