# Athena Configuration for OMOP CDM Lakehouse
# Workgroups and query settings

resource "aws_athena_workgroup" "healthcare" {
  name        = "${var.project_name}-workgroup"
  description = "Athena workgroup for healthcare lakehouse queries"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena_results.id}/query-results/"

      encryption_configuration {
        encryption_option = "SSE_KMS"
        kms_key_arn       = aws_kms_key.s3_key.arn
      }
    }

    engine_version {
      selected_engine_version = "Athena engine version 3"
    }

    bytes_scanned_cutoff_per_query = 10737418240  # 10 GB limit
  }

  tags = var.tags
}

# Named queries for common operations

resource "aws_athena_named_query" "show_tables" {
  name        = "Show All Tables"
  description = "Display all tables in the healthcare database"
  database    = aws_glue_catalog_database.healthcare_db.name
  workgroup   = aws_athena_workgroup.healthcare.name
  query       = "SHOW TABLES;"
}

resource "aws_athena_named_query" "patient_count" {
  name        = "Patient Count"
  description = "Count total patients in the person table"
  database    = aws_glue_catalog_database.healthcare_db.name
  workgroup   = aws_athena_workgroup.healthcare.name
  query       = "SELECT COUNT(*) as total_patients FROM person;"
}

resource "aws_athena_named_query" "concept_coverage" {
  name        = "Concept Mapping Coverage"
  description = "Check concept mapping coverage for conditions"
  database    = aws_glue_catalog_database.healthcare_db.name
  workgroup   = aws_athena_workgroup.healthcare.name
  query       = <<-EOT
    SELECT 
      COUNT(*) as total_conditions,
      COUNT(CASE WHEN condition_concept_id > 0 THEN 1 END) as mapped_conditions,
      ROUND(COUNT(CASE WHEN condition_concept_id > 0 THEN 1 END) * 100.0 / COUNT(*), 2) as coverage_pct
    FROM condition_occurrence;
  EOT
}

resource "aws_athena_named_query" "data_quality_check" {
  name        = "Data Quality Check"
  description = "Basic data quality validation"
  database    = aws_glue_catalog_database.healthcare_db.name
  workgroup   = aws_athena_workgroup.healthcare.name
  query       = <<-EOT
    WITH table_counts AS (
      SELECT 'person' as table_name, COUNT(*) as row_count FROM person
      UNION ALL
      SELECT 'visit_occurrence', COUNT(*) FROM visit_occurrence
      UNION ALL
      SELECT 'condition_occurrence', COUNT(*) FROM condition_occurrence
      UNION ALL
      SELECT 'drug_exposure', COUNT(*) FROM drug_exposure
      UNION ALL
      SELECT 'measurement', COUNT(*) FROM measurement
      UNION ALL
      SELECT 'procedure_occurrence', COUNT(*) FROM procedure_occurrence
      UNION ALL
      SELECT 'observation_period', COUNT(*) FROM observation_period
    )
    SELECT * FROM table_counts ORDER BY row_count DESC;
  EOT
}
