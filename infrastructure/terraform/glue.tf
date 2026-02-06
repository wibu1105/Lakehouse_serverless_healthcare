# Glue Resources for OMOP CDM Lakehouse
# Data Catalog database, crawlers, and ETL jobs (Job 0-6 Structure)
# Uses single database like Databricks original - all tables in one database

# Glue Data Catalog Database - Single database for all OMOP tables
resource "aws_glue_catalog_database" "healthcare_db" {
  name        = var.glue_database_name
  description = "Healthcare Lakehouse database for OMOP CDM - all tables"

  location_uri = "s3://${aws_s3_bucket.data_lake.id}/"
}

# Glue Security Configuration
resource "aws_glue_security_configuration" "healthcare" {
  name = "${var.project_name}-security-config"

  encryption_configuration {
    cloudwatch_encryption {
      cloudwatch_encryption_mode = "SSE-KMS"
      kms_key_arn                = aws_kms_key.glue_key.arn
    }

    job_bookmarks_encryption {
      job_bookmarks_encryption_mode = "CSE-KMS"
      kms_key_arn                   = aws_kms_key.glue_key.arn
    }

    s3_encryption {
      s3_encryption_mode = "SSE-KMS"
      kms_key_arn        = aws_kms_key.s3_key.arn
    }
  }
}

# Medallion Layer Crawler
resource "aws_glue_crawler" "medallion" {
  name          = "${var.project_name}-medallion-crawler"
  description   = "Unified crawler for all medallion layers"
  role          = aws_iam_role.glue_role.arn
  database_name = aws_glue_catalog_database.healthcare_db.name

  s3_target {
    path = "s3://${aws_s3_bucket.data_lake.id}/bronze/"
  }

  s3_target {
    path = "s3://${aws_s3_bucket.data_lake.id}/silver/"
  }

  s3_target {
    path = "s3://${aws_s3_bucket.data_lake.id}/gold/"
  }

  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }

  configuration = jsonencode({
    Version = 1.0
    Grouping = {
      TableGroupingPolicy = "CombineCompatibleSchemas"
    }
    CrawlerOutput = {
      Partitions = {
        AddOrUpdateBehavior = "InheritFromTable"
      }
    }
  })

  tags = var.tags
}

# Iceberg Configuration for Glue
resource "aws_glue_catalog_table" "iceberg_config" {
  name          = "iceberg_config"
  database_name = aws_glue_catalog_database.healthcare_db.name

  table_type = "EXTERNAL_TABLE"

  parameters = {
    "table_type"         = "ICEBERG"
    "metadata_location"  = "s3://${aws_s3_bucket.data_lake.id}/silver/iceberg_metadata/"
    "classification"     = "iceberg"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.data_lake.id}/silver/"
    input_format  = "org.apache.iceberg.mr.hive.HiveIcebergInputFormat"
    output_format = "org.apache.iceberg.mr.hive.HiveIcebergOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.iceberg.mr.hive.HiveIcebergSerDe"
    }
  }
}

# Common Glue job arguments
locals {
  common_glue_args = {
    "--job-language"                     = "python"
    "--job-bookmark-option"              = "job-bookmark-enable"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-glue-datacatalog"          = "true"
    "--datalake-formats"                 = "iceberg"
    "--conf"                             = "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
  }
}

# ============================================================================
# PHASE 1: INITIAL SETUP (One-Time)
# ============================================================================

# Vocabulary Loader Job
resource "aws_glue_job" "vocabulary_loader" {
  name         = "${var.project_name}-vocabulary-loader"
  description  = "Phase 1: Load OMOP vocabulary tables (33 tables)"
  role_arn     = aws_iam_role.glue_role.arn
  glue_version = "4.0"
  worker_type  = "G.1X"
  number_of_workers = 10

  command {
    script_location = "s3://${aws_s3_bucket.data_lake.id}/scripts/vocabulary_loader.py"
    python_version  = "3"
  }

  default_arguments = merge(local.common_glue_args, {
    "--vocabulary_path" = "s3://${aws_s3_bucket.data_lake.id}/raw/vocabulary/"
    "--silver_path"     = "s3://${aws_s3_bucket.data_lake.id}/silver/"
    "--database_name"   = var.glue_database_name
  })

  security_configuration = aws_glue_security_configuration.healthcare.name

  execution_property {
    max_concurrent_runs = 1
  }

  tags = var.tags
}

# ============================================================================
# PHASE 2: ETL PIPELINE (Daily/Weekly Batch)
# ============================================================================

# Job 0: Load Bronze Views
resource "aws_glue_job" "job_0_load_bronze_views" {
  name         = "${var.project_name}-job-0-load-bronze-views"
  description  = "Job 0: Load Bronze parquet tables as temp views"
  role_arn     = aws_iam_role.glue_role.arn
  glue_version = "4.0"
  worker_type  = "G.1X"
  number_of_workers = 10

  command {
    script_location = "s3://${aws_s3_bucket.data_lake.id}/scripts/job_0_load_bronze_views.py"
    python_version  = "3"
  }

  default_arguments = merge(local.common_glue_args, {
    "--bronze_path"   = "s3://${aws_s3_bucket.data_lake.id}/bronze/"
    "--database_name" = var.glue_database_name
  })

  security_configuration = aws_glue_security_configuration.healthcare.name

  execution_property {
    max_concurrent_runs = 1
  }

  tags = var.tags
}

# Job 1: Visit Grouping
resource "aws_glue_job" "job_1_visit_grouping" {
  name         = "${var.project_name}-job-1-visit-grouping"
  description  = "Job 1: Group encounters into visits (IP, ER, OP)"
  role_arn     = aws_iam_role.glue_role.arn
  glue_version = "4.0"
  worker_type  = "G.1X"
  number_of_workers = 10

  command {
    script_location = "s3://${aws_s3_bucket.data_lake.id}/scripts/job_1_visit_grouping.py"
    python_version  = "3"
  }

  default_arguments = merge(local.common_glue_args, {
    "--bronze_path"   = "s3://${aws_s3_bucket.data_lake.id}/bronze/"
    "--silver_path"   = "s3://${aws_s3_bucket.data_lake.id}/silver/"
    "--database_name" = var.glue_database_name
  })

  security_configuration = aws_glue_security_configuration.healthcare.name

  execution_property {
    max_concurrent_runs = 1
  }

  tags = var.tags
}

# Job 2: Person & Observation Period
resource "aws_glue_job" "job_2_person_observation_period" {
  name         = "${var.project_name}-job-2-person-observation-period"
  description  = "Job 2: Create person, observation_period, location"
  role_arn     = aws_iam_role.glue_role.arn
  glue_version = "4.0"
  worker_type  = "G.1X"
  number_of_workers = 10

  command {
    script_location = "s3://${aws_s3_bucket.data_lake.id}/scripts/job_2_person_observation_period.py"
    python_version  = "3"
  }

  default_arguments = merge(local.common_glue_args, {
    "--bronze_path"   = "s3://${aws_s3_bucket.data_lake.id}/bronze/"
    "--silver_path"   = "s3://${aws_s3_bucket.data_lake.id}/silver/"
    "--database_name" = var.glue_database_name
  })

  security_configuration = aws_glue_security_configuration.healthcare.name

  execution_property {
    max_concurrent_runs = 1
  }

  tags = var.tags
}

# Job 3: Visit Occurrence
resource "aws_glue_job" "job_3_visit_occurrence" {
  name         = "${var.project_name}-job-3-visit-occurrence"
  description  = "Job 3: Create visit_occurrence with >95% mapping"
  role_arn     = aws_iam_role.glue_role.arn
  glue_version = "4.0"
  worker_type  = "G.1X"
  number_of_workers = 10

  command {
    script_location = "s3://${aws_s3_bucket.data_lake.id}/scripts/job_3_visit_occurrence.py"
    python_version  = "3"
  }

  default_arguments = merge(local.common_glue_args, {
    "--silver_path"   = "s3://${aws_s3_bucket.data_lake.id}/silver/"
    "--database_name" = var.glue_database_name
  })

  security_configuration = aws_glue_security_configuration.healthcare.name

  execution_property {
    max_concurrent_runs = 1
  }

  tags = var.tags
}

# Job 4: Clinical Events
resource "aws_glue_job" "job_4_clinical_events" {
  name         = "${var.project_name}-job-4-clinical-events"
  description  = "Job 4: Process conditions, procedures, observations"
  role_arn     = aws_iam_role.glue_role.arn
  glue_version = "4.0"
  worker_type  = "G.1X"
  number_of_workers = 10

  command {
    script_location = "s3://${aws_s3_bucket.data_lake.id}/scripts/job_4_clinical_events.py"
    python_version  = "3"
  }

  default_arguments = merge(local.common_glue_args, {
    "--bronze_path"   = "s3://${aws_s3_bucket.data_lake.id}/bronze/"
    "--silver_path"   = "s3://${aws_s3_bucket.data_lake.id}/silver/"
    "--database_name" = var.glue_database_name
  })

  security_configuration = aws_glue_security_configuration.healthcare.name

  execution_property {
    max_concurrent_runs = 1
  }

  tags = var.tags
}

# Job 5: Drug Exposure
resource "aws_glue_job" "job_5_drug_exposure" {
  name         = "${var.project_name}-job-5-drug-exposure"
  description  = "Job 5: Process medications & immunizations"
  role_arn     = aws_iam_role.glue_role.arn
  glue_version = "4.0"
  worker_type  = "G.1X"
  number_of_workers = 10

  command {
    script_location = "s3://${aws_s3_bucket.data_lake.id}/scripts/job_5_drug_exposure.py"
    python_version  = "3"
  }

  default_arguments = merge(local.common_glue_args, {
    "--bronze_path"   = "s3://${aws_s3_bucket.data_lake.id}/bronze/"
    "--silver_path"   = "s3://${aws_s3_bucket.data_lake.id}/silver/"
    "--database_name" = var.glue_database_name
  })

  security_configuration = aws_glue_security_configuration.healthcare.name

  execution_property {
    max_concurrent_runs = 1
  }

  tags = var.tags
}

# Job 6: Era & Metadata
resource "aws_glue_job" "job_6_era_metadata" {
  name         = "${var.project_name}-job-6-era-metadata"
  description  = "Job 6: Calculate eras and create CDM source"
  role_arn     = aws_iam_role.glue_role.arn
  glue_version = "4.0"
  worker_type  = "G.1X"
  number_of_workers = 10

  command {
    script_location = "s3://${aws_s3_bucket.data_lake.id}/scripts/job_6_era_metadata.py"
    python_version  = "3"
  }

  default_arguments = merge(local.common_glue_args, {
    "--silver_path"   = "s3://${aws_s3_bucket.data_lake.id}/silver/"
    "--database_name" = var.glue_database_name
    "--gap_days"      = "30"
  })

  security_configuration = aws_glue_security_configuration.healthcare.name

  execution_property {
    max_concurrent_runs = 1
  }

  tags = var.tags
}

# Raw to Bronze Job (kept for initial data ingestion)
resource "aws_glue_job" "raw_to_bronze" {
  name         = "${var.project_name}-raw-to-bronze"
  description  = "ETL job to ingest raw Synthea data into Bronze layer"
  role_arn     = aws_iam_role.glue_role.arn
  glue_version = "4.0"
  worker_type  = "G.1X"
  number_of_workers = 10

  command {
    script_location = "s3://${aws_s3_bucket.data_lake.id}/scripts/raw_to_bronze.py"
    python_version  = "3"
  }

  default_arguments = merge(local.common_glue_args, {
    "--raw_path"      = "s3://${aws_s3_bucket.data_lake.id}/raw/synthea/"
    "--bronze_path"   = "s3://${aws_s3_bucket.data_lake.id}/bronze/"
    "--database_name" = var.glue_database_name
  })

  security_configuration = aws_glue_security_configuration.healthcare.name

  execution_property {
    max_concurrent_runs = 1
  }

  tags = var.tags
}

# ============================================================================
# VALIDATION JOBS (Run after each ETL job in Step Functions)
# ============================================================================

# Validate Bronze - runs after RawToBronze
resource "aws_glue_job" "validate_bronze" {
  name         = "${var.project_name}-validate-bronze"
  description  = "Validate Bronze layer tables after ingestion"
  role_arn     = aws_iam_role.glue_role.arn
  glue_version = "4.0"
  worker_type  = "G.1X"
  number_of_workers = 2

  command {
    script_location = "s3://${aws_s3_bucket.data_lake.id}/scripts/validate_bronze.py"
    python_version  = "3"
  }

  default_arguments = merge(local.common_glue_args, {
    "--bronze_path"   = "s3://${aws_s3_bucket.data_lake.id}/bronze/"
    "--database_name" = var.glue_database_name
  })

  security_configuration = aws_glue_security_configuration.healthcare.name
  timeout                = 30

  execution_property {
    max_concurrent_runs = 1
  }

  tags = var.tags
}

# Validate Visits - runs after Job1
resource "aws_glue_job" "validate_visits" {
  name         = "${var.project_name}-validate-visits"
  description  = "Validate visits tables after Job1"
  role_arn     = aws_iam_role.glue_role.arn
  glue_version = "4.0"
  worker_type  = "G.1X"
  number_of_workers = 2

  command {
    script_location = "s3://${aws_s3_bucket.data_lake.id}/scripts/validate_visits.py"
    python_version  = "3"
  }

  default_arguments = merge(local.common_glue_args, {
    "--silver_path"   = "s3://${aws_s3_bucket.data_lake.id}/silver/"
    "--database_name" = var.glue_database_name
  })

  security_configuration = aws_glue_security_configuration.healthcare.name
  timeout                = 30

  execution_property {
    max_concurrent_runs = 1
  }

  tags = var.tags
}

# Validate Person - runs after Job2
resource "aws_glue_job" "validate_person" {
  name         = "${var.project_name}-validate-person"
  description  = "Validate person & observation_period after Job2"
  role_arn     = aws_iam_role.glue_role.arn
  glue_version = "4.0"
  worker_type  = "G.1X"
  number_of_workers = 2

  command {
    script_location = "s3://${aws_s3_bucket.data_lake.id}/scripts/validate_person.py"
    python_version  = "3"
  }

  default_arguments = merge(local.common_glue_args, {
    "--silver_path"   = "s3://${aws_s3_bucket.data_lake.id}/silver/"
    "--database_name" = var.glue_database_name
  })

  security_configuration = aws_glue_security_configuration.healthcare.name
  timeout                = 30

  execution_property {
    max_concurrent_runs = 1
  }

  tags = var.tags
}

# Validate Visit Occurrence - runs after Job3
resource "aws_glue_job" "validate_visit_occurrence" {
  name         = "${var.project_name}-validate-visit-occurrence"
  description  = "Validate visit_occurrence with >=95% mapping rate"
  role_arn     = aws_iam_role.glue_role.arn
  glue_version = "4.0"
  worker_type  = "G.1X"
  number_of_workers = 2

  command {
    script_location = "s3://${aws_s3_bucket.data_lake.id}/scripts/validate_visit_occurrence.py"
    python_version  = "3"
  }

  default_arguments = merge(local.common_glue_args, {
    "--silver_path"   = "s3://${aws_s3_bucket.data_lake.id}/silver/"
    "--database_name" = var.glue_database_name
  })

  security_configuration = aws_glue_security_configuration.healthcare.name
  timeout                = 30

  execution_property {
    max_concurrent_runs = 1
  }

  tags = var.tags
}

# Validate Clinical - runs after Job4
resource "aws_glue_job" "validate_clinical" {
  name         = "${var.project_name}-validate-clinical"
  description  = "Validate clinical events after Job4"
  role_arn     = aws_iam_role.glue_role.arn
  glue_version = "4.0"
  worker_type  = "G.1X"
  number_of_workers = 2

  command {
    script_location = "s3://${aws_s3_bucket.data_lake.id}/scripts/validate_clinical.py"
    python_version  = "3"
  }

  default_arguments = merge(local.common_glue_args, {
    "--silver_path"   = "s3://${aws_s3_bucket.data_lake.id}/silver/"
    "--database_name" = var.glue_database_name
  })

  security_configuration = aws_glue_security_configuration.healthcare.name
  timeout                = 30

  execution_property {
    max_concurrent_runs = 1
  }

  tags = var.tags
}

# Validate Drugs - runs after Job5
resource "aws_glue_job" "validate_drugs" {
  name         = "${var.project_name}-validate-drugs"
  description  = "Validate drug_exposure after Job5"
  role_arn     = aws_iam_role.glue_role.arn
  glue_version = "4.0"
  worker_type  = "G.1X"
  number_of_workers = 2

  command {
    script_location = "s3://${aws_s3_bucket.data_lake.id}/scripts/validate_drugs.py"
    python_version  = "3"
  }

  default_arguments = merge(local.common_glue_args, {
    "--silver_path"   = "s3://${aws_s3_bucket.data_lake.id}/silver/"
    "--database_name" = var.glue_database_name
  })

  security_configuration = aws_glue_security_configuration.healthcare.name
  timeout                = 30

  execution_property {
    max_concurrent_runs = 1
  }

  tags = var.tags
}

# Validate Final - runs after Job6
resource "aws_glue_job" "validate_final" {
  name         = "${var.project_name}-validate-final"
  description  = "Final pipeline validation and report"
  role_arn     = aws_iam_role.glue_role.arn
  glue_version = "4.0"
  worker_type  = "G.1X"
  number_of_workers = 2

  command {
    script_location = "s3://${aws_s3_bucket.data_lake.id}/scripts/validate_final.py"
    python_version  = "3"
  }

  default_arguments = merge(local.common_glue_args, {
    "--silver_path"   = "s3://${aws_s3_bucket.data_lake.id}/silver/"
    "--database_name" = var.glue_database_name
  })

  security_configuration = aws_glue_security_configuration.healthcare.name
  timeout                = 30

  execution_property {
    max_concurrent_runs = 1
  }

  tags = var.tags
}
