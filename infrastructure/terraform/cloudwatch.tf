# CloudWatch Monitoring for OMOP CDM Lakehouse
# Dashboards, alarms, and log groups (Updated for Job 0-6 structure)

# Log Groups
resource "aws_cloudwatch_log_group" "glue_jobs" {
  name              = "/aws-glue/jobs/${var.project_name}"
  retention_in_days = var.log_retention_days

  tags = var.tags
}

# CloudWatch Dashboard
resource "aws_cloudwatch_dashboard" "healthcare_lakehouse" {
  dashboard_name = "${var.project_name}-dashboard"

  dashboard_body = jsonencode({
    widgets = [
      {
        type   = "text"
        x      = 0
        y      = 0
        width  = 24
        height = 1
        properties = {
          markdown = "# Healthcare Lakehouse - OMOP CDM Pipeline Dashboard (Job 0-6)"
        }
      },
      {
        type   = "metric"
        x      = 0
        y      = 1
        width  = 12
        height = 6
        properties = {
          title  = "Glue Job Success/Failure"
          region = data.aws_region.current.name
          metrics = [
            ["Glue", "glue.driver.aggregate.numCompletedTasks", "JobName", aws_glue_job.raw_to_bronze.name, { label = "Raw to Bronze" }],
            ["Glue", "glue.driver.aggregate.numCompletedTasks", "JobName", aws_glue_job.job_0_load_bronze_views.name, { label = "Job 0: Load Views" }],
            ["Glue", "glue.driver.aggregate.numCompletedTasks", "JobName", aws_glue_job.job_1_visit_grouping.name, { label = "Job 1: Visits" }],
            ["Glue", "glue.driver.aggregate.numCompletedTasks", "JobName", aws_glue_job.job_2_person_observation_period.name, { label = "Job 2: Person" }],
            ["Glue", "glue.driver.aggregate.numCompletedTasks", "JobName", aws_glue_job.job_3_visit_occurrence.name, { label = "Job 3: Visit Occ" }],
            ["Glue", "glue.driver.aggregate.numCompletedTasks", "JobName", aws_glue_job.job_4_clinical_events.name, { label = "Job 4: Clinical" }],
            ["Glue", "glue.driver.aggregate.numCompletedTasks", "JobName", aws_glue_job.job_5_drug_exposure.name, { label = "Job 5: Drugs" }],
            ["Glue", "glue.driver.aggregate.numCompletedTasks", "JobName", aws_glue_job.job_6_era_metadata.name, { label = "Job 6: Eras" }]
          ]
          stat   = "Sum"
          period = 3600
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 1
        width  = 12
        height = 6
        properties = {
          title  = "Glue Job Duration (seconds)"
          region = data.aws_region.current.name
          metrics = [
            ["Glue", "glue.driver.aggregate.elapsedTime", "JobName", aws_glue_job.raw_to_bronze.name, { label = "Raw to Bronze" }],
            ["Glue", "glue.driver.aggregate.elapsedTime", "JobName", aws_glue_job.job_4_clinical_events.name, { label = "Job 4: Clinical" }],
            ["Glue", "glue.driver.aggregate.elapsedTime", "JobName", aws_glue_job.job_6_era_metadata.name, { label = "Job 6: Eras" }]
          ]
          stat   = "Average"
          period = 3600
        }
      },
      {
        type   = "metric"
        x      = 0
        y      = 7
        width  = 12
        height = 6
        properties = {
          title  = "S3 Storage (Bytes)"
          region = data.aws_region.current.name
          metrics = [
            ["AWS/S3", "BucketSizeBytes", "BucketName", aws_s3_bucket.data_lake.id, "StorageType", "StandardStorage", { label = "Data Lake" }],
            ["AWS/S3", "BucketSizeBytes", "BucketName", aws_s3_bucket.athena_results.id, "StorageType", "StandardStorage", { label = "Athena Results" }]
          ]
          stat   = "Average"
          period = 86400
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 7
        width  = 12
        height = 6
        properties = {
          title  = "Athena Query Metrics"
          region = data.aws_region.current.name
          metrics = [
            ["AWS/Athena", "TotalExecutionTime", "WorkGroup", aws_athena_workgroup.healthcare.name, { label = "Execution Time" }],
            ["AWS/Athena", "DataScannedInBytes", "WorkGroup", aws_athena_workgroup.healthcare.name, { label = "Data Scanned" }]
          ]
          stat   = "Sum"
          period = 3600
        }
      },
      {
        type   = "metric"
        x      = 0
        y      = 13
        width  = 24
        height = 6
        properties = {
          title  = "Step Functions Executions"
          region = data.aws_region.current.name
          metrics = [
            ["AWS/States", "ExecutionsSucceeded", "StateMachineArn", aws_sfn_state_machine.etl_pipeline.arn, { label = "Succeeded" }],
            ["AWS/States", "ExecutionsFailed", "StateMachineArn", aws_sfn_state_machine.etl_pipeline.arn, { label = "Failed" }],
            ["AWS/States", "ExecutionsTimedOut", "StateMachineArn", aws_sfn_state_machine.etl_pipeline.arn, { label = "Timed Out" }]
          ]
          stat   = "Sum"
          period = 86400
        }
      }
    ]
  })
}

# Alarms

# Glue Job Failure Alarm
resource "aws_cloudwatch_metric_alarm" "glue_job_failure" {
  alarm_name          = "${var.project_name}-glue-job-failure"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "glue.driver.aggregate.numFailedTasks"
  namespace           = "Glue"
  period              = 300
  statistic           = "Sum"
  threshold           = 0
  alarm_description   = "Alarm when any Glue job has failed tasks"

  dimensions = {
    JobName = aws_glue_job.raw_to_bronze.name
  }

  tags = var.tags
}

# Step Functions Failure Alarm
resource "aws_cloudwatch_metric_alarm" "step_function_failure" {
  alarm_name          = "${var.project_name}-pipeline-failure"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ExecutionsFailed"
  namespace           = "AWS/States"
  period              = 300
  statistic           = "Sum"
  threshold           = 0
  alarm_description   = "Alarm when ETL pipeline execution fails"

  dimensions = {
    StateMachineArn = aws_sfn_state_machine.etl_pipeline.arn
  }

  tags = var.tags
}

# S3 Storage High Alarm
resource "aws_cloudwatch_metric_alarm" "s3_storage_high" {
  alarm_name          = "${var.project_name}-s3-storage-high"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "BucketSizeBytes"
  namespace           = "AWS/S3"
  period              = 86400
  statistic           = "Average"
  threshold           = 107374182400  # 100 GB
  alarm_description   = "Alarm when S3 storage exceeds 100 GB"

  dimensions = {
    BucketName  = aws_s3_bucket.data_lake.id
    StorageType = "StandardStorage"
  }

  tags = var.tags
}

# Athena Query Timeout Alarm
resource "aws_cloudwatch_metric_alarm" "athena_timeout" {
  alarm_name          = "${var.project_name}-athena-timeout"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "TotalExecutionTime"
  namespace           = "AWS/Athena"
  period              = 300
  statistic           = "Maximum"
  threshold           = 300000  # 5 minutes in milliseconds
  alarm_description   = "Alarm when Athena queries take longer than 5 minutes"

  dimensions = {
    WorkGroup = aws_athena_workgroup.healthcare.name
  }

  tags = var.tags
}
