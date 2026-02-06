# Step Functions for OMOP CDM Lakehouse
# ETL Pipeline Orchestration (Job 0-6 Structure)

# =============================================================================
# PHASE 2: ETL Pipeline State Machine (Daily/Weekly Batch)
# =============================================================================
# Flow: RawToBronze → Job0 → Job1 → Job2 → Job3 → (Job4 || Job5) → Job6
# =============================================================================

resource "aws_sfn_state_machine" "etl_pipeline" {
  name     = "${var.project_name}-etl-pipeline"
  role_arn = aws_iam_role.step_functions_role.arn

  definition = jsonencode({
    Comment = "OMOP CDM Lakehouse ETL Pipeline - With Validation Steps"
    StartAt = "RawToBronze"
    States = {
      # Ingest raw Synthea CSV to Bronze parquet
      RawToBronze = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.raw_to_bronze.name
        }
        ResultPath = "$.RawToBronzeResult"
        Next       = "ValidateBronze"
        Catch = [{
          ErrorEquals = ["States.ALL"]
          Next        = "PipelineFailed"
          ResultPath  = "$.error"
        }]
      }

      # Validate Bronze Layer
      ValidateBronze = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.validate_bronze.name
        }
        ResultPath = "$.ValidateBronzeResult"
        Next       = "Job0_LoadBronzeViews"
        Catch = [{
          ErrorEquals = ["States.ALL"]
          Next        = "PipelineFailed"
          ResultPath  = "$.error"
        }]
      }

      # Job 0: Load Bronze Views
      Job0_LoadBronzeViews = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.job_0_load_bronze_views.name
        }
        ResultPath = "$.Job0Result"
        Next       = "Job1_VisitGrouping"
        Catch = [{
          ErrorEquals = ["States.ALL"]
          Next        = "PipelineFailed"
          ResultPath  = "$.error"
        }]
      }

      # Job 1: Visit Grouping
      Job1_VisitGrouping = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.job_1_visit_grouping.name
        }
        ResultPath = "$.Job1Result"
        Next       = "ValidateVisits"
        Catch = [{
          ErrorEquals = ["States.ALL"]
          Next        = "PipelineFailed"
          ResultPath  = "$.error"
        }]
      }

      # Validate Visits
      ValidateVisits = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.validate_visits.name
        }
        ResultPath = "$.ValidateVisitsResult"
        Next       = "Job2_PersonObservationPeriod"
        Catch = [{
          ErrorEquals = ["States.ALL"]
          Next        = "PipelineFailed"
          ResultPath  = "$.error"
        }]
      }

      # Job 2: Person & Observation Period
      Job2_PersonObservationPeriod = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.job_2_person_observation_period.name
        }
        ResultPath = "$.Job2Result"
        Next       = "ValidatePerson"
        Catch = [{
          ErrorEquals = ["States.ALL"]
          Next        = "PipelineFailed"
          ResultPath  = "$.error"
        }]
      }

      # Validate Person
      ValidatePerson = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.validate_person.name
        }
        ResultPath = "$.ValidatePersonResult"
        Next       = "Job3_VisitOccurrence"
        Catch = [{
          ErrorEquals = ["States.ALL"]
          Next        = "PipelineFailed"
          ResultPath  = "$.error"
        }]
      }

      # Job 3: Visit Occurrence
      Job3_VisitOccurrence = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.job_3_visit_occurrence.name
        }
        ResultPath = "$.Job3Result"
        Next       = "ValidateVisitOccurrence"
        Catch = [{
          ErrorEquals = ["States.ALL"]
          Next        = "PipelineFailed"
          ResultPath  = "$.error"
        }]
      }

      # Validate Visit Occurrence
      ValidateVisitOccurrence = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.validate_visit_occurrence.name
        }
        ResultPath = "$.ValidateVisitOccurrenceResult"
        Next       = "ParallelJob4And5"
        Catch = [{
          ErrorEquals = ["States.ALL"]
          Next        = "PipelineFailed"
          ResultPath  = "$.error"
        }]
      }

      # Job 4 & 5 run in parallel with their validations
      ParallelJob4And5 = {
        Type = "Parallel"
        Branches = [
          {
            StartAt = "Job4_ClinicalEvents"
            States = {
              Job4_ClinicalEvents = {
                Type     = "Task"
                Resource = "arn:aws:states:::glue:startJobRun.sync"
                Parameters = {
                  JobName = aws_glue_job.job_4_clinical_events.name
                }
                ResultPath = "$.Job4Result"
                Next       = "ValidateClinical"
              }
              ValidateClinical = {
                Type     = "Task"
                Resource = "arn:aws:states:::glue:startJobRun.sync"
                Parameters = {
                  JobName = aws_glue_job.validate_clinical.name
                }
                ResultPath = "$.ValidateClinicalResult"
                End        = true
              }
            }
          },
          {
            StartAt = "Job5_DrugExposure"
            States = {
              Job5_DrugExposure = {
                Type     = "Task"
                Resource = "arn:aws:states:::glue:startJobRun.sync"
                Parameters = {
                  JobName = aws_glue_job.job_5_drug_exposure.name
                }
                ResultPath = "$.Job5Result"
                Next       = "ValidateDrugs"
              }
              ValidateDrugs = {
                Type     = "Task"
                Resource = "arn:aws:states:::glue:startJobRun.sync"
                Parameters = {
                  JobName = aws_glue_job.validate_drugs.name
                }
                ResultPath = "$.ValidateDrugsResult"
                End        = true
              }
            }
          }
        ]
        ResultPath = "$.ParallelResult"
        Next       = "Job6_EraMetadata"
        Catch = [{
          ErrorEquals = ["States.ALL"]
          Next        = "PipelineFailed"
          ResultPath  = "$.error"
        }]
      }

      # Job 6: Era & Metadata
      Job6_EraMetadata = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.job_6_era_metadata.name
        }
        ResultPath = "$.Job6Result"
        Next       = "ValidateFinal"
        Catch = [{
          ErrorEquals = ["States.ALL"]
          Next        = "PipelineFailed"
          ResultPath  = "$.error"
        }]
      }

      # Final Validation
      ValidateFinal = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.validate_final.name
        }
        ResultPath = "$.ValidateFinalResult"
        Next       = "RunCrawler"
        Catch = [{
          ErrorEquals = ["States.ALL"]
          Next        = "PipelineFailed"
          ResultPath  = "$.error"
        }]
      }

      # Run Glue Crawler to update catalog
      RunCrawler = {
        Type     = "Task"
        Resource = "arn:aws:states:::aws-sdk:glue:startCrawler"
        Parameters = {
          Name = aws_glue_crawler.medallion.name
        }
        ResultPath = "$.CrawlerResult"
        Next       = "WaitForCrawler"
        Catch = [{
          ErrorEquals = ["States.ALL"]
          Next        = "PipelineSuccess"  # Crawler failure is not critical
          ResultPath  = "$.error"
        }]
      }

      WaitForCrawler = {
        Type    = "Wait"
        Seconds = 60
        Next    = "CheckCrawlerStatus"
      }

      CheckCrawlerStatus = {
        Type     = "Task"
        Resource = "arn:aws:states:::aws-sdk:glue:getCrawler"
        Parameters = {
          Name = aws_glue_crawler.medallion.name
        }
        ResultPath = "$.CrawlerStatus"
        Next       = "IsCrawlerReady"
      }

      IsCrawlerReady = {
        Type = "Choice"
        Choices = [
          {
            Variable      = "$.CrawlerStatus.Crawler.State"
            StringEquals  = "READY"
            Next          = "PipelineSuccess"
          }
        ]
        Default = "WaitForCrawler"
      }

      PipelineSuccess = {
        Type = "Succeed"
      }

      PipelineFailed = {
        Type  = "Fail"
        Error = "PipelineExecutionFailed"
        Cause = "One or more ETL jobs or validations failed during execution"
      }
    }
  })

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.step_functions.arn}:*"
    include_execution_data = true
    level                  = "ALL"
  }

  tags = var.tags
}


# CloudWatch Log Group for Step Functions
resource "aws_cloudwatch_log_group" "step_functions" {
  name              = "/aws/vendedlogs/states/${var.project_name}-etl-pipeline"
  retention_in_days = var.log_retention_days

  tags = var.tags
}

# =============================================================================
# EventBridge Rule for Scheduled Execution
# =============================================================================

resource "aws_cloudwatch_event_rule" "weekly_etl" {
  name                = "${var.project_name}-weekly-etl"
  description         = "Trigger ETL pipeline weekly on Sunday at 1:00 AM UTC"
  schedule_expression = "cron(0 1 ? * SUN *)"
  state               = "DISABLED"  # Disabled by default, enable when ready

  tags = var.tags
}

resource "aws_cloudwatch_event_target" "etl_pipeline" {
  rule      = aws_cloudwatch_event_rule.weekly_etl.name
  target_id = "TriggerETLPipeline"
  arn       = aws_sfn_state_machine.etl_pipeline.arn
  role_arn  = aws_iam_role.eventbridge_role.arn

  input = jsonencode({
    source    = "scheduled"
    timestamp = "<aws.scheduler.scheduled-time>"
  })
}

# EventBridge Role
resource "aws_iam_role" "eventbridge_role" {
  name = "${var.project_name}-eventbridge-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "events.amazonaws.com"
        }
      }
    ]
  })

  tags = var.tags
}

resource "aws_iam_role_policy" "eventbridge_step_functions" {
  name = "${var.project_name}-eventbridge-sfn"
  role = aws_iam_role.eventbridge_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "states:StartExecution"
        Resource = aws_sfn_state_machine.etl_pipeline.arn
      }
    ]
  })
}

# =============================================================================
# PHASE 1: Initial Setup State Machine (One-Time)
# =============================================================================

resource "aws_sfn_state_machine" "initial_setup" {
  name     = "${var.project_name}-initial-setup"
  role_arn = aws_iam_role.step_functions_role.arn

  definition = jsonencode({
    Comment = "OMOP CDM Lakehouse Initial Setup - Phase 1 (Run Once)"
    StartAt = "LoadVocabulary"
    States = {
      # Load 33 OMOP vocabulary tables
      LoadVocabulary = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.vocabulary_loader.name
        }
        ResultPath = "$.VocabularyResult"
        Next       = "RunCrawler"
        Catch = [{
          ErrorEquals = ["States.ALL"]
          Next        = "SetupFailed"
          ResultPath  = "$.error"
        }]
      }

      RunCrawler = {
        Type     = "Task"
        Resource = "arn:aws:states:::aws-sdk:glue:startCrawler"
        Parameters = {
          Name = aws_glue_crawler.medallion.name
        }
        ResultPath = "$.CrawlerResult"
        Next       = "SetupSuccess"
        Catch = [{
          ErrorEquals = ["States.ALL"]
          Next        = "SetupSuccess"
          ResultPath  = "$.error"
        }]
      }

      SetupSuccess = {
        Type = "Succeed"
      }

      SetupFailed = {
        Type  = "Fail"
        Error = "InitialSetupFailed"
        Cause = "Vocabulary loading or crawler failed"
      }
    }
  })

  tags = var.tags
}
