# Lake Formation Configuration for OMOP CDM Lakehouse
# Data lake governance and access control
#
# NOTE: Table-level permissions are commented out because they require tables
# to exist first. After running ETL pipeline to create tables, you can uncomment
# these resources and run terraform apply again.

# Lake Formation Settings
resource "aws_lakeformation_data_lake_settings" "settings" {
  admins = [
    aws_iam_role.lake_formation_role.arn
  ]

  # Use IAM-based access control by default (simpler for initial setup)
  create_database_default_permissions {
    permissions = ["ALL"]
    principal   = "IAM_ALLOWED_PRINCIPALS"
  }

  create_table_default_permissions {
    permissions = ["ALL"]
    principal   = "IAM_ALLOWED_PRINCIPALS"
  }
}

# Register S3 location with Lake Formation
resource "aws_lakeformation_resource" "data_lake" {
  arn      = aws_s3_bucket.data_lake.arn
  role_arn = aws_iam_role.lake_formation_role.arn
}

# Grant permissions to Glue role on database
resource "aws_lakeformation_permissions" "glue_database" {
  principal   = aws_iam_role.glue_role.arn
  permissions = ["ALL"]

  database {
    name = aws_glue_catalog_database.healthcare_db.name
  }

  depends_on = [aws_glue_catalog_database.healthcare_db]
}

# Grant permissions to Athena role on database
resource "aws_lakeformation_permissions" "athena_database" {
  principal   = aws_iam_role.athena_role.arn
  permissions = ["DESCRIBE"]

  database {
    name = aws_glue_catalog_database.healthcare_db.name
  }

  depends_on = [aws_glue_catalog_database.healthcare_db]
}


# ==============================================================================
# TABLE-LEVEL PERMISSIONS (Uncomment after tables are created by ETL pipeline)
# ==============================================================================
# 
# resource "aws_lakeformation_permissions" "glue_tables" {
#   principal   = aws_iam_role.glue_role.arn
#   permissions = ["ALL"]
# 
#   table {
#     database_name = aws_glue_catalog_database.healthcare_db.name
#     wildcard      = true
#   }
# 
#   depends_on = [aws_lakeformation_permissions.glue_database]
# }
# 
# resource "aws_lakeformation_permissions" "athena_tables" {
#   principal   = aws_iam_role.athena_role.arn
#   permissions = ["SELECT", "DESCRIBE"]
# 
#   table {
#     database_name = aws_glue_catalog_database.healthcare_db.name
#     wildcard      = true
#   }
# 
#   depends_on = [aws_lakeformation_permissions.athena_database]
# }
