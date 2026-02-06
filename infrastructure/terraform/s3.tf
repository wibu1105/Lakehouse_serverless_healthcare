# S3 Buckets for OMOP CDM Lakehouse
# Main data lake bucket with medallion architecture prefixes

# Main Data Lake Bucket
resource "aws_s3_bucket" "data_lake" {
  bucket = var.s3_bucket_name

  tags = merge(var.tags, {
    Name = var.s3_bucket_name
    Layer = "data-lake"
  })
}

resource "aws_s3_bucket_versioning" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  versioning_configuration {
    status = var.enable_versioning ? "Enabled" : "Disabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = aws_kms_key.s3_key.arn
      sse_algorithm     = "aws:kms"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  rule {
    id     = "medallion-lifecycle"
    status = "Enabled"

    filter {
      prefix = ""
    }

    transition {
      days          = var.data_lifecycle_days.to_standard_ia
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = var.data_lifecycle_days.to_glacier
      storage_class = "GLACIER"
    }

    noncurrent_version_expiration {
      noncurrent_days = var.data_lifecycle_days.delete_noncurrent
    }
  }
}

# Create medallion layer prefixes
resource "aws_s3_object" "raw_layer" {
  bucket  = aws_s3_bucket.data_lake.id
  key     = "raw/"
  content = ""
}

resource "aws_s3_object" "bronze_layer" {
  bucket  = aws_s3_bucket.data_lake.id
  key     = "bronze/"
  content = ""
}

resource "aws_s3_object" "silver_layer" {
  bucket  = aws_s3_bucket.data_lake.id
  key     = "silver/"
  content = ""
}

resource "aws_s3_object" "gold_layer" {
  bucket  = aws_s3_bucket.data_lake.id
  key     = "gold/"
  content = ""
}

# Logs Bucket
resource "aws_s3_bucket" "logs" {
  bucket = "${var.s3_bucket_name}-logs"

  tags = merge(var.tags, {
    Name = "${var.s3_bucket_name}-logs"
    Purpose = "logging"
  })
}

resource "aws_s3_bucket_versioning" "logs" {
  bucket = aws_s3_bucket.logs.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "logs" {
  bucket = aws_s3_bucket.logs.id

  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = aws_kms_key.s3_key.arn
      sse_algorithm     = "aws:kms"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "logs" {
  bucket = aws_s3_bucket.logs.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "logs" {
  bucket = aws_s3_bucket.logs.id

  rule {
    id     = "logs-lifecycle"
    status = "Enabled"

    filter {
      prefix = ""
    }

    expiration {
      days = 90
    }
  }
}

# Athena Query Results Bucket
resource "aws_s3_bucket" "athena_results" {
  bucket = "${var.s3_bucket_name}-athena-results"

  tags = merge(var.tags, {
    Name = "${var.s3_bucket_name}-athena-results"
    Purpose = "athena-results"
  })
}

resource "aws_s3_bucket_versioning" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id

  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = aws_kms_key.s3_key.arn
      sse_algorithm     = "aws:kms"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id

  rule {
    id     = "athena-results-lifecycle"
    status = "Enabled"

    filter {
      prefix = ""
    }

    expiration {
      days = 7
    }
  }
}

# Enable access logging from data lake to logs bucket
resource "aws_s3_bucket_logging" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  target_bucket = aws_s3_bucket.logs.id
  target_prefix = "s3-access-logs/"
}
