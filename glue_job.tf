# Upload do script do Glue Job para S3
resource "aws_s3_object" "glue_etl_script" {
  bucket = aws_s3_bucket.glue_scripts.bucket
  key    = "scripts/etl_job.py"
  source = "./glue_scripts/etl_job.py"
  etag   = filemd5("./glue_scripts/etl_job.py")

  depends_on = [aws_s3_bucket.glue_scripts]
}

# Glue Job para ETL Visual
resource "aws_glue_job" "etl_job" {
  name         = "${var.project_name}-etl-job-${random_id.bucket_suffix.hex}"
  role_arn     = aws_iam_role.glue_role.arn
  glue_version = "4.0"

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.glue_scripts.bucket}/scripts/etl_job.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--job-bookmark-option"              = "job-bookmark-disable"
    "--enable-metrics"                   = "true"
    "--enable-spark-ui"                  = "true"
    "--spark-event-logs-path"           = "s3://${aws_s3_bucket.glue_temp.bucket}/sparkHistoryLogs/"
    "--enable-job-insights"             = "true"
    "--enable-observability-metrics"    = "true"
    "--enable-glue-datacatalog"         = "true"
    "--S3_BUCKET"                       = aws_s3_bucket.data_source.bucket
    "--DATABASE_NAME"                   = var.db_name
    "--CONNECTION_NAME"                 = aws_glue_connection.postgresql.name
    "--TempDir"                         = "s3://${aws_s3_bucket.glue_temp.bucket}/temp/"
    "--additional-python-modules"       = "psycopg2-binary"
  }

  connections = [aws_glue_connection.postgresql.name]

  execution_property {
    max_concurrent_runs = 1
  }

  max_retries = 1
  timeout     = 60

  worker_type       = "G.1X"
  number_of_workers = 2

  tags = {
    Name = "${var.project_name}-etl-job"
    Type = "Visual"
  }

  depends_on = [
    aws_s3_object.glue_etl_script,
    aws_glue_connection.postgresql
  ]
}

# Glue Crawler para catalogar dados do S3
resource "aws_glue_crawler" "s3_crawler" {
  database_name = aws_glue_catalog_database.main.name
  name          = "${var.project_name}-s3-crawler-${random_id.bucket_suffix.hex}"
  role          = aws_iam_role.glue_role.arn

  s3_target {
    path = "s3://${aws_s3_bucket.data_source.bucket}/raw-data/"
  }

  configuration = jsonencode({
    Version = 1.0
    CrawlerOutput = {
      Partitions = { AddOrUpdateBehavior = "InheritFromTable" }
    }
  })

  tags = {
    Name = "${var.project_name}-s3-crawler"
  }
}

# Glue Trigger para execução manual
resource "aws_glue_trigger" "etl_trigger" {
  name = "${var.project_name}-etl-trigger-${random_id.bucket_suffix.hex}"
  type = "ON_DEMAND"

  actions {
    job_name = aws_glue_job.etl_job.name
  }

  tags = {
    Name = "${var.project_name}-etl-trigger"
  }
}
