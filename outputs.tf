output "aurora_endpoint" {
  description = "Endpoint do Aurora Serverless"
  value       = aws_rds_cluster.aurora_serverless.endpoint
}

output "aurora_port" {
  description = "Porta do Aurora Serverless"
  value       = aws_rds_cluster.aurora_serverless.port
}

output "database_name" {
  description = "Nome do banco de dados"
  value       = aws_rds_cluster.aurora_serverless.database_name
}

output "database_username" {
  description = "Username do banco de dados"
  value       = aws_rds_cluster.aurora_serverless.master_username
}

output "database_password" {
  description = "Senha do banco de dados"
  value       = random_password.db_password.result
  sensitive   = true
}

output "glue_database_name" {
  description = "Nome do database do Glue Catalog"
  value       = aws_glue_catalog_database.main.name
}

output "glue_connection_name" {
  description = "Nome da conexão do Glue para Aurora"
  value       = aws_glue_connection.postgresql.name
}

output "glue_role_arn" {
  description = "ARN da role do Glue"
  value       = aws_iam_role.glue_role.arn
}

output "glue_job_name" {
  description = "Nome do Glue Job"
  value       = aws_glue_job.etl_job.name
}

output "glue_crawler_name" {
  description = "Nome do Glue Crawler"
  value       = aws_glue_crawler.s3_crawler.name
}

output "data_verification_command" {
  description = "Comando para verificar os dados no Aurora"
  value = "PGPASSWORD='${random_password.db_password.result}' psql -h ${aws_rds_cluster.aurora_serverless.endpoint} -U ${var.db_username} -d ${var.db_name}"
  sensitive = true
}

output "create_tables_command" {
  description = "Comando para criar tabelas no Aurora"
  value = "PGPASSWORD='${random_password.db_password.result}' psql -h ${aws_rds_cluster.aurora_serverless.endpoint} -U ${var.db_username} -d ${var.db_name} -f ./sql/create_tables.sql"
  sensitive = true
}

output "glue_studio_url" {
  description = "URL para acessar o AWS Glue Studio"
  value = "https://${var.aws_region}.console.aws.amazon.com/gluestudio/home?region=${var.aws_region}#/jobs"
}
