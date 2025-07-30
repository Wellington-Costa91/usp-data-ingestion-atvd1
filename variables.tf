variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Nome do projeto"
  type        = string
  default     = "etl-ingestao-dados"
}

variable "db_name" {
  description = "Nome do banco de dados PostgreSQL"
  type        = string
  default     = "etl_database"
}

variable "db_username" {
  description = "Username do banco de dados PostgreSQL"
  type        = string
  default     = "etl_user"
}
