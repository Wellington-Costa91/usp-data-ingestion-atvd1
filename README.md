# 📊 Projeto ETL - Ingestão e Tratamento de Dados com AWS Glue

Este projeto implementa uma solução completa de **ETL (Extract, Transform, Load)** usando **AWS Glue** como ferramenta visual para ingestão e tratamento de dados, com **Aurora Serverless PostgreSQL** como banco de dados de destino.

## 🎯 Objetivo

Criar um pipeline de dados que:
- **Ingere** dados de múltiplas fontes (CSV, TSV)
- **Transforma** e limpa os dados usando AWS Glue
- **Carrega** os dados tratados em um banco PostgreSQL
- **Consolida** informações de diferentes fontes em uma tabela final

## 🏗️ Arquitetura da Solução

```
┌─────────────────┐    ┌──────────────┐    ┌─────────────────┐    ┌──────────────────┐
│   Dados Locais  │───▶│   Amazon S3  │───▶│   AWS Glue      │───▶│ Aurora Serverless│
│   (CSV/TSV)     │    │  (Raw Data)  │    │ (ETL Visual)    │    │   PostgreSQL     │
└─────────────────┘    └──────────────┘    └─────────────────┘    └──────────────────┘
                              │                      │
                              ▼                      ▼
                       ┌──────────────┐    ┌─────────────────┐
                       │ Glue Catalog │    │ Tabela Final    │
                       │ (Metadados)  │    │ Consolidada     │
                       └──────────────┘    └─────────────────┘
```

## 📁 Estrutura dos Dados

### Fontes de Dados (Pasta `Dados/`):

1. **📈 Reclamações** (`Dados/Reclamacoes/`)
   - Dados trimestrais de reclamações bancárias (2021-2022)
   - Formato: CSV com separador `;`
   - Contém: CNPJ, instituição, quantidade de reclamações, índices

2. **🏦 Bancos** (`Dados/Bancos/`)
   - Informações de enquadramento das instituições financeiras
   - Formato: TSV (Tab-separated values)
   - Contém: Segmento, CNPJ, nome da instituição

3. **👥 Empregados** (`Dados/Empregados/`)
   - Avaliações de funcionários do Glassdoor
   - Formato: CSV com separador `|`
   - Contém: Avaliações, cultura, salários, benefícios

### Tabelas de Destino no Aurora:

- **`reclamacoes`** - Dados consolidados de reclamações
- **`bancos_enquadramento`** - Informações dos bancos por segmento
- **`empregados_glassdoor`** - Avaliações dos funcionários
- **`dados_consolidados`** - **Tabela final** com dados unidos e tratados

## 🛠️ Componentes da Solução

### 1. **Amazon S3** - Armazenamento de Dados
- **Bucket de dados**: Armazena arquivos CSV/TSV originais
- **Bucket de scripts**: Contém scripts Python do Glue
- **Bucket temporário**: Arquivos temporários do processamento

### 2. **AWS Glue** - Ferramenta Visual de ETL
- **Glue Studio**: Interface visual para criar jobs de ETL
- **Glue Catalog**: Catálogo de metadados das tabelas
- **Glue Crawler**: Descobre automaticamente esquemas dos dados
- **Glue Job**: Executa o processamento ETL

### 3. **Aurora Serverless v2 PostgreSQL** - Banco de Dados
- **Serverless v2**: Paga apenas pelo que usa, auto-scaling contínuo
- **Capacidade**: 0.5-2 ACU (Aurora Capacity Units)
- **Escalabilidade**: Escala automaticamente conforme demanda
- **Economia**: Reduz para 0.5 ACU quando inativo

### 4. **Networking e Segurança**
- **VPC**: Rede privada isolada
- **Subnets**: Públicas (NAT) e privadas (Aurora/Glue)
- **Security Groups**: Controle de acesso entre componentes
- **IAM Roles**: Permissões granulares para cada serviço

## 🚀 Como Executar

### Pré-requisitos
- AWS CLI configurado (`aws configure`)
- Terraform instalado (>= 1.0)
- PostgreSQL client (psql) para verificação

### Execução Completa

```bash
# 1. Inicializar Terraform
terraform init

# 2. Aplicar toda a infraestrutura (15-20 min)
terraform apply

# 3. Criar tabelas no Aurora
terraform output -raw create_tables_command | bash

# 4. Verificar dados carregados
terraform output -raw data_verification_command
```

### O que Acontece Automaticamente:

1. ✅ **Infraestrutura AWS** criada (VPC, S3, Aurora, Glue, IAM)
2. ✅ **Upload de dados** da pasta `Dados/` para S3
3. ✅ **Catalogação** dos dados pelo Glue Crawler
4. ✅ **Conexões** configuradas entre Glue e Aurora
5. ✅ **Job ETL** pronto para execução visual

## 🎨 Usando a Ferramenta Visual

### Acessar o AWS Glue Studio:
```bash
# Obter URL do Glue Studio
terraform output glue_studio_url
```

### No Glue Studio você pode:
- **Visualizar** o fluxo de dados graficamente
- **Modificar** transformações usando interface drag-and-drop
- **Adicionar** novos nós de processamento
- **Testar** diferentes configurações
- **Executar** jobs com um clique

### Recursos Pré-configurados:
- **Conexão Aurora**: `terraform output glue_connection_name`
- **Database Catalog**: `terraform output glue_database_name`
- **Dados S3**: Catalogados e prontos para uso
- **Job ETL**: `terraform output glue_job_name`

## 📊 Fluxo de Dados Detalhado

### 1. **Extract (Extração)**
```
Dados Locais → S3 → Glue Catalog
```
- Arquivos CSV/TSV são enviados para S3
- Glue Crawler descobre esquemas automaticamente
- Metadados ficam disponíveis no Catalog

### 2. **Transform (Transformação)**
```
S3 Data → Spark Processing → Cleaned Data
```
- **Limpeza**: Remove caracteres especiais, normaliza encoding
- **Conversão**: Ajusta tipos de dados (string → integer, decimal)
- **Agregação**: Soma reclamações por CNPJ/ano
- **Join**: Une dados de diferentes fontes

### 3. **Load (Carregamento)**
```
Processed Data → Aurora Serverless → Final Tables
```
- Dados limpos são inseridos em tabelas individuais
- Tabela consolidada é criada com joins entre fontes
- Índices são criados para performance

## 📋 Estrutura das Tabelas

### Tabela `dados_consolidados` (Principal)
```sql
CREATE TABLE dados_consolidados (
    id SERIAL PRIMARY KEY,
    cnpj VARCHAR(20),                    -- CNPJ da instituição
    nome_instituicao VARCHAR(200),       -- Nome do banco
    segmento VARCHAR(10),                -- S1, S2, S3, etc.
    ano INTEGER,                         -- Ano de referência
    total_reclamacoes INTEGER,           -- Total de reclamações
    total_clientes BIGINT,               -- Total de clientes
    indice_reclamacoes DECIMAL(10,2),    -- Índice médio
    avaliacao_geral DECIMAL(3,1),        -- Nota Glassdoor
    cultura_valores DECIMAL(3,1),        -- Avaliação cultura
    remuneracao_beneficios DECIMAL(3,1), -- Avaliação salários
    recomendam_empresa DECIMAL(5,1),     -- % recomendação
    data_processamento TIMESTAMP,
    UNIQUE(cnpj, ano)
);
```

## 🔍 Verificação e Monitoramento

### Verificar Dados Carregados:
```bash
# Conectar ao Aurora
terraform output -raw data_verification_command

# Contar registros por tabela
SELECT 'reclamacoes' as tabela, count(*) FROM reclamacoes
UNION ALL
SELECT 'bancos_enquadramento', count(*) FROM bancos_enquadramento
UNION ALL
SELECT 'empregados_glassdoor', count(*) FROM empregados_glassdoor
UNION ALL
SELECT 'dados_consolidados', count(*) FROM dados_consolidados;
```

### Monitorar Jobs Glue:
```bash
# Listar execuções do job
aws glue get-job-runs --job-name $(terraform output -raw glue_job_name)

# Ver logs no CloudWatch
aws logs describe-log-groups --log-group-name-prefix /aws-glue
```

## 💰 Custos Estimados

| Serviço | Custo Mensal | Observações |
|---------|--------------|-------------|
| Aurora Serverless v2 | $5-15 | Escala de 0.5-2 ACU conforme uso |
| S3 | $1-3 | Poucos GB de dados |
| Glue | $2-5 | Por execução (~$0.44/hora) |
| VPC/Networking | Gratuito | Dentro dos limites |
| **Total** | **$8-23** | Muito econômico para desenvolvimento |

## 🔧 Personalização

### Modificar Configurações:
```hcl
# variables.tf
variable "project_name" {
  default = "meu-projeto-etl"  # Altere aqui
}

variable "aws_region" {
  default = "us-west-2"        # Altere a região
}
```

### Adicionar Novos Dados:
1. Coloque arquivos na pasta `Dados/`
2. Execute `terraform apply` para upload
3. Execute Glue Crawler para catalogar
4. Modifique job ETL no Glue Studio

## 🧹 Limpeza

```bash
# Remover toda a infraestrutura
terraform destroy
```

## 🎓 Conceitos Aprendidos

- **ETL com Ferramenta Visual**: AWS Glue Studio
- **Banco Serverless v2**: Aurora com auto-scaling contínuo
- **Infraestrutura como Código**: Terraform
- **Data Lake**: S3 como repositório central
- **Catalogação**: Glue Catalog para metadados
- **Processamento Distribuído**: Spark no Glue
- **Networking AWS**: VPC, subnets, security groups

## 📚 Próximos Passos

1. **Agendamento**: Configurar execução automática (CloudWatch Events)
2. **Monitoramento**: Alertas para falhas no ETL
3. **Qualidade**: Validação de dados com Great Expectations
4. **Visualização**: Conectar com QuickSight ou Grafana
5. **CI/CD**: Pipeline automatizado com GitHub Actions

---

**🎉 Parabéns! Você criou uma solução completa de ETL usando ferramentas visuais na AWS!**
