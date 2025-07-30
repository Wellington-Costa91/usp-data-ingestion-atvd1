# AWS Glue ETL Job: S3 CSV → Aurora PostgreSQL

## 📌 Descrição
Este script Python implementa um **ETL (Extract, Transform, Load)** utilizando **AWS Glue**, que lê arquivos CSV do **Amazon S3**, aplica transformações e grava os dados em uma tabela do **Amazon Aurora PostgreSQL**.  
Ele utiliza conexões do Glue e credenciais armazenadas no **AWS Secrets Manager** para autenticação segura.

---

## 🚀 Funcionalidades Principais
- 🔹 **Leitura de CSV do S3** com detecção automática do separador.  
- 🔹 **Transformação de dados**, incluindo:
  - Criação/normalização de coluna `id` (primary key).  
  - Adição de colunas `created_at` e `updated_at`.  
  - Padronização dos nomes das colunas.  
- 🔹 **Criação automática de schema e tabela** no Aurora PostgreSQL, caso não existam.  
- 🔹 **Escrita dos dados** no banco utilizando JDBC com batch otimizado.  
- 🔹 **Verificação pós-inserção** para confirmar a carga.  
- 🔹 **Logging detalhado** em todas as etapas.  

---

## 📂 Estrutura do Projeto
```
etl_s3_to_aurora.py   # Script principal do Glue Job
```

---

## ⚙️ Parâmetros Esperados

| Parâmetro         | Descrição                                           | Exemplo                          |
|-------------------|-----------------------------------------------------|---------------------------------|
| `JOB_NAME`        | Nome do job no AWS Glue                             | `etl_s3_to_aurora`              |
| `S3_INPUT_PATH`   | Caminho do arquivo CSV no S3                        | `s3://bucket/dados/arquivo.csv` |
| `TARGET_TABLE`    | Nome da tabela destino no Aurora PostgreSQL         | `clientes`                      |
| `CONNECTION_NAME` | Nome da conexão Glue para JDBC (Aurora)             | `aurora-connection`             |
| `SECRET_NAME`     | Nome do secret no AWS Secrets Manager               | `aurora-db-secret`              |
| `DATABASE_NAME`   | Nome do banco de dados no Aurora                    | `mydatabase`                    |
| `SCHEMA_NAME`     | Nome do schema no banco (será criado se não existir)| `public`                        |

---

## 🔑 Formato do Secret no AWS Secrets Manager
```json
{
  "username": "dbuser",
  "password": "dbpassword",
  "host": "aurora-cluster-endpoint",
  "port": 5432
}
```

---

## 🏗️ Dependências
- AWS Glue com PySpark (já disponível no ambiente Glue)
- `boto3`
- `psycopg2`
- Conexão JDBC configurada no Glue
- Secret configurado no Secrets Manager

---

## ▶️ Execução no AWS Glue
1. Faça upload do script para o S3.  
2. Crie um Job no Glue apontando para o script.  
3. Configure os parâmetros necessários.  
4. Execute o job e monitore os logs no CloudWatch.  

Exemplo de execução local:
```bash
spark-submit etl_s3_to_aurora.py \
  --JOB_NAME etl_s3_to_aurora \
  --S3_INPUT_PATH s3://bucket/dados/arquivo.csv \
  --TARGET_TABLE clientes \
  --CONNECTION_NAME aurora-connection \
  --SECRET_NAME aurora-db-secret \
  --DATABASE_NAME mydatabase \
  --SCHEMA_NAME public
```

---

## 📜 Fluxo do Processo
1. 🔍 **Recupera credenciais** do Aurora via Secrets Manager.  
2. 📥 **Lê arquivo CSV** do S3.  
3. 🔄 **Aplica transformações** e garante consistência dos dados.  
4. 🏗️ **Cria schema/tabela** no Aurora se não existirem.  
5. 📝 **Escreve os dados** no banco (modo overwrite ou append).  
6. ✅ **Verifica inserção** e registra logs detalhados.  

---

## ✅ Logs
Utiliza o logger nativo do Glue para exibir progresso, contagens, schema e erros detalhados.

---

## 📌 Observações
- A conexão JDBC do Glue deve possuir acesso à instância do Aurora.  
- A role do Glue deve ter permissões para acessar S3, Secrets Manager e Aurora.

