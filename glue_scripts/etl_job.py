#!/usr/bin/env python3
"""
AWS Glue ETL Job: S3 CSV to Aurora PostgreSQL
Lê arquivo CSV do S3 e grava em tabela Aurora PostgreSQL
Usa conexões Glue e credenciais do Secrets Manager
"""

import sys
import boto3
import json
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# =============================================================================
# CONFIGURAÇÃO INICIAL
# =============================================================================

# Obter argumentos do job
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'S3_INPUT_PATH',
    'TARGET_TABLE',
    'CONNECTION_NAME',
    'SECRET_NAME',
    'DATABASE_NAME',
    'SCHEMA_NAME'  # Novo parâmetro para schema específico
])

# Inicializar contextos Spark/Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Configurar logging
logger = glueContext.get_logger()
logger.info(f"=== INICIANDO JOB ETL: {args['JOB_NAME']} ===")

# =============================================================================
# FUNÇÕES AUXILIARES
# =============================================================================

def get_secret_value(secret_name: str, region: str = 'us-east-1') -> dict:
    """
    Recupera credenciais do AWS Secrets Manager
    """
    try:
        logger.info(f"Recuperando credenciais do secret: {secret_name}")
        
        # Cliente Secrets Manager
        secrets_client = boto3.client('secretsmanager', region_name=region)
        
        # Obter secret
        response = secrets_client.get_secret_value(SecretId=secret_name)
        secret_string = response['SecretString']
        
        # Parse JSON
        credentials = json.loads(secret_string)
        logger.info("✅ Credenciais recuperadas com sucesso")
        
        return credentials
        
    except Exception as e:
        logger.error(f"❌ Erro ao recuperar credenciais: {str(e)}")
        raise e

def detect_csv_separator(s3_path: str) -> str:
    """
    Detecta o separador do CSV baseado na extensão e conteúdo
    """
    try:
        if s3_path.lower().endswith('.tsv'):
            return '\t'
        elif '|' in s3_path or 'glassdoor' in s3_path.lower():
            return '|'
        else:
            return ','
    except:
        return ','

def read_csv_from_s3(s3_path: str) -> DataFrame:
    """
    Lê arquivo CSV do S3 usando Spark
    """
    try:
        logger.info(f"Lendo arquivo CSV do S3: {s3_path}")
        
        # Detectar separador automaticamente
        separator = detect_csv_separator(s3_path)
        logger.info(f"📋 Separador detectado: '{separator}'")
        
        # Ler CSV com inferência de schema
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("encoding", "UTF-8") \
            .option("sep", separator) \
            .option("quote", '"') \
            .option("escape", '"') \
            .option("multiline", "true") \
            .csv(s3_path)
        
        # Log informações do DataFrame
        row_count = df.count()
        col_count = len(df.columns)
        
        logger.info(f"✅ CSV lido com sucesso:")
        logger.info(f"   - Linhas: {row_count:,}")
        logger.info(f"   - Colunas: {col_count}")
        logger.info(f"   - Schema: {df.schema}")
        
        # Mostrar primeiras linhas (para debug)
        logger.info("📋 Primeiras 5 linhas:")
        df.show(5, truncate=False)
        
        return df
        
    except Exception as e:
        logger.error(f"❌ Erro ao ler CSV do S3: {str(e)}")
        raise e

def transform_data(df: DataFrame) -> DataFrame:
    """
    Aplica transformações nos dados (customize conforme necessário)
    """
    try:
        logger.info("🔄 Aplicando transformações nos dados...")
        
        # Verificar se coluna 'id' existe (com variações)
        column_names_lower = [col.lower() for col in df.columns]
        id_variations = ['id', 'id_', '_id', 'pk', 'primary_key', 'key']
        
        existing_id_col = None
        for variation in id_variations:
            if variation in column_names_lower:
                # Encontrar o nome original da coluna
                original_idx = column_names_lower.index(variation)
                existing_id_col = df.columns[original_idx]
                break
        
        if existing_id_col:
            logger.info(f"✅ Coluna ID encontrada: '{existing_id_col}'")
            # Renomear para 'id' se necessário
            if existing_id_col.lower() != 'id':
                df = df.withColumnRenamed(existing_id_col, 'id')
                logger.info(f"🔄 Coluna '{existing_id_col}' renomeada para 'id'")
        else:
            logger.info("⚠️ Coluna 'id' não encontrada. Criando ID incremental...")
            # Adicionar coluna ID incremental usando row_number()
            from pyspark.sql.window import Window
            
            # Criar window spec para row_number
            window_spec = Window.orderBy(monotonically_increasing_id())
            
            # Adicionar coluna ID incremental
            df = df.withColumn("id", row_number().over(window_spec))
            logger.info("✅ Coluna 'id' criada com valores incrementais")
        
        # Aplicar outras transformações
        df_transformed = df \
            .withColumn("created_at", current_timestamp()) \
            .withColumn("updated_at", current_timestamp())
        
        # Validar se ID não é nulo (agora que garantimos que existe)
        initial_count = df_transformed.count()
        df_transformed = df_transformed.filter(col("id").isNotNull())
        final_count = df_transformed.count()
        
        if initial_count != final_count:
            logger.warning(f"⚠️ Removidas {initial_count - final_count} linhas com ID nulo")
        
        # Limpar nomes de colunas (remover espaços, caracteres especiais)
        for old_col in df_transformed.columns:
            new_col = old_col.strip().lower().replace(" ", "").replace("-", "").replace(".", "_")
            # Remover caracteres especiais adicionais
            import re
            new_col = re.sub(r'[^\w]', '_', new_col)
            # Remover underscores múltiplos
            new_col = re.sub(r'+', '', new_col).strip('_')
            
            if old_col != new_col:
                df_transformed = df_transformed.withColumnRenamed(old_col, new_col)
                logger.info(f"🔄 Coluna renomeada: '{old_col}' → '{new_col}'")
        
        # Log informações sobre a coluna ID
        try:
            id_stats = df_transformed.agg(
                min("id").alias("min_id"),
                max("id").alias("max_id"),
                count("id").alias("count_id")
            ).collect()[0]
            
            logger.info(f"📊 Estatísticas da coluna ID:")
            logger.info(f"   - ID mínimo: {id_stats['min_id']}")
            logger.info(f"   - ID máximo: {id_stats['max_id']}")
            logger.info(f"   - Total de registros: {id_stats['count_id']:,}")
        except Exception as e:
            logger.warning(f"⚠️ Não foi possível calcular estatísticas do ID: {str(e)}")
        
        logger.info(f"✅ Transformações aplicadas. Linhas finais: {df_transformed.count():,}")
        
        # Log schema final
        logger.info("📋 Schema final:")
        for field in df_transformed.schema.fields:
            logger.info(f"   - {field.name}: {field.dataType}")
        
        return df_transformed
        
    except Exception as e:
        logger.error(f"❌ Erro nas transformações: {str(e)}")
        raise e

def write_to_aurora(df: DataFrame, connection_name: str, database_name: str, schema_name: str, table_name: str, credentials: dict, table_exists: bool = False):
    """
    Escreve DataFrame no Aurora PostgreSQL usando conexão Glue
    """
    try:
        logger.info(f"📝 Escrevendo dados na tabela Aurora: {database_name}.{schema_name}.{table_name}")
        
        # Construir JDBC URL
        jdbc_url = f"jdbc:postgresql://{credentials['host']}:{credentials['port']}/{database_name}"
        
        # Determinar modo de escrita
        write_mode = "append" if table_exists else "overwrite"
        logger.info(f"📋 Modo de escrita: {write_mode}")
        
        # Nome completo da tabela com schema
        full_table_name = f"{schema_name}.{table_name}"
        
        # Configurações de escrita
        write_options = {
            "url": jdbc_url,
            "dbtable": full_table_name,
            "user": credentials['username'],
            "password": credentials['password'],
            "driver": "org.postgresql.Driver",
            # Configurações de performance
            "batchsize": "1000",
            "isolationLevel": "READ_UNCOMMITTED"
        }
        
        # Escrever dados
        df.write \
            .format("jdbc") \
            .options(**write_options) \
            .mode(write_mode) \
            .save()
        
        logger.info(f"✅ Dados escritos com sucesso na tabela {full_table_name}")
        
        # Log estatísticas finais
        final_count = df.count()
        logger.info(f"📊 Total de registros inseridos: {final_count:,}")
        
        # Verificar dados na tabela após inserção
        verify_insertion(credentials, database_name, schema_name, table_name)
        
    except Exception as e:
        logger.error(f"❌ Erro ao escrever no Aurora: {str(e)}")
        raise e

def verify_insertion(credentials: dict, database_name: str, schema_name: str, table_name: str):
    """
    Verifica se os dados foram inseridos corretamente
    """
    try:
        logger.info(f"🔍 Verificando inserção na tabela {schema_name}.{table_name}...")
        
        import psycopg2
        
        # Conectar ao PostgreSQL
        conn = psycopg2.connect(
            host=credentials['host'],
            port=credentials['port'],
            database=database_name,
            user=credentials['username'],
            password=credentials['password']
        )
        
        cursor = conn.cursor()
        
        # Contar registros na tabela
        cursor.execute(f"SELECT COUNT(*) FROM {schema_name}.{table_name};")
        total_count = cursor.fetchone()[0]
        
        # Verificar primeiros registros
        cursor.execute(f"SELECT * FROM {schema_name}.{table_name} LIMIT 5;")
        sample_records = cursor.fetchall()
        
        # Obter nomes das colunas
        cursor.execute(f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = '{schema_name}' 
            AND table_name = '{table_name}' 
            ORDER BY ordinal_position;
        """)
        column_names = [row[0] for row in cursor.fetchall()]
        
        logger.info(f"✅ Verificação concluída:")
        logger.info(f"   - Total de registros na tabela: {total_count:,}")
        logger.info(f"   - Colunas na tabela: {len(column_names)}")
        logger.info(f"   - Nomes das colunas: {', '.join(column_names)}")
        
        if sample_records:
            logger.info("📋 Primeiros 5 registros inseridos:")
            for i, record in enumerate(sample_records, 1):
                logger.info(f"   Registro {i}: {record[:3]}...")  # Mostrar apenas primeiros 3 campos
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.warning(f"⚠️ Erro ao verificar inserção: {str(e)}")

def create_schema_if_not_exists(credentials: dict, database_name: str, schema_name: str):
    """
    Cria schema no Aurora se não existir
    """
    try:
        logger.info(f"🔍 Verificando se schema {schema_name} existe...")
        
        import psycopg2
        
        # Conectar ao PostgreSQL
        conn = psycopg2.connect(
            host=credentials['host'],
            port=credentials['port'],
            database=database_name,
            user=credentials['username'],
            password=credentials['password']
        )
        
        cursor = conn.cursor()
        
        # Verificar se schema existe
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.schemata 
                WHERE schema_name = %s
            );
        """, (schema_name,))
        
        schema_exists = cursor.fetchone()[0]
        
        if not schema_exists:
            logger.info(f"📋 Criando schema {schema_name}...")
            
            # Criar schema
            create_schema_sql = f"CREATE SCHEMA IF NOT EXISTS {schema_name};"
            cursor.execute(create_schema_sql)
            conn.commit()
            
            logger.info(f"✅ Schema {schema_name} criado com sucesso")
        else:
            logger.info(f"✅ Schema {schema_name} já existe")
        
        cursor.close()
        conn.close()
        
        return schema_exists
        
    except Exception as e:
        logger.error(f"❌ Erro ao verificar/criar schema: {str(e)}")
        # Não falhar o job se não conseguir criar o schema
        logger.warning("⚠️ Continuando sem criar schema. Certifique-se que ele existe.")
        return False

def create_table_if_not_exists(credentials: dict, database_name: str, schema_name: str, table_name: str, df: DataFrame):
    """
    Cria tabela no Aurora se não existir (baseado no schema do DataFrame)
    """
    try:
        logger.info(f"🔍 Verificando se tabela {schema_name}.{table_name} existe...")
        
        import psycopg2
        
        # Conectar ao PostgreSQL
        conn = psycopg2.connect(
            host=credentials['host'],
            port=credentials['port'],
            database=database_name,
            user=credentials['username'],
            password=credentials['password']
        )
        
        cursor = conn.cursor()
        
        # Verificar se tabela existe no schema específico
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = %s 
                AND table_name = %s
            );
        """, (schema_name, table_name))
        
        table_exists = cursor.fetchone()[0]
        
        if not table_exists:
            logger.info(f"📋 Criando tabela {schema_name}.{table_name}...")
            
            # Gerar DDL baseado no schema do DataFrame
            columns_ddl = []
            
            # Garantir que ID seja a primeira coluna e seja PRIMARY KEY
            id_added = False
            for field in df.schema.fields:
                col_name = field.name.lower()
                col_type = field.dataType
                
                # Mapear tipos Spark para PostgreSQL
                if isinstance(col_type, StringType):
                    pg_type = "TEXT"
                elif isinstance(col_type, IntegerType):
                    pg_type = "INTEGER"
                elif isinstance(col_type, LongType):
                    pg_type = "BIGINT"
                elif isinstance(col_type, DoubleType):
                    pg_type = "DOUBLE PRECISION"
                elif isinstance(col_type, BooleanType):
                    pg_type = "BOOLEAN"
                elif isinstance(col_type, TimestampType):
                    pg_type = "TIMESTAMP"
                elif isinstance(col_type, DateType):
                    pg_type = "DATE"
                else:
                    pg_type = "TEXT"  # Default
                
                # Se for coluna ID, adicionar como PRIMARY KEY
                if col_name == 'id':
                    columns_ddl.insert(0, f"{col_name} {pg_type} PRIMARY KEY")
                    id_added = True
                else:
                    columns_ddl.append(f"{col_name} {pg_type}")
            
            # Se não encontrou coluna ID, adicionar uma
            if not id_added:
                columns_ddl.insert(0, "id BIGINT PRIMARY KEY")
                logger.info("⚠️ Adicionando coluna ID como PRIMARY KEY na tabela")
            
            # Criar tabela no schema específico
            create_table_sql = f"""
                CREATE TABLE {schema_name}.{table_name} (
                    {', '.join(columns_ddl)}
                );
            """
            
            logger.info(f"📝 DDL da tabela: {create_table_sql}")
            cursor.execute(create_table_sql)
            conn.commit()
            
            logger.info(f"✅ Tabela {schema_name}.{table_name} criada com sucesso")
        else:
            logger.info(f"✅ Tabela {schema_name}.{table_name} já existe")
            
            # Verificar se precisa limpar dados existentes
            cursor.execute(f"SELECT COUNT(*) FROM {schema_name}.{table_name};")
            existing_count = cursor.fetchone()[0]
            logger.info(f"📊 Registros existentes na tabela: {existing_count:,}")
        
        cursor.close()
        conn.close()
        
        return table_exists
        
    except Exception as e:
        logger.error(f"❌ Erro ao verificar/criar tabela: {str(e)}")
        # Não falhar o job se não conseguir criar a tabela
        logger.warning("⚠️ Continuando sem criar tabela. Certifique-se que ela existe.")
        return False

# =============================================================================
# EXECUÇÃO PRINCIPAL
# =============================================================================

def main():
    """
    Função principal do ETL
    """
    try:
        logger.info("🚀 Iniciando processo ETL S3 → Aurora PostgreSQL")
        
        # 1. Recuperar credenciais do Secrets Manager
        logger.info("1️⃣ Recuperando credenciais...")
        credentials = get_secret_value(args['SECRET_NAME'])
        
        # 2. Ler dados do S3
        logger.info("2️⃣ Lendo dados do S3...")
        df_raw = read_csv_from_s3(args['S3_INPUT_PATH'])
        
        # 3. Aplicar transformações
        logger.info("3️⃣ Aplicando transformações...")
        df_transformed = transform_data(df_raw)
        
        # 4. Criar schema se não existir
        logger.info("4️⃣ Verificando/criando schema...")
        create_schema_if_not_exists(credentials, args['DATABASE_NAME'], args['SCHEMA_NAME'])
        
        # 5. Criar tabela se não existir
        logger.info("5️⃣ Verificando/criando tabela...")
        table_exists = create_table_if_not_exists(credentials, args['DATABASE_NAME'], args['SCHEMA_NAME'], args['TARGET_TABLE'], df_transformed)
        
        # 6. Escrever dados no Aurora
        logger.info("6️⃣ Escrevendo dados no Aurora...")
        write_to_aurora(df_transformed, args['CONNECTION_NAME'], args['DATABASE_NAME'], args['SCHEMA_NAME'], args['TARGET_TABLE'], credentials, table_exists)
        
        logger.info("🎉 ETL concluído com sucesso!")
        
        # Estatísticas finais
        logger.info("📊 ESTATÍSTICAS FINAIS:")
        logger.info(f"   - Arquivo S3: {args['S3_INPUT_PATH']}")
        logger.info(f"   - Schema destino: {args['SCHEMA_NAME']}")
        logger.info(f"   - Tabela destino: {args['DATABASE_NAME']}.{args['SCHEMA_NAME']}.{args['TARGET_TABLE']}")
        logger.info(f"   - Registros processados: {df_transformed.count():,}")
        
    except Exception as e:
        logger.error(f"💥 ERRO CRÍTICO no ETL: {str(e)}")
        import traceback
        logger.error(f"Stack trace: {traceback.format_exc()}")
        raise e

# =============================================================================
# EXECUÇÃO
# =============================================================================

if _name_ == "_main_":
    try:
        main()
    finally:
        # Finalizar job
        job.commit()
        logger.info("✅ Job finalizado")