import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
import re

# Inicialização
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_BUCKET', 'DATABASE_NAME', 'CONNECTION_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Configurações
s3_bucket = args['S3_BUCKET']
database_name = args['DATABASE_NAME']
connection_name = args['CONNECTION_NAME']

def clean_column_names(df):
    """Limpa nomes de colunas removendo caracteres especiais"""
    for col_name in df.columns:
        clean_name = re.sub(r'[^\w]', '_', col_name.lower())
        clean_name = re.sub(r'_+', '_', clean_name).strip('_')
        df = df.withColumnRenamed(col_name, clean_name)
    return df

def process_reclamacoes_data():
    """Processa dados de reclamações"""
    print("Processando dados de reclamações...")
    
    # Lista de arquivos de reclamações
    reclamacoes_files = [
        "2021_tri_01.csv", "2021_tri_02.csv", "2021_tri_03.csv", "2021_tri_04.csv",
        "2022_tri_01.csv", "2022_tri_03.csv", "2022_tri_04.csv"
    ]
    
    all_reclamacoes = []
    
    for file_name in reclamacoes_files:
        try:
            # Lê arquivo CSV do S3
            df = spark.read.option("header", "true") \
                          .option("delimiter", ";") \
                          .option("encoding", "ISO-8859-1") \
                          .csv(f"s3://{s3_bucket}/raw-data/reclamacoes/{file_name}")
            
            # Adiciona coluna com nome do arquivo
            df = df.withColumn("arquivo_origem", lit(file_name))
            
            # Limpa nomes das colunas
            df = clean_column_names(df)
            
            # Converte tipos de dados
            df = df.withColumn("ano", col("ano").cast(IntegerType())) \
                   .withColumn("indice", regexp_replace(col("indice"), ",", ".").cast(DecimalType(10,2))) \
                   .withColumn("quantidade_de_reclamacoes_reguladas_procedentes", 
                              col("quantidade_de_reclamacoes_reguladas_procedentes").cast(IntegerType())) \
                   .withColumn("quantidade_de_reclamacoes_reguladas_outras", 
                              col("quantidade_de_reclamacoes_reguladas_outras").cast(IntegerType())) \
                   .withColumn("quantidade_de_reclamacoes_nao_reguladas", 
                              col("quantidade_de_reclamacoes_nao_reguladas").cast(IntegerType())) \
                   .withColumn("quantidade_total_de_reclamacoes", 
                              col("quantidade_total_de_reclamacoes").cast(IntegerType())) \
                   .withColumn("quantidade_total_de_clientes_ccs_e_scr", 
                              col("quantidade_total_de_clientes_ccs_e_scr").cast(LongType())) \
                   .withColumn("quantidade_de_clientes_ccs", 
                              col("quantidade_de_clientes_ccs").cast(LongType())) \
                   .withColumn("quantidade_de_clientes_scr", 
                              col("quantidade_de_clientes_scr").cast(LongType()))
            
            all_reclamacoes.append(df)
            print(f"Processado: {file_name} - {df.count()} registros")
            
        except Exception as e:
            print(f"Erro ao processar {file_name}: {str(e)}")
    
    # Une todos os DataFrames
    if all_reclamacoes:
        reclamacoes_df = all_reclamacoes[0]
        for df in all_reclamacoes[1:]:
            reclamacoes_df = reclamacoes_df.union(df)
        
        # Remove registros com dados vazios importantes
        reclamacoes_df = reclamacoes_df.filter(col("instituicao_financeira").isNotNull() & 
                                              (col("instituicao_financeira") != ""))
        
        return reclamacoes_df
    
    return None

def process_bancos_data():
    """Processa dados de bancos"""
    print("Processando dados de bancos...")
    
    try:
        # Lê arquivo TSV do S3
        df = spark.read.option("header", "true") \
                      .option("delimiter", "\t") \
                      .option("encoding", "UTF-8") \
                      .csv(f"s3://{s3_bucket}/raw-data/bancos/EnquadramentoInicia_v2.tsv")
        
        # Limpa nomes das colunas
        df = clean_column_names(df)
        
        # Remove registros vazios
        df = df.filter(col("nome").isNotNull() & (col("nome") != ""))
        
        print(f"Bancos processados: {df.count()} registros")
        return df
        
    except Exception as e:
        print(f"Erro ao processar dados de bancos: {str(e)}")
        return None

def process_empregados_data():
    """Processa dados de empregados"""
    print("Processando dados de empregados...")
    
    try:
        # Lê arquivo CSV do S3
        df = spark.read.option("header", "true") \
                      .option("delimiter", "|") \
                      .option("encoding", "UTF-8") \
                      .csv(f"s3://{s3_bucket}/raw-data/empregados/glassdoor_consolidado_join_match_v2.csv")
        
        # Limpa nomes das colunas
        df = clean_column_names(df)
        
        # Converte tipos de dados
        numeric_columns = ["reviews_count", "culture_count", "salaries_count", "benefits_count",
                          "employer_founded", "geral", "cultura_e_valores", "diversidade_e_inclusao",
                          "qualidade_de_vida", "alta_lideranca", "remuneracao_e_beneficios",
                          "oportunidades_de_carreira", "recomendam_para_outras_pessoas",
                          "perspectiva_positiva_da_empresa", "match_percent"]
        
        for col_name in numeric_columns:
            if col_name in df.columns:
                df = df.withColumn(col_name, regexp_replace(col(col_name), ",", "."))
                if col_name in ["reviews_count", "culture_count", "salaries_count", "benefits_count", "match_percent"]:
                    df = df.withColumn(col_name, col(col_name).cast(IntegerType()))
                else:
                    df = df.withColumn(col_name, col(col_name).cast(DecimalType(10,2)))
        
        # Remove registros vazios
        df = df.filter(col("nome").isNotNull() & (col("nome") != ""))
        
        print(f"Empregados processados: {df.count()} registros")
        return df
        
    except Exception as e:
        print(f"Erro ao processar dados de empregados: {str(e)}")
        return None

def create_consolidated_data(reclamacoes_df, bancos_df, empregados_df):
    """Cria tabela consolidada"""
    print("Criando dados consolidados...")
    
    try:
        # Agrega dados de reclamações por CNPJ e ano
        reclamacoes_agg = reclamacoes_df.groupBy("cnpj_if", "ano") \
            .agg(
                sum("quantidade_total_de_reclamacoes").alias("total_reclamacoes"),
                sum("quantidade_total_de_clientes_ccs_e_scr").alias("total_clientes"),
                avg("indice").alias("indice_reclamacoes"),
                first("instituicao_financeira").alias("nome_instituicao")
            ) \
            .filter(col("cnpj_if").isNotNull() & (col("cnpj_if") != ""))
        
        # Join com dados de bancos para obter segmento
        consolidated = reclamacoes_agg.join(
            bancos_df.select("cnpj", "segmento"),
            reclamacoes_agg.cnpj_if == bancos_df.cnpj,
            "left"
        )
        
        # Join com dados de empregados para obter avaliações
        consolidated = consolidated.join(
            empregados_df.select("nome", "geral", "cultura_e_valores", 
                                "remuneracao_e_beneficios", "recomendam_para_outras_pessoas"),
            upper(trim(consolidated.nome_instituicao)).contains(upper(trim(empregados_df.nome))),
            "left"
        )
        
        # Seleciona e renomeia colunas finais
        consolidated = consolidated.select(
            col("cnpj_if").alias("cnpj"),
            col("nome_instituicao"),
            col("segmento"),
            col("ano"),
            col("total_reclamacoes"),
            col("total_clientes"),
            col("indice_reclamacoes"),
            col("geral").alias("avaliacao_geral"),
            col("cultura_e_valores").alias("cultura_valores"),
            col("remuneracao_e_beneficios"),
            col("recomendam_para_outras_pessoas").alias("recomendam_empresa")
        )
        
        print(f"Dados consolidados: {consolidated.count()} registros")
        return consolidated
        
    except Exception as e:
        print(f"Erro ao criar dados consolidados: {str(e)}")
        return None

def write_to_aurora(df, table_name):
    """Escreve DataFrame no Aurora Serverless"""
    try:
        # Converte para DynamicFrame
        dynamic_frame = DynamicFrame.fromDF(df, glueContext, table_name)
        
        # Escreve no Aurora Serverless via conexão
        glueContext.write_dynamic_frame.from_jdbc_conf(
            frame=dynamic_frame,
            catalog_connection=connection_name,
            connection_options={
                "dbtable": table_name,
                "database": database_name
            },
            transformation_ctx=f"write_{table_name}"
        )
        
        print(f"Dados escritos na tabela {table_name} no Aurora Serverless")
        
    except Exception as e:
        print(f"Erro ao escrever na tabela {table_name}: {str(e)}")

# Execução principal
try:
    # Processa cada fonte de dados
    reclamacoes_df = process_reclamacoes_data()
    bancos_df = process_bancos_data()
    empregados_df = process_empregados_data()
    
    # Escreve dados nas tabelas individuais
    if reclamacoes_df:
        write_to_aurora(reclamacoes_df, "reclamacoes")
    
    if bancos_df:
        write_to_aurora(bancos_df, "bancos_enquadramento")
    
    if empregados_df:
        write_to_aurora(empregados_df, "empregados_glassdoor")
    
    # Cria e escreve dados consolidados
    if reclamacoes_df and bancos_df and empregados_df:
        consolidated_df = create_consolidated_data(reclamacoes_df, bancos_df, empregados_df)
        if consolidated_df:
            write_to_aurora(consolidated_df, "dados_consolidados")
    
    print("ETL concluído com sucesso no Aurora Serverless!")
    
except Exception as e:
    print(f"Erro no processo ETL: {str(e)}")
    raise e

finally:
    job.commit()
