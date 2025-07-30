from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, upper, trim, sum, avg, first

# Função para leitura de tabelas do Aurora
def read_from_aurora(table_name):
    """Lê tabela do Aurora Serverless usando GlueContext"""
    try:
        dyf = glueContext.create_dynamic_frame.from_options(
            connection_type="jdbc",
            connection_options={
                "connectionName": connection_name,
                "dbtable": table_name,
                "database": database_name
            }
        )
        print(f"Tabela {table_name} lida com sucesso do Aurora Serverless")
        return dyf.toDF()
    except Exception as e:
        print(f"Erro ao ler a tabela {table_name}: {str(e)}")
        return None

# --- Leitura das três tabelas criadas ---
reclamacoes_df = read_from_aurora("reclamacoes")
bancos_df = read_from_aurora("bancos_enquadramento")
empregados_df = read_from_aurora("empregados_glassdoor")

# --- Apenas segue se todas as tabelas foram lidas ---
if reclamacoes_df and bancos_df and empregados_df:
    print("Realizando joins entre as três tabelas...")

    # Agregação de reclamações por CNPJ e ano
    reclamacoes_agg = reclamacoes_df.groupBy("cnpj_if", "ano").agg(
        sum("quantidade_total_de_reclamacoes").alias("total_reclamacoes"),
        sum("quantidade_total_de_clientes_ccs_e_scr").alias("total_clientes"),
        avg("indice").alias("indice_reclamacoes"),
        first("instituicao_financeira").alias("nome_instituicao")
    )

    # Join com dados de bancos
    df_join1 = reclamacoes_agg.join(
        bancos_df.select("cnpj", "segmento"),
        reclamacoes_agg.cnpj_if == bancos_df.cnpj,
        "left"
    )

    # Join com dados de empregados (match por nome)
    df_join2 = df_join1.join(
        empregados_df.select("nome", "geral", "cultura_e_valores",
                             "remuneracao_e_beneficios", "recomendam_para_outras_pessoas"),
        upper(trim(df_join1.nome_instituicao)).contains(upper(trim(empregados_df.nome))),
        "left"
    )

    # Seleção das colunas finais
    consolidated_df = df_join2.select(
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

    # --- Escreve resultado consolidado no Aurora ---
    write_to_aurora(consolidated_df, "dados_consolidados")
    print("Tabela dados_consolidados criada/atualizada com sucesso!")

else:
    print("Falha na leitura de uma ou mais tabelas, não foi possível consolidar os dados.")
