-- Este arquivo é mantido apenas para referência
-- As tabelas são criadas automaticamente via Terraform usando null_resource

-- Tabela para dados de Reclamações
CREATE TABLE IF NOT EXISTS reclamacoes (
    id SERIAL PRIMARY KEY,
    ano INTEGER,
    trimestre VARCHAR(10),
    categoria VARCHAR(100),
    tipo VARCHAR(100),
    cnpj_if VARCHAR(20),
    instituicao_financeira VARCHAR(200),
    indice DECIMAL(10,2),
    qtd_reclamacoes_reguladas_procedentes INTEGER,
    qtd_reclamacoes_reguladas_outras INTEGER,
    qtd_reclamacoes_nao_reguladas INTEGER,
    qtd_total_reclamacoes INTEGER,
    qtd_total_clientes_ccs_scr BIGINT,
    qtd_clientes_ccs BIGINT,
    qtd_clientes_scr BIGINT,
    arquivo_origem VARCHAR(100),
    data_processamento TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabela para dados de Bancos (Enquadramento)
CREATE TABLE IF NOT EXISTS bancos_enquadramento (
    id SERIAL PRIMARY KEY,
    segmento VARCHAR(10),
    cnpj VARCHAR(20),
    nome VARCHAR(200),
    data_processamento TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabela para dados de Empregados (Glassdoor)
CREATE TABLE IF NOT EXISTS empregados_glassdoor (
    id SERIAL PRIMARY KEY,
    employer_name VARCHAR(200),
    reviews_count INTEGER,
    culture_count INTEGER,
    salaries_count INTEGER,
    benefits_count INTEGER,
    employer_website VARCHAR(500),
    employer_headquarters VARCHAR(200),
    employer_founded DECIMAL(10,2),
    employer_industry VARCHAR(500),
    employer_revenue VARCHAR(200),
    url VARCHAR(1000),
    geral DECIMAL(3,1),
    cultura_valores DECIMAL(3,1),
    diversidade_inclusao DECIMAL(3,1),
    qualidade_vida DECIMAL(3,1),
    alta_lideranca DECIMAL(3,1),
    remuneracao_beneficios DECIMAL(3,1),
    oportunidades_carreira DECIMAL(3,1),
    recomendam_outras_pessoas DECIMAL(5,1),
    perspectiva_positiva_empresa DECIMAL(5,1),
    segmento VARCHAR(10),
    nome VARCHAR(200),
    match_percent INTEGER,
    data_processamento TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabela consolidada final
CREATE TABLE IF NOT EXISTS dados_consolidados (
    id SERIAL PRIMARY KEY,
    cnpj VARCHAR(20),
    nome_instituicao VARCHAR(200),
    segmento VARCHAR(10),
    ano INTEGER,
    total_reclamacoes INTEGER,
    total_clientes BIGINT,
    indice_reclamacoes DECIMAL(10,2),
    avaliacao_geral DECIMAL(3,1),
    cultura_valores DECIMAL(3,1),
    remuneracao_beneficios DECIMAL(3,1),
    recomendam_empresa DECIMAL(5,1),
    data_processamento TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(cnpj, ano)
);

-- Índices para melhor performance
CREATE INDEX IF NOT EXISTS idx_reclamacoes_cnpj ON reclamacoes(cnpj_if);
CREATE INDEX IF NOT EXISTS idx_reclamacoes_ano ON reclamacoes(ano);
CREATE INDEX IF NOT EXISTS idx_bancos_cnpj ON bancos_enquadramento(cnpj);
CREATE INDEX IF NOT EXISTS idx_empregados_nome ON empregados_glassdoor(nome);
CREATE INDEX IF NOT EXISTS idx_consolidados_cnpj_ano ON dados_consolidados(cnpj, ano);
