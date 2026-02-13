-- ============================================================================
-- Migration 001: Estrutura inicial do banco de dados
-- Projeto Sol - Coleta de dados Bling v3
-- ============================================================================

-- 1. OAuth Tokens (sempre 1 registro)
CREATE TABLE IF NOT EXISTS oauth_tokens (
    id          INTEGER PRIMARY KEY DEFAULT 1,
    access_token  TEXT NOT NULL,
    refresh_token TEXT NOT NULL,
    token_type    VARCHAR(50) DEFAULT 'Bearer',
    expires_at    TIMESTAMP NOT NULL,
    updated_at    TIMESTAMP DEFAULT NOW()
);

-- 2. NF-e Cabeçalho
CREATE TABLE IF NOT EXISTS nfe_cabecalho (
    id                BIGINT PRIMARY KEY,
    numero            VARCHAR(50),
    data_emissao      TIMESTAMP,
    situacao          INTEGER,
    contato_id        BIGINT,
    contato_nome      VARCHAR(500),
    contato_documento VARCHAR(20),
    contato_municipio VARCHAR(200),
    contato_uf        VARCHAR(2),
    total_produtos    FLOAT DEFAULT 0,
    total_nota        FLOAT DEFAULT 0,
    total_descontos   FLOAT DEFAULT 0,
    extraido_em       TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_nfe_cabecalho_numero ON nfe_cabecalho (numero);
CREATE INDEX IF NOT EXISTS ix_nfe_cabecalho_data_emissao ON nfe_cabecalho (data_emissao);
CREATE INDEX IF NOT EXISTS ix_nfe_cabecalho_contato_id ON nfe_cabecalho (contato_id);
CREATE INDEX IF NOT EXISTS ix_nfe_cabecalho_contato_documento ON nfe_cabecalho (contato_documento);

-- 3. NF-e Itens (FK → nfe_cabecalho)
CREATE TABLE IF NOT EXISTS nfe_itens (
    id                SERIAL PRIMARY KEY,
    nfe_id            BIGINT NOT NULL REFERENCES nfe_cabecalho(id),
    codigo_produto    VARCHAR(100),
    descricao_produto VARCHAR(500),
    quantidade        FLOAT DEFAULT 0,
    valor_unitario    FLOAT DEFAULT 0,
    valor_total       FLOAT DEFAULT 0,
    valor_desconto    FLOAT DEFAULT 0,
    unidade_medida    VARCHAR(20),
    CONSTRAINT uq_nfe_item UNIQUE (nfe_id, codigo_produto)
);

CREATE INDEX IF NOT EXISTS ix_nfe_itens_nfe_id ON nfe_itens (nfe_id);
CREATE INDEX IF NOT EXISTS ix_nfe_itens_codigo_produto ON nfe_itens (codigo_produto);

-- 4. NF-e Pagamentos (FK → nfe_cabecalho)
CREATE TABLE IF NOT EXISTS nfe_pagamentos (
    id              SERIAL PRIMARY KEY,
    nfe_id          BIGINT NOT NULL REFERENCES nfe_cabecalho(id),
    tipo_pagamento  INTEGER,
    valor           FLOAT DEFAULT 0
);

COMMENT ON COLUMN nfe_pagamentos.tipo_pagamento IS '1=Dinheiro, 2=Cheque, 3=CC, 4=CD, 15=Boleto, 17=PIX';

CREATE INDEX IF NOT EXISTS ix_nfe_pagamentos_nfe_id ON nfe_pagamentos (nfe_id);

-- 5. Contatos
CREATE TABLE IF NOT EXISTS contatos (
    id          BIGINT PRIMARY KEY,
    nome        VARCHAR(500),
    documento   VARCHAR(20),
    email       VARCHAR(500),
    tipo_pessoa VARCHAR(1),
    municipio   VARCHAR(200),
    uf          VARCHAR(2),
    extraido_em TIMESTAMP DEFAULT NOW()
);

COMMENT ON COLUMN contatos.tipo_pessoa IS 'F=Física, J=Jurídica';

CREATE INDEX IF NOT EXISTS ix_contatos_documento ON contatos (documento);

-- 6. Produtos
CREATE TABLE IF NOT EXISTS produtos (
    id                   BIGINT PRIMARY KEY,
    codigo               VARCHAR(100) UNIQUE,
    nome                 VARCHAR(500),
    preco_venda          FLOAT DEFAULT 0,
    preco_custo          FLOAT DEFAULT 0,
    categoria_id         BIGINT,
    categoria_descricao  VARCHAR(300),
    extraido_em          TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_produtos_codigo ON produtos (codigo);

-- 7. ETL Controle
CREATE TABLE IF NOT EXISTS etl_controle (
    id                SERIAL PRIMARY KEY,
    inicio            TIMESTAMP NOT NULL,
    fim               TIMESTAMP,
    status            VARCHAR(20) DEFAULT 'running',
    data_referencia   DATE,
    nfes_processadas  INTEGER DEFAULT 0,
    contatos_novos    INTEGER DEFAULT 0,
    produtos_novos    INTEGER DEFAULT 0,
    erro_mensagem     TEXT,
    created_at        TIMESTAMP DEFAULT NOW()
);

COMMENT ON COLUMN etl_controle.status IS 'running, success, error';

CREATE INDEX IF NOT EXISTS ix_etl_controle_status ON etl_controle (status);
CREATE INDEX IF NOT EXISTS ix_etl_controle_data_ref ON etl_controle (data_referencia);
