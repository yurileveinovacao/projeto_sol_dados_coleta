# SOL Engrenagens — Pipeline de Coleta de Dados Bling

Pipeline ETL que extrai dados da API Bling v3 (ERP) e persiste em PostgreSQL.
Coleta NF-e de saída (cabeçalho, itens e pagamentos), contatos e produtos diariamente de forma automatizada no Google Cloud.

---

## Sumario

- [Arquitetura](#arquitetura)
- [Infraestrutura de Producao (GCP)](#infraestrutura-de-producao-gcp)
- [Fluxo OAuth2 e Token Refresh](#fluxo-oauth2-e-token-refresh)
- [Pipeline ETL](#pipeline-etl)
- [Modelo de Dados](#modelo-de-dados)
- [API — Endpoints](#api--endpoints)
- [Desenvolvimento Local](#desenvolvimento-local)
- [Primeira Autorizacao OAuth](#primeira-autorizacao-oauth)
- [Carga Historica](#carga-historica)
- [Variaveis de Ambiente](#variaveis-de-ambiente)
- [CI/CD](#cicd)
- [Estrutura de Pastas](#estrutura-de-pastas)
- [Dependencias](#dependencias)

---

## Arquitetura

```
Cloud Scheduler                Cloud Run                       Cloud SQL
(cron diario 06:00 BRT)       (FastAPI + Pipeline ETL)         (PostgreSQL 16)
         |                              |                            |
         | POST /run (OIDC auth)        |                            |
         +---------------------------->  |                            |
                                        | 1. Refresh OAuth token     |
                                        |--------------------------> |
                                        |                            |
                                        | 2. GET /nfe (listagem)     |
                                        |------> API Bling v3        |
                                        |                            |
                                        | 3. GET /nfe/{id} (detalhe) |
                                        |------> API Bling v3        |
                                        |                            |
                                        | 4. Upsert NF-e + itens     |
                                        |--------------------------> |
                                        |                            |
                                        | 5. GET /contatos/{id}      |
                                        |------> API Bling v3        |
                                        |                            |
                                        | 6. Upsert contatos         |
                                        |--------------------------> |
                                        |                            |
                                        | 7. GET /produtos?codigo=X  |
                                        |------> API Bling v3        |
                                        |                            |
                                        | 8. Upsert produtos         |
                                        |--------------------------> |
```

**Resumo do fluxo:**

1. O **Cloud Scheduler** dispara `POST /run` diariamente as 06:00 (America/Sao_Paulo)
2. O servico no **Cloud Run** executa o pipeline completo
3. O pipeline renova o token OAuth, extrai dados da API Bling e persiste no **Cloud SQL**
4. Cada execucao e registrada na tabela `etl_controle` para auditoria

---

## Infraestrutura de Producao (GCP)

### Componentes

| Servico | Recurso | Detalhes |
|---------|---------|----------|
| **Cloud Run** | `sol-bling-collector` | Regiao `us-central1`, 512Mi RAM, 1 vCPU, timeout 3600s |
| **Cloud SQL** | `sol-postgres` | PostgreSQL, instancia `sol-engrenagens:us-central1:sol-postgres` |
| **Cloud Scheduler** | `sol-bling-daily-run` | Cron `0 6 * * *` (06:00 BRT), timeout 900s |
| **Artifact Registry** | `sol-docker` | Repositorio de imagens Docker em `us-central1` |
| **Secret Manager** | 3 secrets | `bling-client-id`, `bling-client-secret`, `database-url` |

### Service Accounts

| Service Account | Funcao |
|-----------------|--------|
| `sol-cloudrun-sa@sol-engrenagens.iam.gserviceaccount.com` | Executa o Cloud Run e invoca o servico (Cloud Run Invoker) |
| `github-actions-sa@sol-engrenagens.iam.gserviceaccount.com` | CI/CD — build e deploy via GitHub Actions (Workload Identity Federation) |
| `service-838831242440@gcp-sa-cloudscheduler.iam.gserviceaccount.com` | Cloud Scheduler Service Agent — gera tokens OIDC |

### Permissoes IAM configuradas

- `sol-cloudrun-sa` tem `roles/run.invoker` no servico Cloud Run (para o Scheduler poder invoca-lo)
- O Scheduler Service Agent tem `roles/iam.serviceAccountTokenCreator` na `sol-cloudrun-sa` (para gerar tokens OIDC)
- GitHub Actions usa Workload Identity Federation (sem chave de service account)

### Cloud Run — Configuracao

```
--no-allow-unauthenticated          # Requer autenticacao (OIDC)
--service-account sol-cloudrun-sa    # SA dedicada
--add-cloudsql-instances ...         # Conexao via Unix socket
--set-secrets BLING_CLIENT_ID=...    # Secrets injetados como env vars
--memory 512Mi --cpu 1
--timeout 3600                       # 60 min (maximo)
--max-instances 1 --min-instances 0
--concurrency 1                      # 1 request por vez
```

### Cloud Scheduler — Configuracao

```
schedule:  0 6 * * *                 # Diariamente as 06:00
timezone:  America/Sao_Paulo
method:    POST /run
auth:      OIDC token via sol-cloudrun-sa
deadline:  900s
```

### URLs do servico

| Ambiente | URL |
|----------|-----|
| Producao | `https://sol-bling-collector-838831242440.us-central1.run.app` |

---

## Fluxo OAuth2 e Token Refresh

O Bling v3 usa OAuth2 Authorization Code Flow. O token tem validade de **6 horas** e o refresh token e de **uso unico** (cada refresh gera um novo par access+refresh).

### Fluxo inicial (unica vez)

```
Navegador                   App (first_auth.py)              Bling OAuth
    |                              |                              |
    |  1. Abre URL de autorizacao  |                              |
    |----------------------------> |                              |
    |                              |  2. Redirect para Bling      |
    |                              |----------------------------> |
    |  3. Usuario autoriza         |                              |
    | <---------------------------------------------------------  |
    |  4. Redirect com ?code=XXX   |                              |
    | ----------------------------> |                              |
    |                              |  5. POST /oauth/token         |
    |                              |     grant_type=authorization_code
    |                              |     code=XXX                  |
    |                              |----------------------------> |
    |                              |  6. {access_token,            |
    |                              |      refresh_token,           |
    |                              |      expires_in: 21600}       |
    |                              | <--------------------------- |
    |                              |  7. Salva no banco (upsert)   |
    |                              |---> PostgreSQL                |
```

### Fluxo de refresh (inteligente)

```python
# src/auth/oauth.py — get_valid_access_token()

1. Busca token atual do banco (tabela oauth_tokens, id=1)
2. Verifica se faltam menos de 10 minutos para expirar
3. Se ainda valido: retorna o access_token atual (sem refresh)
4. Se expirando: POST /oauth/token com grant_type=refresh_token
5. Bling retorna novo access_token + novo refresh_token
6. CRITICO: salva imediatamente no banco (o refresh_token antigo ja foi invalidado)
7. Retorna o novo access_token para o pipeline
```

**Pontos importantes:**

- O refresh token do Bling e de **uso unico** — apos usado, e invalidado e um novo e retornado
- Por isso o `save_oauth_token` roda imediatamente apos o refresh, antes de qualquer outra operacao
- Se o refresh falhar com `invalid_grant`, significa que o token expirou e e necessario reautorizar via `first_auth.py`
- A autenticacao com a API Bling usa **Basic Auth** (base64 de `client_id:client_secret`) no header Authorization

---

## Pipeline ETL

### Execucao diaria (`POST /run`)

O pipeline diario e incremental: busca NF-e desde a ultima execucao bem-sucedida.

```
1. Determinar periodo
   - Se tem last_successful_run: usa data_referencia como data_inicio
   - Se nao tem: usa (hoje - EXTRACTION_DAYS_BACK)
   - data_fim: hoje

2. Criar registro ETL (status=running)

3. Refresh do token OAuth

4. Etapa NF-e:
   a. Listar NF-e do periodo (paginacao automatica, 100/pagina)
   b. Para cada NF-e: GET /nfe/{id} para obter detalhes
   c. Salvar cabecalho + itens + pagamentos via upsert
   d. Checkpoint (commit) a cada 50 NF-e

5. Etapa Contatos:
   a. Coletar IDs de contatos das NF-e detalhadas
   b. Filtrar apenas IDs que NAO existem no banco
   c. Para cada novo: GET /contatos/{id} e upsert

6. Etapa Produtos:
   a. Coletar codigos de produtos dos itens das NF-e
   b. Filtrar apenas codigos que NAO existem no banco
   c. Para cada novo: GET /produtos?codigo=X e upsert

7. Commit final + atualizar ETL (status=success)
```

### Execucao completa (`POST /run/full`)

Para carga historica. Recebe `data_inicio` e `data_fim` obrigatorios.

```
1. Quebrar o range em periodos mensais (evita timeout da API Bling)
   Ex: 2021-01-01 a 2021-12-31 = 12 periodos

2. Para cada periodo mensal:
   a. Executar Etapa NF-e (listar + detalhar + salvar)
   b. Commit parcial

3. Apos todos os periodos:
   a. Executar Etapa Contatos (sobre TODAS as NF-e coletadas)
   b. Executar Etapa Produtos (sobre TODAS as NF-e coletadas)

4. Commit final
```

### Tratamento de erros

- **Erro individual por NF-e/contato/produto**: cada item e processado em um SAVEPOINT (`db.begin_nested()`), isolando falhas sem afetar os demais no batch
- **Erro geral do pipeline**: rollback completo, registra erro na tabela `etl_controle`
- **Rate limit Bling (429)**: retry automatico com backoff exponencial (tenacity: 5 tentativas, 2-30s)
- **Delay entre requests**: 0.35s entre cada chamada para respeitar o rate limit da API
- **Itens duplicados**: quando uma NF-e tem o mesmo produto em linhas diferentes, os itens sao agrupados (quantidades e valores somados) antes do INSERT
- **Token refresh inteligente**: so renova o token quando faltam menos de 10 minutos para expirar, evitando refreshes desnecessarios e consumo de rate limit

### Rate limit e retry

O cliente HTTP (`BlingClient`) implementa:

```python
# Delay fixo entre requests
API_RATE_LIMIT_DELAY = 0.35  # segundos

# Retry automatico para 429
@retry(
    retry=retry_if_exception_type(BlingRateLimitError),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=30),
)
```

---

## Modelo de Dados

### Diagrama ER

```
oauth_tokens (1 registro)
+----+---------------+----------------+------------+
| id | access_token  | refresh_token  | expires_at |
+----+---------------+----------------+------------+

nfe_cabecalho
+----+--------+--------------+----------+-------------+----------------+
| id | numero | data_emissao | situacao | contato_id  | total_nota     |
+----+--------+--------------+----------+-------------+----------------+
  |
  |--- 1:N ---> nfe_itens
  |             +----+--------+-----------------+------+--------+------+
  |             | id | nfe_id | codigo_produto  | qtd  | vlr_un | vlr_total |
  |             +----+--------+-----------------+------+--------+------+
  |             UNIQUE(nfe_id, codigo_produto)
  |
  |--- 1:N ---> nfe_pagamentos
                +----+--------+-----------------+-------+
                | id | nfe_id | tipo_pagamento  | valor |
                +----+--------+-----------------+-------+

contatos
+----+------+-----------+-------+-------------+--------+----+
| id | nome | documento | email | tipo_pessoa | municipio | uf |
+----+------+-----------+-------+-------------+--------+----+

produtos
+----+--------+------+-------------+-------------+--------------+
| id | codigo | nome | preco_venda | preco_custo | categoria_id |
+----+--------+------+-------------+-------------+--------------+
UNIQUE(codigo)

etl_controle
+----+--------+-----+---------+-----------------+------+----------+----------+
| id | inicio | fim | status  | data_referencia | nfes | contatos | produtos |
+----+--------+-----+---------+-----------------+------+----------+----------+
status: running | success | error
```

### Tabelas

| Tabela | PK | Registros* | Descricao |
|--------|----|-----------|-----------|
| `oauth_tokens` | `id=1` (fixo) | 1 | Token OAuth atual (sempre 1 registro, upsert) |
| `nfe_cabecalho` | `id` (Bling) | ~2.850 | Cabecalho das NF-e de saida |
| `nfe_itens` | `id` (serial) | ~5.848 | Itens de cada NF-e (FK → nfe_cabecalho) |
| `nfe_pagamentos` | `id` (serial) | ~2.850 | Formas de pagamento (FK → nfe_cabecalho) |
| `contatos` | `id` (Bling) | ~1.418 | Clientes/fornecedores referenciados nas NF-e |
| `produtos` | `id` (Bling) | ~1.039 | Produtos referenciados nos itens das NF-e |
| `etl_controle` | `id` (serial) | variavel | Log de cada execucao do pipeline |

*Numeros referentes a carga historica 2021-2026 (fev/2026).*

### Notas sobre a API Bling

- A API retorna apenas NF-e autorizadas (`situacao=6`) na listagem padrao com `tipo=1`
- NF-e canceladas (`situacao=2`) nao sao retornadas — precisam ser consultadas com o filtro `situacao=2`
- O limite maximo por pagina e 100 registros (a API rejeita valores maiores)
- A API pode retornar mais de 100 itens por pagina e duplicatas entre paginas (bug do Bling) — o pipeline trata via upsert

### Estrategia de persistencia

- **NF-e cabecalho**: `INSERT ... ON CONFLICT (id) DO UPDATE` — atualiza todos os campos
- **NF-e itens**: `DELETE` dos itens existentes + `INSERT` (replace completo). Itens com mesmo `codigo_produto` sao agrupados antes do INSERT
- **NF-e pagamentos**: `DELETE` + `INSERT` (replace completo)
- **Contatos e Produtos**: `INSERT ... ON CONFLICT DO UPDATE` (upsert por ID)
- **OAuth token**: `INSERT ... ON CONFLICT (id=1) DO UPDATE` (sempre 1 registro)

---

## API — Endpoints

| Metodo | Rota | Autenticacao | Descricao |
|--------|------|-------------|-----------|
| `GET` | `/health` | Nao | Health check — retorna `{"status": "ok"}` |
| `GET` | `/status` | Nao | Status da ultima execucao e token OAuth |
| `POST` | `/run` | Nao* | Executa pipeline incremental |
| `POST` | `/run/full` | Nao* | Executa carga completa (requer `data_inicio`) |
| `GET` | `/auth/start` | Nao | Retorna URL para autorizacao OAuth |
| `GET` | `/auth/callback` | Nao | Callback OAuth (recebe `code` e troca por tokens) |

*Em producao, o Cloud Run requer autenticacao OIDC via IAM. "Nao" refere-se a autenticacao interna da aplicacao.*

### Parametros de `/run`

| Parametro | Tipo | Obrigatorio | Descricao |
|-----------|------|-------------|-----------|
| `data_inicio` | string (YYYY-MM-DD) | Nao | Inicio do periodo. Se omitido, usa data da ultima execucao |
| `data_fim` | string (YYYY-MM-DD) | Nao | Fim do periodo. Se omitido, usa hoje |

### Parametros de `/run/full`

| Parametro | Tipo | Obrigatorio | Descricao |
|-----------|------|-------------|-----------|
| `data_inicio` | string (YYYY-MM-DD) | Sim | Inicio do periodo |
| `data_fim` | string (YYYY-MM-DD) | Nao | Fim do periodo. Se omitido, usa hoje |

### Exemplos

```bash
# Health check
curl https://<SERVICE_URL>/health

# Status
curl -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
  https://<SERVICE_URL>/status

# Execucao diaria
curl -X POST -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
  "https://<SERVICE_URL>/run"

# Execucao com periodo especifico
curl -X POST -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
  "https://<SERVICE_URL>/run?data_inicio=2025-01-01&data_fim=2025-01-31"

# Carga historica completa
curl -X POST -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
  "https://<SERVICE_URL>/run/full?data_inicio=2021-01-01&data_fim=2026-02-20"
```

---

## Desenvolvimento Local

### Pre-requisitos

- Docker e Docker Compose
- Python 3.12+
- Credenciais do app Bling (Client ID e Client Secret)

### Subir o ambiente

```bash
# 1. Copiar variaveis de ambiente
cp .env.example .env
# Preencher BLING_CLIENT_ID e BLING_CLIENT_SECRET

# 2. Subir containers (app + PostgreSQL)
docker compose up --build

# 3. Verificar saude
curl http://localhost:8080/health

# 4. Ver status
curl http://localhost:8080/status
```

O `docker-compose.yml` sobe:
- **app**: FastAPI na porta 8080
- **db**: PostgreSQL 16 na porta 5432 (usuario: postgres, senha: postgres, banco: sol_dados)

O script `migrations/001_initial.sql` e executado automaticamente na inicializacao do PostgreSQL.

### Executar pipeline localmente

```bash
# Execucao incremental
curl -X POST http://localhost:8080/run

# Carga completa desde 2021
curl -X POST "http://localhost:8080/run/full?data_inicio=2021-01-01"
```

---

## Primeira Autorizacao OAuth

Necessario rodar **uma unica vez** para obter os tokens iniciais do Bling. O script abre o navegador, o usuario autoriza no Bling, e os tokens sao salvos no banco.

```bash
# Instalar dependencias localmente (fora do Docker)
pip install -r requirements.txt

# Rodar script de autorizacao
python scripts/first_auth.py
```

### O que o script faz

1. Cria as tabelas no banco (se nao existirem)
2. Inicia um servidor HTTP local na porta 8000
3. Abre o navegador na URL de autorizacao do Bling
4. Aguarda o callback com o `authorization_code` (timeout: 5 min)
5. Troca o code por `access_token` + `refresh_token` via `POST /oauth/token`
6. Salva os tokens na tabela `oauth_tokens` (id=1)

Apos isso, o pipeline renova os tokens automaticamente a cada execucao.

### Quando reautorizar

- Se o refresh token expirar (30 dias sem execucao)
- Se o pipeline retornar erro `invalid_grant`
- Se as credenciais do app Bling forem revogadas

---

## Carga Historica

Para popular o banco com dados retroativos, usar o endpoint `/run/full`.

### Recomendacoes

- Usar `POST /run` (sem split mensal) por ano — o `/run/full` quebra em periodos mensais e a API do Bling pode omitir NF-e nas bordas dos meses
- O pipeline usa **upsert**, entao re-executar e seguro (nao duplica dados)
- Respeitar o rate limit do Bling — aguardar 3-5 minutos entre fatias se receber 429

### Exemplo de carga completa

```bash
SERVICE_URL="https://sol-bling-collector-838831242440.us-central1.run.app"
TOKEN=$(gcloud auth print-identity-token)

# Ano a ano (usar /run em vez de /run/full para evitar perda por split mensal)
for ANO in 2021 2022 2023 2024 2025; do
  echo "=== $ANO ==="
  curl -s -X POST -H "Authorization: Bearer $TOKEN" \
    "$SERVICE_URL/run?data_inicio=$ANO-01-01&data_fim=$ANO-12-31"
  echo ""
  sleep 300  # 5 min entre fatias (rate limit)
done

# 2026 ate hoje
curl -s -X POST -H "Authorization: Bearer $TOKEN" \
  "$SERVICE_URL/run?data_inicio=2026-01-01"
```

### Volumes da carga historica (referencia)

| Ano | Dashboard Bling | NF-e coletadas | Canceladas (nao coletadas) |
|-----|----------------|----------------|---------------------------|
| 2021 | 147 | 147 | 0 |
| 2022 | 186 | 178 | 8 |
| 2023 | 402 | 389 | 13 |
| 2024 | 820 | 792 | 28 |
| 2025 | 1.200 | 1.175 | 25 |
| 2026 (jan-fev) | 171 | 169 | 2 |
| **Total** | **2.926** | **2.850** | **76** |

*A API do Bling nao retorna NF-e canceladas (situacao=2) na listagem padrao. As NF-e "faltantes" em relacao ao dashboard sao exclusivamente canceladas.*

---

## Variaveis de Ambiente

| Variavel | Obrigatoria | Default | Descricao |
|----------|-------------|---------|-----------|
| `BLING_CLIENT_ID` | Sim | — | Client ID do app Bling |
| `BLING_CLIENT_SECRET` | Sim | — | Client Secret do app Bling |
| `DATABASE_URL` | Sim | — | Connection string PostgreSQL |
| `BLING_API_BASE_URL` | Nao | `https://api.bling.com.br/Api/v3` | Base URL da API Bling |
| `BLING_OAUTH_URL` | Nao | `https://api.bling.com.br/Api/v3/oauth/token` | Endpoint OAuth |
| `EXTRACTION_DAYS_BACK` | Nao | `1` | Dias retroativos quando nao ha execucao anterior |
| `API_RATE_LIMIT_DELAY` | Nao | `0.35` | Delay entre requests a API Bling (segundos) |
| `API_PAGE_SIZE` | Nao | `100` | Registros por pagina na listagem |
| `LOG_LEVEL` | Nao | `INFO` | Nivel de log (DEBUG, INFO, WARNING, ERROR) |

Em producao, `BLING_CLIENT_ID`, `BLING_CLIENT_SECRET` e `DATABASE_URL` sao injetados via **Secret Manager**.

---

## CI/CD

### GitHub Actions (`.github/workflows/deploy.yml`)

O deploy e automatico a cada push na branch `main`.

```
Push na main
    |
    v
GitHub Actions
    |
    |--> Auth GCP (Workload Identity Federation)
    |--> Build imagem Docker
    |--> Push para Artifact Registry (us-central1-docker.pkg.dev)
    |--> Deploy no Cloud Run
```

### Workload Identity Federation

O GitHub Actions autentica no GCP **sem chave de service account**, usando WIF:

- **Pool**: `github-pool`
- **Provider**: `github-provider`
- **Service Account**: `github-actions-sa@sol-engrenagens.iam.gserviceaccount.com`

### Comandos uteis

```bash
# Ver ultimos deploys
gh run list --limit 5

# Acompanhar deploy em andamento
gh run watch <RUN_ID>

# Ver logs de um deploy
gh run view <RUN_ID> --log
```

---

## Estrutura de Pastas

```
projeto_sol_dados_coleta/
├── src/
│   ├── __init__.py
│   ├── main.py                 # FastAPI app + endpoints
│   ├── config.py               # Settings via pydantic-settings
│   ├── auth/
│   │   ├── __init__.py
│   │   └── oauth.py            # OAuth2: exchange code, refresh token
│   ├── api/
│   │   ├── __init__.py
│   │   └── bling_client.py     # Cliente HTTP da API Bling v3 (com retry)
│   ├── db/
│   │   ├── __init__.py
│   │   ├── database.py         # SQLAlchemy engine, session, Base
│   │   ├── models.py           # 7 modelos: OAuthToken, NfeCabecalho, NfeItem,
│   │   │                       #   NfePagamento, Contato, Produto, EtlControle
│   │   └── repository.py       # Operacoes de banco (upserts, queries)
│   └── etl/
│       ├── __init__.py
│       └── pipeline.py         # Orquestrador: run() e run_full()
├── scripts/
│   ├── first_auth.py           # Autorizacao OAuth inicial (rodar 1x)
│   └── init_db.py              # Criacao manual de tabelas
├── migrations/
│   └── 001_initial.sql         # DDL PostgreSQL (7 tabelas + indices)
├── .github/workflows/
│   └── deploy.yml              # CI/CD: build + deploy no Cloud Run
├── Dockerfile                  # python:3.12-slim-bookworm + uvicorn
├── docker-compose.yml          # Dev local: app + PostgreSQL 16
├── requirements.txt            # Dependencias Python
├── .env.example                # Template de variaveis de ambiente
├── .dockerignore
└── .gitignore
```

---

## Dependencias

| Pacote | Versao | Funcao |
|--------|--------|--------|
| `fastapi` | 0.115.0 | Framework web (endpoints) |
| `uvicorn` | 0.30.0 | Servidor ASGI |
| `httpx` | 0.27.0 | Cliente HTTP (chamadas a API Bling) |
| `sqlalchemy` | 2.0.35 | ORM e acesso ao banco |
| `psycopg2-binary` | 2.9.9 | Driver PostgreSQL |
| `pydantic` | 2.9.0 | Validacao de dados |
| `pydantic-settings` | 2.5.0 | Configuracao via env vars |
| `tenacity` | 9.0.0 | Retry com backoff exponencial |
| `python-dotenv` | 1.0.1 | Carrega `.env` no dev local |
