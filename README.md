# SOL Engrenagens — Coleta de Dados Bling

Pipeline de coleta de dados da API Bling v3 (ERP) para PostgreSQL.
Extrai NF-e de saída, contatos e produtos diariamente.

## Arquitetura

```
Cloud Scheduler (cron diário)
        │
        ▼ POST /run
Cloud Run (FastAPI + Pipeline ETL)
        │
        ▼ SQL
Cloud SQL (PostgreSQL)
```

O Cloud Scheduler dispara uma requisição `POST /run` diariamente.
O serviço no Cloud Run executa o pipeline completo:

1. Renova token OAuth (refresh_token)
2. Lista e detalha NF-e do período
3. Extrai contatos novos referenciados nas NF-e
4. Extrai produtos novos referenciados nos itens
5. Persiste tudo no PostgreSQL via upsert

## Rodar localmente

```bash
# 1. Copiar variáveis de ambiente
cp .env.example .env
# Preencher BLING_CLIENT_ID e BLING_CLIENT_SECRET

# 2. Subir containers
docker compose up --build

# 3. Verificar saúde
curl http://localhost:8080/health
```

## Primeira autorização OAuth

Necessário rodar **uma vez** para obter os tokens iniciais do Bling:

```bash
# Instalar dependências localmente
pip install -r requirements.txt

# Rodar script de autorização
python scripts/first_auth.py
```

O script abre o navegador para autorizar no Bling e salva os tokens no banco.
Após isso, o pipeline renova os tokens automaticamente via refresh_token.

## Variáveis de ambiente

| Variável | Obrigatória | Default | Descrição |
|---|---|---|---|
| `BLING_CLIENT_ID` | Sim | — | Client ID do app Bling |
| `BLING_CLIENT_SECRET` | Sim | — | Client Secret do app Bling |
| `DATABASE_URL` | Sim | — | Connection string PostgreSQL |
| `BLING_API_BASE_URL` | Não | `https://api.bling.com.br/Api/v3` | Base URL da API |
| `BLING_OAUTH_URL` | Não | `https://api.bling.com.br/Api/v3/oauth/token` | Endpoint OAuth |
| `EXTRACTION_DAYS_BACK` | Não | `1` | Dias retroativos na primeira execução |
| `API_RATE_LIMIT_DELAY` | Não | `0.35` | Delay entre requests (segundos) |
| `API_PAGE_SIZE` | Não | `100` | Registros por página |
| `LOG_LEVEL` | Não | `INFO` | Nível de log |

## Endpoints

| Método | Rota | Descrição |
|---|---|---|
| GET | `/health` | Health check |
| GET | `/status` | Status da última execução e token OAuth |
| POST | `/run` | Executa o pipeline (params: `data_inicio`, `data_fim`) |
| GET | `/auth/start` | Retorna URL de autorização OAuth |
| GET | `/auth/callback` | Callback OAuth (recebe `code`) |

## Estrutura de pastas

```
projeto_sol_dados_coleta/
├── src/
│   ├── main.py              # FastAPI app + endpoints
│   ├── config.py            # Settings (pydantic-settings)
│   ├── auth/
│   │   └── oauth.py         # OAuth2 token management
│   ├── api/
│   │   └── bling_client.py  # Cliente HTTP da API Bling v3
│   ├── db/
│   │   ├── database.py      # Engine, SessionLocal, Base
│   │   ├── models.py        # SQLAlchemy models (7 tabelas)
│   │   └── repository.py    # Operações de banco (upserts)
│   └── etl/
│       └── pipeline.py      # Orquestrador do pipeline
├── scripts/
│   ├── first_auth.py        # Autorização OAuth inicial
│   └── init_db.py           # Criação manual de tabelas
├── migrations/
│   └── 001_initial.sql      # DDL PostgreSQL
├── .github/workflows/
│   └── deploy.yml           # CI/CD para Cloud Run
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── .env.example
```
