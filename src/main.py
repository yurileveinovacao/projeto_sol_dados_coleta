import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import Depends, FastAPI, HTTPException, Query
from sqlalchemy.orm import Session

from src.auth.oauth import exchange_authorization_code
from src.config import get_settings
from src.db.database import Base, engine, get_db
from src.db.repository import get_last_successful_run, get_oauth_token
from src.etl.pipeline import Pipeline

settings = get_settings()

logging.basicConfig(
    level=settings.LOG_LEVEL,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Iniciando aplicação — criando tabelas...")
    Base.metadata.create_all(bind=engine)
    logger.info("Tabelas criadas. Aplicação pronta.")
    yield
    logger.info("Encerrando aplicação.")


app = FastAPI(
    title="SOL - Bling Data Collector",
    version="1.0.0",
    lifespan=lifespan,
)


# ── Health ───────────────────────────────────────────────────────────────────


@app.get("/health")
def health():
    return {
        "status": "ok",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


# ── Status ───────────────────────────────────────────────────────────────────


@app.get("/status")
def status(db: Session = Depends(get_db)):
    last_run = get_last_successful_run(db)
    token = get_oauth_token(db)

    return {
        "last_successful_run": {
            "date": last_run.data_referencia.isoformat() if last_run.data_referencia else None,
            "nfes": last_run.nfes_processadas,
            "contatos": last_run.contatos_novos,
            "produtos": last_run.produtos_novos,
        } if last_run else None,
        "oauth": {
            "has_token": token is not None,
            "expires_at": token.expires_at.isoformat() if token else None,
            "token_updated_at": token.updated_at.isoformat() if token else None,
        },
    }


# ── Run pipeline ─────────────────────────────────────────────────────────────


@app.post("/run")
def run_pipeline(
    data_inicio: str | None = Query(None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    data_fim: str | None = Query(None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    db: Session = Depends(get_db),
):
    pipeline = Pipeline(db)
    return pipeline.run(data_inicio=data_inicio, data_fim=data_fim)


@app.post("/run/full")
def run_full_pipeline(
    data_inicio: str = Query(..., pattern=r"^\d{4}-\d{2}-\d{2}$"),
    data_fim: str | None = Query(None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    db: Session = Depends(get_db),
):
    if not data_fim:
        data_fim = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    pipeline = Pipeline(db)
    return pipeline.run_full(data_inicio=data_inicio, data_fim=data_fim)


# ── Auth ─────────────────────────────────────────────────────────────────────


@app.get("/auth/callback")
def auth_callback(
    code: str = Query(...),
    state: str | None = Query(None),
    db: Session = Depends(get_db),
):
    try:
        data = exchange_authorization_code(db, code)
        return {
            "status": "success",
            "message": "Tokens salvos!",
            "expires_in": data.get("expires_in"),
        }
    except Exception as e:
        logger.error("Erro no callback OAuth: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/auth/start")
def auth_start():
    auth_url = (
        "https://api.bling.com.br/Api/v3/oauth/authorize"
        f"?response_type=code"
        f"&client_id={settings.BLING_CLIENT_ID}"
        f"&state=sol_dados"
    )
    return {
        "auth_url": auth_url,
        "instruction": "Acesse a URL para autorizar",
    }
