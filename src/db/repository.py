import logging
from datetime import date, datetime, timedelta, timezone

from sqlalchemy import select, delete
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from src.db.models import (
    Contato,
    EtlControle,
    NfeCabecalho,
    NfeItem,
    NfePagamento,
    OAuthToken,
    Produto,
)

logger = logging.getLogger(__name__)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


# ── OAuth ────────────────────────────────────────────────────────────────────


def get_oauth_token(db: Session) -> OAuthToken | None:
    return db.scalars(select(OAuthToken)).first()


def save_oauth_token(
    db: Session, access_token: str, refresh_token: str, expires_in: int
) -> None:
    expires_at = _utcnow() + timedelta(seconds=expires_in)
    stmt = pg_insert(OAuthToken).values(
        id=1,
        access_token=access_token,
        refresh_token=refresh_token,
        token_type="Bearer",
        expires_at=expires_at,
        updated_at=_utcnow(),
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=["id"],
        set_={
            "access_token": stmt.excluded.access_token,
            "refresh_token": stmt.excluded.refresh_token,
            "expires_at": stmt.excluded.expires_at,
            "updated_at": stmt.excluded.updated_at,
        },
    )
    db.execute(stmt)
    db.commit()
    logger.info("Token OAuth salvo/atualizado (expira em %d segundos)", expires_in)


# ── NF-e ─────────────────────────────────────────────────────────────────────


def upsert_nfe_cabecalho(db: Session, data: dict) -> int:
    stmt = pg_insert(NfeCabecalho).values(**data)
    stmt = stmt.on_conflict_do_update(
        index_elements=["id"],
        set_={k: stmt.excluded[k] for k in data if k != "id"},
    )
    result = db.execute(stmt)
    db.flush()
    nfe_id = data["id"]
    logger.debug("Upsert NF-e cabeçalho id=%d", nfe_id)
    return nfe_id


def upsert_nfe_itens(db: Session, nfe_id: int, itens: list[dict]) -> None:
    db.execute(delete(NfeItem).where(NfeItem.nfe_id == nfe_id))
    if itens:
        for item in itens:
            item["nfe_id"] = nfe_id
        stmt = pg_insert(NfeItem).values(itens)
        stmt = stmt.on_conflict_do_update(
            constraint="uq_nfe_item",
            set_={
                "descricao_produto": stmt.excluded.descricao_produto,
                "quantidade": NfeItem.quantidade + stmt.excluded.quantidade,
                "valor_unitario": stmt.excluded.valor_unitario,
                "valor_total": NfeItem.valor_total + stmt.excluded.valor_total,
                "valor_desconto": NfeItem.valor_desconto + stmt.excluded.valor_desconto,
                "unidade_medida": stmt.excluded.unidade_medida,
            },
        )
        db.execute(stmt)
    logger.debug("NF-e %d: %d itens substituídos", nfe_id, len(itens))


def upsert_nfe_pagamentos(db: Session, nfe_id: int, pagamentos: list[dict]) -> None:
    db.execute(delete(NfePagamento).where(NfePagamento.nfe_id == nfe_id))
    if pagamentos:
        for pag in pagamentos:
            pag["nfe_id"] = nfe_id
        db.execute(pg_insert(NfePagamento).values(pagamentos))
    logger.debug("NF-e %d: %d pagamentos substituídos", nfe_id, len(pagamentos))


# ── Contatos ─────────────────────────────────────────────────────────────────


def get_existing_contato_ids(db: Session) -> set[int]:
    rows = db.scalars(select(Contato.id)).all()
    return set(rows)


def upsert_contato(db: Session, data: dict) -> None:
    stmt = pg_insert(Contato).values(**data)
    stmt = stmt.on_conflict_do_update(
        index_elements=["id"],
        set_={k: stmt.excluded[k] for k in data if k != "id"},
    )
    db.execute(stmt)
    logger.debug("Upsert contato id=%d", data["id"])


# ── Produtos ─────────────────────────────────────────────────────────────────


def get_existing_produto_codigos(db: Session) -> set[str]:
    rows = db.scalars(select(Produto.codigo)).all()
    return {c for c in rows if c is not None}


def upsert_produto(db: Session, data: dict) -> None:
    stmt = pg_insert(Produto).values(**data)
    stmt = stmt.on_conflict_do_update(
        index_elements=["id"],
        set_={k: stmt.excluded[k] for k in data if k != "id"},
    )
    db.execute(stmt)
    logger.debug("Upsert produto id=%d", data["id"])


# ── ETL Controle ─────────────────────────────────────────────────────────────


def create_etl_run(db: Session, data_referencia: date) -> int:
    run = EtlControle(
        inicio=_utcnow(),
        status="running",
        data_referencia=data_referencia,
    )
    db.add(run)
    db.commit()
    db.refresh(run)
    logger.info("ETL run #%d criada (ref: %s)", run.id, data_referencia)
    return run.id


def finish_etl_run(
    db: Session,
    run_id: int,
    status: str,
    nfes: int = 0,
    contatos: int = 0,
    produtos: int = 0,
    erro: str | None = None,
) -> None:
    run = db.get(EtlControle, run_id)
    if not run:
        logger.error("ETL run #%d não encontrada", run_id)
        return
    run.fim = _utcnow()
    run.status = status
    run.nfes_processadas = nfes
    run.contatos_novos = contatos
    run.produtos_novos = produtos
    run.erro_mensagem = erro
    db.commit()
    logger.info("ETL run #%d finalizada: status=%s, nfes=%d", run_id, status, nfes)


def get_last_successful_run(db: Session) -> EtlControle | None:
    return db.scalars(
        select(EtlControle)
        .where(EtlControle.status == "success")
        .order_by(EtlControle.data_referencia.desc())
    ).first()
