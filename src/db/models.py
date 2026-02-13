from datetime import datetime, timezone

from sqlalchemy import (
    BigInteger,
    Date,
    DateTime,
    Float,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    ForeignKey,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.db.database import Base


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


# ── 1. OAuth Token (sempre 1 registro) ──────────────────────────────────────

class OAuthToken(Base):
    __tablename__ = "oauth_tokens"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, default=1)
    access_token: Mapped[str] = mapped_column(Text, nullable=False)
    refresh_token: Mapped[str] = mapped_column(Text, nullable=False)
    token_type: Mapped[str] = mapped_column(String(50), default="Bearer")
    expires_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=_utcnow, onupdate=_utcnow
    )


# ── 2. NF-e Cabeçalho ───────────────────────────────────────────────────────

class NfeCabecalho(Base):
    __tablename__ = "nfe_cabecalho"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=False)
    numero: Mapped[str | None] = mapped_column(String(50), index=True)
    data_emissao: Mapped[datetime | None] = mapped_column(DateTime, index=True)
    situacao: Mapped[int | None] = mapped_column(Integer)
    contato_id: Mapped[int | None] = mapped_column(BigInteger, index=True)
    contato_nome: Mapped[str | None] = mapped_column(String(500))
    contato_documento: Mapped[str | None] = mapped_column(String(20), index=True)
    contato_municipio: Mapped[str | None] = mapped_column(String(200))
    contato_uf: Mapped[str | None] = mapped_column(String(2))
    total_produtos: Mapped[float] = mapped_column(Float, default=0)
    total_nota: Mapped[float] = mapped_column(Float, default=0)
    total_descontos: Mapped[float] = mapped_column(Float, default=0)
    extraido_em: Mapped[datetime] = mapped_column(DateTime, default=_utcnow)

    itens: Mapped[list["NfeItem"]] = relationship(
        back_populates="nfe", cascade="all, delete-orphan"
    )
    pagamentos: Mapped[list["NfePagamento"]] = relationship(
        back_populates="nfe", cascade="all, delete-orphan"
    )


# ── 3. NF-e Itens ───────────────────────────────────────────────────────────

class NfeItem(Base):
    __tablename__ = "nfe_itens"
    __table_args__ = (
        UniqueConstraint("nfe_id", "codigo_produto", name="uq_nfe_item"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    nfe_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("nfe_cabecalho.id"), index=True, nullable=False
    )
    codigo_produto: Mapped[str | None] = mapped_column(String(100), index=True)
    descricao_produto: Mapped[str | None] = mapped_column(String(500))
    quantidade: Mapped[float] = mapped_column(Float, default=0)
    valor_unitario: Mapped[float] = mapped_column(Float, default=0)
    valor_total: Mapped[float] = mapped_column(Float, default=0)
    valor_desconto: Mapped[float] = mapped_column(Float, default=0)
    unidade_medida: Mapped[str | None] = mapped_column(String(20))

    nfe: Mapped["NfeCabecalho"] = relationship(back_populates="itens")


# ── 4. NF-e Pagamentos ──────────────────────────────────────────────────────

class NfePagamento(Base):
    __tablename__ = "nfe_pagamentos"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    nfe_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("nfe_cabecalho.id"), index=True, nullable=False
    )
    tipo_pagamento: Mapped[int | None] = mapped_column(
        Integer, comment="1=Dinheiro, 2=Cheque, 3=CC, 4=CD, 15=Boleto, 17=PIX"
    )
    valor: Mapped[float] = mapped_column(Float, default=0)

    nfe: Mapped["NfeCabecalho"] = relationship(back_populates="pagamentos")


# ── 5. Contatos ──────────────────────────────────────────────────────────────

class Contato(Base):
    __tablename__ = "contatos"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=False)
    nome: Mapped[str | None] = mapped_column(String(500))
    documento: Mapped[str | None] = mapped_column(String(20), index=True)
    email: Mapped[str | None] = mapped_column(String(500))
    tipo_pessoa: Mapped[str | None] = mapped_column(
        String(1), comment="F=Física, J=Jurídica"
    )
    municipio: Mapped[str | None] = mapped_column(String(200))
    uf: Mapped[str | None] = mapped_column(String(2))
    extraido_em: Mapped[datetime] = mapped_column(DateTime, default=_utcnow)


# ── 6. Produtos ──────────────────────────────────────────────────────────────

class Produto(Base):
    __tablename__ = "produtos"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=False)
    codigo: Mapped[str | None] = mapped_column(String(100), unique=True, index=True)
    nome: Mapped[str | None] = mapped_column(String(500))
    preco_venda: Mapped[float] = mapped_column(Float, default=0)
    preco_custo: Mapped[float] = mapped_column(Float, default=0)
    categoria_id: Mapped[int | None] = mapped_column(BigInteger)
    categoria_descricao: Mapped[str | None] = mapped_column(String(300))
    extraido_em: Mapped[datetime] = mapped_column(DateTime, default=_utcnow)


# ── 7. ETL Controle ─────────────────────────────────────────────────────────

class EtlControle(Base):
    __tablename__ = "etl_controle"
    __table_args__ = (
        Index("ix_etl_controle_status", "status"),
        Index("ix_etl_controle_data_ref", "data_referencia"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    inicio: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    fim: Mapped[datetime | None] = mapped_column(DateTime)
    status: Mapped[str] = mapped_column(
        String(20), default="running", comment="running, success, error"
    )
    data_referencia: Mapped[datetime | None] = mapped_column(Date)
    nfes_processadas: Mapped[int] = mapped_column(Integer, default=0)
    contatos_novos: Mapped[int] = mapped_column(Integer, default=0)
    produtos_novos: Mapped[int] = mapped_column(Integer, default=0)
    erro_mensagem: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow)
