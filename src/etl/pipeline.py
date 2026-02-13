import logging
from datetime import date, datetime, timedelta, timezone

from sqlalchemy.orm import Session

from src.api.bling_client import BlingClient
from src.auth.oauth import refresh_access_token
from src.config import get_settings
from src.db.repository import (
    create_etl_run,
    finish_etl_run,
    get_existing_contato_ids,
    get_existing_produto_codigos,
    get_last_successful_run,
    upsert_contato,
    upsert_nfe_cabecalho,
    upsert_nfe_itens,
    upsert_nfe_pagamentos,
    upsert_produto,
)

logger = logging.getLogger(__name__)

CHECKPOINT_INTERVAL = 50


# ── Funções auxiliares ───────────────────────────────────────────────────────


def _to_float(val) -> float:
    try:
        return float(val)
    except (TypeError, ValueError):
        return 0.0


def _to_int(val) -> int | None:
    try:
        return int(val)
    except (TypeError, ValueError):
        return None


def _safe_get(data, *keys, default=None):
    """Navegação segura em dicts aninhados."""
    current = data
    for key in keys:
        if not isinstance(current, dict):
            return default
        current = current.get(key)
        if current is None:
            return default
    return current


def _gerar_periodos_mensais(data_inicio: str, data_fim: str) -> list[tuple[str, str]]:
    """Quebra um range de datas em períodos mensais."""
    inicio = date.fromisoformat(data_inicio)
    fim = date.fromisoformat(data_fim)
    periodos = []

    cursor = inicio
    while cursor <= fim:
        # Último dia do mês corrente
        if cursor.month == 12:
            fim_mes = date(cursor.year + 1, 1, 1) - timedelta(days=1)
        else:
            fim_mes = date(cursor.year, cursor.month + 1, 1) - timedelta(days=1)

        periodo_fim = min(fim_mes, fim)
        periodos.append((cursor.isoformat(), periodo_fim.isoformat()))
        cursor = periodo_fim + timedelta(days=1)

    return periodos


# ── Pipeline ─────────────────────────────────────────────────────────────────


class Pipeline:
    def __init__(self, db: Session) -> None:
        self.db = db
        self.stats = {"nfes": 0, "contatos": 0, "produtos": 0}

    def run(
        self,
        data_inicio: str | None = None,
        data_fim: str | None = None,
    ) -> dict:
        settings = get_settings()
        now = datetime.now(timezone.utc)

        # 1. Determinar período
        if not data_inicio:
            last_run = get_last_successful_run(self.db)
            if last_run and last_run.data_referencia:
                data_inicio = last_run.data_referencia.strftime("%Y-%m-%d")
                logger.info("Usando data da última execução: %s", data_inicio)
            else:
                dt = now - timedelta(days=settings.EXTRACTION_DAYS_BACK)
                data_inicio = dt.strftime("%Y-%m-%d")
                logger.info("Sem execução anterior, usando: %s", data_inicio)

        if not data_fim:
            data_fim = now.strftime("%Y-%m-%d")

        logger.info("Período de extração: %s a %s", data_inicio, data_fim)

        # 2. Criar registro ETL
        run_id = create_etl_run(self.db, now.date())

        try:
            # 3. Refresh do token OAuth
            logger.info("Renovando token OAuth...")
            access_token = refresh_access_token(self.db)

            # 4. Criar client
            with BlingClient(access_token) as client:
                # 5. Etapa NF-e
                nfes = self._extrair_nfes(client, data_inicio, data_fim)

                # 6. Etapa Contatos
                self._extrair_contatos(client, nfes)

                # 7. Etapa Produtos
                self._extrair_produtos(client, nfes)

            # 8. Commit final
            self.db.commit()

            # 9. Finalizar ETL com sucesso
            finish_etl_run(
                self.db,
                run_id,
                status="success",
                nfes=self.stats["nfes"],
                contatos=self.stats["contatos"],
                produtos=self.stats["produtos"],
            )
            logger.info("Pipeline concluído com sucesso: %s", self.stats)
            return {"status": "success", "stats": self.stats, "run_id": run_id}

        except Exception as e:
            # 10. Rollback e registrar erro
            self.db.rollback()
            erro_msg = f"{type(e).__name__}: {e}"
            logger.error("Pipeline falhou: %s", erro_msg)
            finish_etl_run(
                self.db,
                run_id,
                status="error",
                nfes=self.stats["nfes"],
                contatos=self.stats["contatos"],
                produtos=self.stats["produtos"],
                erro=erro_msg,
            )
            return {"status": "error", "stats": self.stats, "run_id": run_id}

    def run_full(
        self,
        data_inicio: str,
        data_fim: str,
    ) -> dict:
        """Extração completa quebrando em períodos mensais."""
        now = datetime.now(timezone.utc)
        run_id = create_etl_run(self.db, now.date())

        try:
            logger.info("Renovando token OAuth...")
            access_token = refresh_access_token(self.db)

            # Gerar períodos mensais
            periodos = _gerar_periodos_mensais(data_inicio, data_fim)
            logger.info(
                "Extração completa: %s a %s (%d períodos)",
                data_inicio, data_fim, len(periodos),
            )

            todas_nfes = []
            with BlingClient(access_token) as client:
                for i, (ini, fim) in enumerate(periodos, 1):
                    logger.info(
                        "=== Período %d/%d: %s a %s ===",
                        i, len(periodos), ini, fim,
                    )
                    nfes = self._extrair_nfes(client, ini, fim)
                    todas_nfes.extend(nfes)
                    self.db.commit()

                # Contatos e Produtos sobre todas as NF-e coletadas
                self._extrair_contatos(client, todas_nfes)
                self._extrair_produtos(client, todas_nfes)

            self.db.commit()
            finish_etl_run(
                self.db, run_id, status="success",
                nfes=self.stats["nfes"],
                contatos=self.stats["contatos"],
                produtos=self.stats["produtos"],
            )
            logger.info("Extração completa finalizada: %s", self.stats)
            return {"status": "success", "stats": self.stats, "run_id": run_id}

        except Exception as e:
            self.db.rollback()
            erro_msg = f"{type(e).__name__}: {e}"
            logger.error("Extração completa falhou: %s", erro_msg)
            finish_etl_run(
                self.db, run_id, status="error",
                nfes=self.stats["nfes"],
                contatos=self.stats["contatos"],
                produtos=self.stats["produtos"],
                erro=erro_msg,
            )
            return {"status": "error", "stats": self.stats, "run_id": run_id}

    # ── Etapa NF-e ───────────────────────────────────────────────────────

    def _extrair_nfes(
        self,
        client: BlingClient,
        data_inicio: str,
        data_fim: str,
    ) -> list[dict]:
        logger.info("=== Etapa 1: Extração de NF-e ===")
        resumos = client.listar_todas_nfes(data_inicio=data_inicio, data_fim=data_fim)
        logger.info("NF-es encontradas na listagem: %d", len(resumos))

        nfes_detalhadas = []
        for i, resumo in enumerate(resumos, 1):
            nfe_id = resumo.get("id")
            try:
                detalhe = client.detalhar_nfe(nfe_id)
                self._salvar_nfe(resumo, detalhe)
                nfes_detalhadas.append(detalhe)
                self.stats["nfes"] += 1

                # Checkpoint a cada 50 NF-e
                if self.stats["nfes"] % CHECKPOINT_INTERVAL == 0:
                    self.db.commit()
                    logger.info("Checkpoint: %d NF-es salvas", self.stats["nfes"])

            except Exception:
                logger.error("Erro ao processar NF-e id=%s", nfe_id, exc_info=True)

            if i % 100 == 0:
                logger.info("Progresso NF-e: %d/%d", i, len(resumos))

        logger.info("Etapa NF-e concluída: %d processadas", self.stats["nfes"])
        return nfes_detalhadas

    def _salvar_nfe(self, resumo: dict, detalhe: dict) -> None:
        nfe_id = resumo["id"]
        contato = resumo.get("contato", {}) or {}
        endereco = contato.get("endereco", {}) or {}
        dados = detalhe.get("data", {}) or {}

        # Totais vêm direto do detalhe
        valor_nota = _to_float(dados.get("valorNota"))
        # Somar valorTotal dos itens para total_produtos
        itens_raw = dados.get("itens", []) or []
        total_produtos = sum(_to_float(it.get("valorTotal")) for it in itens_raw)

        cabecalho = {
            "id": nfe_id,
            "numero": resumo.get("numero"),
            "data_emissao": resumo.get("dataEmissao"),
            "situacao": _to_int(resumo.get("situacao")),
            "contato_id": _to_int(contato.get("id")),
            "contato_nome": contato.get("nome"),
            "contato_documento": contato.get("numeroDocumento"),
            "contato_municipio": endereco.get("municipio"),
            "contato_uf": endereco.get("uf"),
            "total_produtos": total_produtos,
            "total_nota": valor_nota,
            "total_descontos": total_produtos - valor_nota if total_produtos > valor_nota else 0,
        }
        upsert_nfe_cabecalho(self.db, cabecalho)

        # Itens vêm de data.itens[]
        itens = []
        for it in itens_raw:
            itens.append({
                "codigo_produto": it.get("codigo"),
                "descricao_produto": it.get("descricao"),
                "quantidade": _to_float(it.get("quantidade")),
                "valor_unitario": _to_float(it.get("valor")),
                "valor_total": _to_float(it.get("valorTotal")),
                "valor_desconto": 0,
                "unidade_medida": it.get("unidade"),
            })
        upsert_nfe_itens(self.db, nfe_id, itens)

        # Pagamentos vêm de data.parcelas[]
        parcelas_raw = dados.get("parcelas", []) or []
        pagamentos = []
        for parc in parcelas_raw:
            pagamentos.append({
                "tipo_pagamento": _to_int(parc.get("formaPagamento", {}).get("id")),
                "valor": _to_float(parc.get("valor")),
            })
        upsert_nfe_pagamentos(self.db, nfe_id, pagamentos)

    # ── Etapa Contatos ───────────────────────────────────────────────────

    def _extrair_contatos(self, client: BlingClient, nfes: list[dict]) -> None:
        logger.info("=== Etapa 2: Extração de Contatos ===")

        # Coletar IDs de contatos das NF-e
        contato_ids_nfe = set()
        for nfe in nfes:
            cid = _to_int(_safe_get(nfe, "data", "contato", "id"))
            if cid:
                contato_ids_nfe.add(cid)

        existentes = get_existing_contato_ids(self.db)
        novos = contato_ids_nfe - existentes
        logger.info(
            "Contatos nas NF-e: %d | Já existentes: %d | Novos: %d",
            len(contato_ids_nfe),
            len(existentes & contato_ids_nfe),
            len(novos),
        )

        for contato_id in novos:
            try:
                resp = client.buscar_contato(contato_id)
                data = resp.get("data", {}) or {}
                endereco_geral = _safe_get(data, "endereco", "geral") or {}

                upsert_contato(self.db, {
                    "id": contato_id,
                    "nome": data.get("nome"),
                    "documento": data.get("numeroDocumento"),
                    "email": data.get("email"),
                    "tipo_pessoa": data.get("tipo"),
                    "municipio": endereco_geral.get("municipio"),
                    "uf": endereco_geral.get("uf"),
                })
                self.stats["contatos"] += 1
            except Exception:
                logger.error(
                    "Erro ao buscar contato id=%d", contato_id, exc_info=True
                )

        logger.info("Etapa Contatos concluída: %d novos", self.stats["contatos"])

    # ── Etapa Produtos ───────────────────────────────────────────────────

    def _extrair_produtos(self, client: BlingClient, nfes: list[dict]) -> None:
        logger.info("=== Etapa 3: Extração de Produtos ===")

        # Coletar códigos de produtos dos itens das NF-e
        codigos_nfe = set()
        for nfe in nfes:
            itens = _safe_get(nfe, "data", "itens") or []
            for it in itens:
                codigo = it.get("codigo")
                if codigo:
                    codigos_nfe.add(str(codigo))

        existentes = get_existing_produto_codigos(self.db)
        novos = codigos_nfe - existentes
        logger.info(
            "Produtos nas NF-e: %d | Já existentes: %d | Novos: %d",
            len(codigos_nfe),
            len(existentes & codigos_nfe),
            len(novos),
        )

        for codigo in novos:
            try:
                produto = client.buscar_produto_por_codigo(codigo)
                if not produto:
                    logger.warning("Produto código=%s não encontrado na API", codigo)
                    continue

                categoria = produto.get("categoria", {}) or {}
                fornecedor = produto.get("fornecedor", {}) or {}
                upsert_produto(self.db, {
                    "id": produto["id"],
                    "codigo": codigo,
                    "nome": produto.get("nome"),
                    "preco_venda": _to_float(produto.get("preco")),
                    "preco_custo": _to_float(fornecedor.get("precoCusto")),
                    "categoria_id": _to_int(categoria.get("id")),
                    "categoria_descricao": categoria.get("descricao"),
                })
                self.stats["produtos"] += 1
            except Exception:
                logger.error(
                    "Erro ao buscar produto código=%s", codigo, exc_info=True
                )

        logger.info("Etapa Produtos concluída: %d novos", self.stats["produtos"])
