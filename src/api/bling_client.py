import logging
import time
from typing import Self

import httpx
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from src.config import get_settings

logger = logging.getLogger(__name__)


class BlingRateLimitError(Exception):
    """Erro de rate limit da API Bling (429)."""


class BlingClient:
    def __init__(self, access_token: str) -> None:
        settings = get_settings()
        self._base_url = settings.BLING_API_BASE_URL
        self._delay = settings.API_RATE_LIMIT_DELAY
        self._page_size = settings.API_PAGE_SIZE
        self._last_request_time = 0.0
        self._client = httpx.Client(
            timeout=30,
            headers={
                "Authorization": f"Bearer {access_token}",
                "Accept": "application/json",
            },
        )

    # ── Context manager ──────────────────────────────────────────────────

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def close(self) -> None:
        self._client.close()

    # ── Internos ─────────────────────────────────────────────────────────

    def _wait_rate_limit(self) -> None:
        elapsed = time.monotonic() - self._last_request_time
        if elapsed < self._delay:
            time.sleep(self._delay - elapsed)
        self._last_request_time = time.monotonic()

    @retry(
        retry=retry_if_exception_type(BlingRateLimitError),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    def _request(self, method: str, path: str, params: dict | None = None) -> dict:
        self._wait_rate_limit()
        url = f"{self._base_url}/{path}"
        response = self._client.request(method, url, params=params)

        if response.status_code == 429:
            logger.warning("Rate limit atingido (429) em %s", path)
            raise BlingRateLimitError(f"Rate limit em {path}")

        if response.status_code == 401:
            raise RuntimeError("Token inválido ou expirado")

        response.raise_for_status()
        return response.json()

    def get(self, path: str, params: dict | None = None) -> dict:
        return self._request("GET", path, params)

    # ── NF-e ─────────────────────────────────────────────────────────────

    def listar_nfes(
        self,
        tipo: int = 1,
        data_inicio: str | None = None,
        data_fim: str | None = None,
        situacao: int | None = None,
        pagina: int = 1,
    ) -> dict:
        params = {
            "tipo": tipo,
            "pagina": pagina,
            "limite": self._page_size,
        }
        if data_inicio:
            params["dataEmissaoInicial"] = data_inicio
        if data_fim:
            params["dataEmissaoFinal"] = data_fim
        if situacao is not None:
            params["situacao"] = situacao

        return self.get("nfe", params=params)

    def listar_todas_nfes(
        self,
        data_inicio: str | None = None,
        data_fim: str | None = None,
        situacao: int | None = None,
    ) -> list[dict]:
        todas = []
        pagina = 1

        while True:
            logger.info("Listando NF-es — página %d", pagina)
            resp = self.listar_nfes(
                data_inicio=data_inicio,
                data_fim=data_fim,
                situacao=situacao,
                pagina=pagina,
            )
            registros = resp.get("data", [])
            todas.extend(registros)

            logger.info(
                "Página %d: %d registros (total acumulado: %d)",
                pagina,
                len(registros),
                len(todas),
            )

            if len(registros) < self._page_size:
                break
            pagina += 1

        logger.info("Listagem completa: %d NF-es em %d páginas", len(todas), pagina)
        return todas

    def detalhar_nfe(self, nfe_id: int) -> dict:
        return self.get(f"nfe/{nfe_id}")

    # ── Contatos ─────────────────────────────────────────────────────────

    def buscar_contato(self, contato_id: int) -> dict:
        return self.get(f"contatos/{contato_id}")

    # ── Produtos ─────────────────────────────────────────────────────────

    def buscar_produto(self, produto_id: int) -> dict:
        return self.get(f"produtos/{produto_id}")

    def buscar_produto_por_codigo(self, codigo: str) -> dict | None:
        try:
            resp = self.get("produtos", params={"codigo": codigo})
            data = resp.get("data", [])
            return data[0] if data else None
        except Exception:
            logger.warning("Produto código=%s não encontrado", codigo)
            return None
