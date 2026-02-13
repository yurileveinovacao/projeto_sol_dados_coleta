import base64
import logging
from datetime import datetime, timedelta, timezone

import httpx
from sqlalchemy.orm import Session

from src.config import get_settings
from src.db.repository import get_oauth_token, save_oauth_token

logger = logging.getLogger(__name__)

TIMEOUT = 30


def _get_basic_auth_header() -> str:
    settings = get_settings()
    credentials = f"{settings.BLING_CLIENT_ID}:{settings.BLING_CLIENT_SECRET}"
    encoded = base64.b64encode(credentials.encode()).decode()
    return f"Basic {encoded}"


def exchange_authorization_code(db: Session, code: str) -> dict:
    """Troca authorization code por tokens. Usado apenas na primeira autorização."""
    settings = get_settings()
    logger.info("Trocando authorization code por tokens...")

    response = httpx.post(
        settings.BLING_OAUTH_URL,
        headers={
            "Authorization": _get_basic_auth_header(),
            "Content-Type": "application/x-www-form-urlencoded",
        },
        data={
            "grant_type": "authorization_code",
            "code": code,
        },
        timeout=TIMEOUT,
    )
    response.raise_for_status()
    data = response.json()

    save_oauth_token(
        db=db,
        access_token=data["access_token"],
        refresh_token=data["refresh_token"],
        expires_in=data.get("expires_in", 21600),
    )
    logger.info("Tokens salvos com sucesso (authorization code)")
    return data


def refresh_access_token(db: Session) -> str:
    """Renova o access_token usando o refresh_token atual."""
    settings = get_settings()
    token = get_oauth_token(db)

    if not token:
        raise RuntimeError(
            "Nenhum token encontrado no banco. Execute first_auth.py primeiro."
        )

    logger.info("Renovando access_token via refresh_token...")

    try:
        response = httpx.post(
            settings.BLING_OAUTH_URL,
            headers={
                "Authorization": _get_basic_auth_header(),
                "Content-Type": "application/x-www-form-urlencoded",
            },
            data={
                "grant_type": "refresh_token",
                "refresh_token": token.refresh_token,
            },
            timeout=TIMEOUT,
        )
        response.raise_for_status()
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 400:
            body = e.response.json() if e.response.content else {}
            if body.get("error") == "invalid_grant":
                logger.error("Refresh token expirado ou inválido: %s", body)
                raise RuntimeError(
                    "Refresh token expirado. Necessário reautorizar via first_auth.py"
                ) from e
        raise

    data = response.json()

    # CRÍTICO: salvar imediatamente — o refresh_token antigo já foi invalidado
    save_oauth_token(
        db=db,
        access_token=data["access_token"],
        refresh_token=data["refresh_token"],
        expires_in=data.get("expires_in", 21600),
    )
    logger.info("Tokens renovados e salvos com sucesso")
    return data["access_token"]


def get_valid_access_token(db: Session) -> str:
    """Retorna um access_token válido, renovando se necessário."""
    token = get_oauth_token(db)

    if not token:
        raise RuntimeError(
            "Nenhum token encontrado no banco. Execute first_auth.py primeiro."
        )

    now = datetime.now(timezone.utc)
    expires_at = token.expires_at.replace(tzinfo=timezone.utc)
    remaining = expires_at - now

    if remaining < timedelta(minutes=10):
        logger.info(
            "Token expira em %s — renovando...", remaining
        )
        return refresh_access_token(db)

    logger.debug("Token válido (expira em %s)", remaining)
    return token.access_token
