from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    BLING_CLIENT_ID: str
    BLING_CLIENT_SECRET: str
    BLING_API_BASE_URL: str = "https://api.bling.com.br/Api/v3"
    BLING_OAUTH_URL: str = "https://api.bling.com.br/Api/v3/oauth/token"

    DATABASE_URL: str

    EXTRACTION_DAYS_BACK: int = 1
    API_RATE_LIMIT_DELAY: float = 0.35
    API_PAGE_SIZE: int = 100
    LOG_LEVEL: str = "INFO"


@lru_cache
def get_settings() -> Settings:
    return Settings()
