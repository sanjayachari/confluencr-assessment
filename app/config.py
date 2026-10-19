"""Application settings, loaded from environment variables (or a local .env file)."""
from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # --- Database -------------------------------------------------------
    database_url: str
    db_pool_min_size: int = Field(default=5, ge=1)
    db_pool_max_size: int = Field(default=20, ge=1)

    # --- Background processing -------------------------------------------
    # Simulated external API call duration (the assignment requires 30s).
    processing_delay_seconds: float = Field(default=30.0, ge=0)
    # How long a worker "owns" a transaction before another worker may take
    # it over (crash recovery). Must comfortably exceed the processing delay.
    lease_seconds: float = Field(default=120.0, gt=0)
    # Timeout after which an uncompleted in-flight transaction is considered
    # stuck and re-queued (crash recovery).
    stuck_timeout_seconds: float = Field(default=35.0, gt=0)
    max_attempts: int = Field(default=5, ge=1)
    retry_backoff_seconds: float = Field(default=5.0, ge=0)
    worker_poll_interval_seconds: float = Field(default=0.5, gt=0)
    worker_batch_size: int = Field(default=100, ge=1)
    worker_max_in_flight: int = Field(default=1000, ge=1)

    log_level: str = "INFO"


@lru_cache
def get_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]
