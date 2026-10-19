"""Connection-pool lifecycle and schema bootstrap."""
import logging
from pathlib import Path

import asyncpg

from app.config import Settings

logger = logging.getLogger(__name__)
SCHEMA_PATH = Path(__file__).with_name("schema.sql")


async def create_pool(settings: Settings) -> asyncpg.Pool:
    pool = await asyncpg.create_pool(
        dsn=settings.database_url,
        min_size=settings.db_pool_min_size,
        max_size=settings.db_pool_max_size,
        command_timeout=30.0,
        timeout=60,
    )
    logger.info("Database pool ready")
    return pool


async def init_schema(pool: asyncpg.Pool) -> None:
    await pool.execute(SCHEMA_PATH.read_text())
    logger.info("Database schema ensured")
