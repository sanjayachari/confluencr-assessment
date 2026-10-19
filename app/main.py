"""Application factory and wiring."""
import asyncio
import logging
from contextlib import asynccontextmanager

import asyncpg
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from app.api import router
from app.config import Settings, get_settings
from app.db import create_pool, init_schema
from app.repository import TransactionRepository
from app.worker import BackgroundProcessor

logger = logging.getLogger(__name__)


def create_app(
    repository: TransactionRepository | None = None,
    settings: Settings | None = None,
    run_worker: bool = True,
) -> FastAPI:
    """Build the app. `repository`/`run_worker` exist so tests can inject fakes."""

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        pool = None
        processor = None
        if repository is not None:
            app.state.repo = repository
        else:
            cfg = settings or get_settings()
            logging.basicConfig(
                level=cfg.log_level.upper(),
                format="%(asctime)s %(levelname)s %(name)s: %(message)s",
            )
            pool = await create_pool(cfg)
            await init_schema(pool)
            app.state.repo = TransactionRepository(pool)
            if run_worker:
                processor = BackgroundProcessor(app.state.repo, cfg)
                app.state.processor = processor
                processor.start()
        try:
            yield
        finally:
            if processor:
                await processor.stop()
            if pool:
                await pool.close()

    app = FastAPI(
        title="Transaction Webhook Service",
        version="1.0.0",
        description=(
            "Accepts payment-processor webhooks instantly (202) and processes "
            "them reliably and idempotently in the background."
        ),
        lifespan=lifespan,
    )
    app.include_router(router)

    @app.exception_handler(asyncpg.PostgresError)
    @app.exception_handler(asyncpg.InterfaceError)
    @app.exception_handler(OSError)
    @app.exception_handler(asyncio.TimeoutError)
    async def database_unavailable(request: Request, exc: Exception) -> JSONResponse:
        # 503 (not 202) so the webhook sender knows to retry later.
        logger.error("Database error on %s %s: %r", request.method, request.url.path, exc)
        return JSONResponse(
            status_code=503,
            content={"detail": "Service temporarily unavailable, please retry"},
            headers={"Retry-After": "5"},
        )

    return app


# `uvicorn app.main:app` -> lazily builds the app; settings are read at startup.
app = create_app()
