"""HTTP routes. Thin: validate -> call repository -> shape the response."""
import logging
from datetime import datetime, timezone
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Path, Request, status

from app.repository.transactions import TransactionRepository
from app.models.schemas import (
    ID_PATTERN,
    ErrorOut,
    HealthOut,
    TransactionOut,
    WebhookAck,
    WebhookPayload,
)
from app.services.worker import BackgroundProcessor

logger = logging.getLogger(__name__)
router = APIRouter()


def get_repository(request: Request) -> TransactionRepository:
    return request.app.state.repo


def get_processor(request: Request) -> BackgroundProcessor | None:
    return getattr(request.app.state, "processor", None)


@router.get("/", response_model=HealthOut, tags=["health"], summary="Liveness check")
async def health() -> HealthOut:
    # Deliberately does not touch the database: this is a liveness probe and
    # must stay fast and green even if the DB is briefly unreachable.
    return HealthOut(current_time=datetime.now(timezone.utc).replace(microsecond=0))


@router.get("/ready", tags=["health"], summary="Readiness check (verifies the database)")
async def ready(repo: Annotated[TransactionRepository, Depends(get_repository)]) -> dict:
    await repo.ping()
    return {"status": "READY"}


@router.post(
    "/v1/webhooks/transactions",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=WebhookAck,
    tags=["webhooks"],
    summary="Receive a transaction webhook",
    responses={503: {"model": ErrorOut, "description": "Could not persist; sender should retry"}},
)
async def receive_transaction_webhook(
    payload: WebhookPayload,
    repo: Annotated[TransactionRepository, Depends(get_repository)],
    processor: Annotated[BackgroundProcessor | None, Depends(get_processor)],
) -> WebhookAck:
    # Only one fast DB write happens in the request path. The 30-second work
    # is done later by the background processor, so latency does not depend on
    # processing complexity.
    #
    # We return 202 only AFTER the row is committed. If the database is down
    # the exception handler returns 503 and the sender retries, so an
    # acknowledged webhook is never lost.
    created = await repo.insert_if_absent(payload)
    if created:
        if processor:
            processor.wake()
        logger.info("Accepted %s", payload.transaction_id)
    else:
        logger.info("Duplicate webhook ignored: %s", payload.transaction_id)
    return WebhookAck(transaction_id=payload.transaction_id, duplicate=not created)


@router.get(
    "/v1/transactions/{transaction_id}",
    response_model=list[TransactionOut],
    tags=["transactions"],
    summary="Get transaction status (for testing)",
    responses={404: {"model": ErrorOut}},
)
async def get_transaction(
    transaction_id: Annotated[str, Path(pattern=ID_PATTERN)],
    repo: Annotated[TransactionRepository, Depends(get_repository)],
) -> list[TransactionOut]:
    row = await repo.get(transaction_id)
    if row is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Transaction not found")
    # The assignment specifies a JSON array containing one object.
    return [TransactionOut.model_validate(row)]
