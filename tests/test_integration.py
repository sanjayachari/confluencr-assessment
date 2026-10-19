"""End-to-end tests against a REAL Postgres (repository + worker + HTTP).

Run with:  TEST_DATABASE_URL=postgresql://user:pass@localhost/db pytest
Skipped automatically when TEST_DATABASE_URL is not set.
"""
import asyncio
import os
import time

import asyncpg
import httpx
import pytest

from app.core.config import Settings
from app.core.db import init_schema
from app.main import create_app
from app.repository.transactions import TransactionRepository
from app.models.schemas import WebhookPayload
from app.services.worker import BackgroundProcessor

DB_URL = os.getenv("TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(not DB_URL, reason="TEST_DATABASE_URL not set")


def payload(tid: str) -> dict:
    return {
        "transaction_id": tid,
        "source_account": "acc_user_789",
        "destination_account": "acc_merchant_456",
        "amount": 1500,
        "currency": "INR",
    }


def make_settings(**kw) -> Settings:
    base = dict(
        database_url=DB_URL,
        processing_delay_seconds=0.3,
        lease_seconds=2,
        retry_backoff_seconds=0.2,
        worker_poll_interval_seconds=0.1,
    )
    base.update(kw)
    return Settings(**base)


@pytest.fixture
async def pool():
    p = await asyncpg.create_pool(DB_URL, statement_cache_size=0)
    await init_schema(p)
    await p.execute("TRUNCATE transactions")
    yield p
    await p.close()


async def wait_for(predicate, timeout=10.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if await predicate():
            return True
        await asyncio.sleep(0.05)
    return False


async def test_full_flow_over_http_and_duplicates_processed_once(pool):
    settings = make_settings()
    app = create_app(settings=settings)
    async with app.router.lifespan_context(app):
        await pool.execute("TRUNCATE transactions")
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
            # 20 concurrent identical webhooks
            rs = await asyncio.gather(*[c.post("/v1/webhooks/transactions", json=payload("txn_dup")) for _ in range(20)])
            assert all(r.status_code == 202 for r in rs)
            assert sum(1 for r in rs if r.json()["duplicate"] is False) == 1

            async def done():
                r = await c.get("/v1/transactions/txn_dup")
                return r.json()[0]["status"] == "PROCESSED"

            assert await wait_for(done)
            txn = (await c.get("/v1/transactions/txn_dup")).json()[0]
            assert txn["processed_at"] is not None
            assert txn["processed_at"] > txn["created_at"]

    assert await pool.fetchval("SELECT count(*) FROM transactions") == 1
    # processed exactly once: attempts == 1
    assert await pool.fetchval("SELECT attempts FROM transactions WHERE transaction_id='txn_dup'") == 1


async def test_many_transactions_none_lost(pool):
    settings = make_settings(processing_delay_seconds=0.5)
    app = create_app(settings=settings)
    async with app.router.lifespan_context(app):
        await pool.execute("TRUNCATE transactions")
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
            n = 300
            rs = await asyncio.gather(*[c.post("/v1/webhooks/transactions", json=payload(f"txn_{i}")) for i in range(n)])
            assert all(r.status_code == 202 for r in rs)

            async def all_done():
                return await pool.fetchval("SELECT count(*) FROM transactions WHERE status='PROCESSED'") == n

            assert await wait_for(all_done, timeout=30)
    assert await pool.fetchval("SELECT max(attempts) FROM transactions") == 1


async def test_crashed_worker_is_recovered_after_lease_expiry(pool):
    """Simulate a crash: a worker claims a row then dies without finishing."""
    repo = TransactionRepository(pool)
    await repo.insert_if_absent(WebhookPayload(**payload("txn_crash")))
    claimed = await repo.claim_batch(10, lease_seconds=1, max_attempts=5)  # "dead" worker
    assert [c.transaction_id for c in claimed] == ["txn_crash"]
    # While leased, nobody else can take it
    assert await repo.claim_batch(10, lease_seconds=1, max_attempts=5) == []

    processor = BackgroundProcessor(repo, make_settings(processing_delay_seconds=0.1))
    processor.start()
    try:
        async def done():
            return (await repo.get("txn_crash"))["status"] == "PROCESSED"
        assert await wait_for(done, timeout=8)
    finally:
        await processor.stop()
    assert await pool.fetchval("SELECT attempts FROM transactions WHERE transaction_id='txn_crash'") == 2


async def test_two_workers_never_process_the_same_row(pool):
    repo = TransactionRepository(pool)
    for i in range(50):
        await repo.insert_if_absent(WebhookPayload(**payload(f"txn_{i}")))
    a, b = await asyncio.gather(
        repo.claim_batch(50, 60, 5), repo.claim_batch(50, 60, 5)
    )
    ids_a = {c.transaction_id for c in a}
    ids_b = {c.transaction_id for c in b}
    assert ids_a.isdisjoint(ids_b)
    assert len(ids_a | ids_b) == 50


async def test_failed_processing_is_retried_then_marked_failed(pool):
    repo = TransactionRepository(pool)
    await repo.insert_if_absent(WebhookPayload(**payload("txn_bad")))

    settings = make_settings(max_attempts=3, retry_backoff_seconds=0.05, lease_seconds=1)
    processor = BackgroundProcessor(repo, settings)

    async def boom(_tid):
        raise RuntimeError("external API down")

    processor._call_external_service = boom  # type: ignore[method-assign]
    processor.start()
    try:
        async def failed():
            return (await repo.get("txn_bad"))["status"] == "FAILED"
        assert await wait_for(failed, timeout=10)
    finally:
        await processor.stop()
    row = await pool.fetchrow("SELECT attempts, last_error FROM transactions WHERE transaction_id='txn_bad'")
    assert row["attempts"] == 3
    assert "external API down" in row["last_error"]


async def test_graceful_shutdown_releases_in_flight_work(pool):
    repo = TransactionRepository(pool)
    await repo.insert_if_absent(WebhookPayload(**payload("txn_shutdown")))
    processor = BackgroundProcessor(repo, make_settings(processing_delay_seconds=30, lease_seconds=120))
    processor.start()
    await asyncio.sleep(0.5)  # worker claims it and starts the 30s wait
    assert await pool.fetchval("SELECT locked_until IS NOT NULL FROM transactions WHERE transaction_id='txn_shutdown'")
    await processor.stop()
    row = await pool.fetchrow("SELECT status, locked_until, attempts FROM transactions WHERE transaction_id='txn_shutdown'")
    assert row["status"] == "PROCESSING"   # not lost
    assert row["locked_until"] is None      # immediately re-claimable
    assert row["attempts"] == 0
