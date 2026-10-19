"""All SQL lives here. The rest of the app never touches the database directly.

The `transactions` table is both the system of record and the work queue.
Using Postgres as the queue (instead of an in-memory task) means a webhook that
has been acknowledged with 202 is already durable: if the process crashes or is
redeployed, another worker simply picks the row up again once its lease expires.
"""
from dataclasses import dataclass
from typing import Any

import asyncpg

from app.schemas import WebhookPayload


@dataclass(frozen=True)
class ClaimedTransaction:
    transaction_id: str
    attempts: int


class TransactionRepository:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    # ------------------------------------------------------------------ API

    async def insert_if_absent(self, payload: WebhookPayload) -> bool:
        """Persist the webhook. Returns True if inserted, False if duplicate.

        `ON CONFLICT DO NOTHING` on the primary key makes idempotency atomic:
        even if 100 identical webhooks arrive concurrently, exactly one INSERT
        wins and the rest are no-ops. No read-then-write race.
        """
        row = await self._pool.fetchrow(
            """
            INSERT INTO transactions
                (transaction_id, source_account, destination_account, amount, currency)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (transaction_id) DO NOTHING
            RETURNING transaction_id
            """,
            payload.transaction_id,
            payload.source_account,
            payload.destination_account,
            payload.amount,
            payload.currency,
        )
        return row is not None

    async def get(self, transaction_id: str) -> dict[str, Any] | None:
        row = await self._pool.fetchrow(
            """
            SELECT transaction_id, source_account, destination_account, amount,
                   currency, status, created_at, processed_at
            FROM transactions
            WHERE transaction_id = $1
            """,
            transaction_id,
        )
        return dict(row) if row else None

    async def ping(self) -> None:
        await self._pool.fetchval("SELECT 1")

    # --------------------------------------------------------------- Worker

    async def claim_batch(
        self, limit: int, lease_seconds: float, max_attempts: int
    ) -> list[ClaimedTransaction]:
        """Atomically lease up to `limit` unfinished transactions.

        `FOR UPDATE SKIP LOCKED` lets several workers/instances poll the same
        table without blocking each other or claiming the same row twice.
        A row whose lease has expired (worker crashed) becomes claimable again.
        """
        rows = await self._pool.fetch(
            """
            UPDATE transactions
            SET locked_until = now() + make_interval(secs => $2),
                attempts = attempts + 1
            WHERE transaction_id IN (
                SELECT transaction_id
                FROM transactions
                WHERE status = 'PROCESSING'
                  AND attempts < $3
                  AND (locked_until IS NULL OR locked_until < now())
                ORDER BY created_at
                LIMIT $1
                FOR UPDATE SKIP LOCKED
            )
            RETURNING transaction_id, attempts
            """,
            limit,
            lease_seconds,
            max_attempts,
        )
        return [ClaimedTransaction(r["transaction_id"], r["attempts"]) for r in rows]

    async def mark_processed(self, transaction_id: str) -> bool:
        """PROCESSING -> PROCESSED. The status guard makes this idempotent."""
        result = await self._pool.execute(
            """
            UPDATE transactions
            SET status = 'PROCESSED', processed_at = now(),
                locked_until = NULL, last_error = NULL
            WHERE transaction_id = $1 AND status = 'PROCESSING'
            """,
            transaction_id,
        )
        return result.endswith(" 1")

    async def release_for_retry(
        self, transaction_id: str, error: str, delay_seconds: float
    ) -> None:
        """Processing failed: record why and retry after a back-off."""
        await self._pool.execute(
            """
            UPDATE transactions
            SET locked_until = now() + make_interval(secs => $3), last_error = $2
            WHERE transaction_id = $1 AND status = 'PROCESSING'
            """,
            transaction_id,
            error[:500],
            delay_seconds,
        )

    async def fail_exhausted(self, max_attempts: int) -> int:
        """Give up on rows that have used all attempts (poison messages)."""
        result = await self._pool.execute(
            """
            UPDATE transactions
            SET status = 'FAILED', locked_until = NULL
            WHERE status = 'PROCESSING'
              AND attempts >= $1
              AND (locked_until IS NULL OR locked_until < now())
            """,
            max_attempts,
        )
        return int(result.split()[-1])

    async def release_locks(self, transaction_ids: list[str]) -> None:
        """Graceful shutdown: hand in-flight work back immediately."""
        if not transaction_ids:
            return
        await self._pool.execute(
            """
            UPDATE transactions
            SET locked_until = NULL, attempts = GREATEST(attempts - 1, 0)
            WHERE transaction_id = ANY($1::text[]) AND status = 'PROCESSING'
            """,
            transaction_ids,
        )
