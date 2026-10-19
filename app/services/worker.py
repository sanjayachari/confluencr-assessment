"""Background processor.

Runs inside the web process as an asyncio task. It polls the database for
unfinished transactions, leases them, performs the (simulated) slow external
call, and records the result. See README for the reliability model.
"""
import asyncio
import logging

from app.core.config import Settings
from app.repository.transactions import ClaimedTransaction, TransactionRepository

logger = logging.getLogger(__name__)


class BackgroundProcessor:
    def __init__(self, repo: TransactionRepository, settings: Settings) -> None:
        self._repo = repo
        self._s = settings
        self._wake = asyncio.Event()
        self._runner: asyncio.Task | None = None
        self._in_flight: dict[str, asyncio.Task] = {}

    # ----------------------------------------------------------- lifecycle

    def start(self) -> None:
        self._runner = asyncio.create_task(self._run(), name="transaction-poller")
        logger.info("Background processor started")

    async def stop(self) -> None:
        if self._runner:
            self._runner.cancel()
            await asyncio.gather(self._runner, return_exceptions=True)

        pending = dict(self._in_flight)
        for task in pending.values():
            task.cancel()
        await asyncio.gather(*pending.values(), return_exceptions=True)

        # Hand unfinished work back right away instead of waiting for leases
        # to expire. Best effort: if this fails the lease is the safety net.
        try:
            await self._repo.release_locks(list(pending))
        except Exception:
            logger.warning("Could not release locks on shutdown", exc_info=True)
        logger.info("Background processor stopped (%d in flight)", len(pending))

    def wake(self) -> None:
        """Nudge the poller so new work starts immediately, not on next poll."""
        self._wake.set()

    # ---------------------------------------------------------------- loop

    async def _run(self) -> None:
        while True:
            claimed = 0
            try:
                claimed = await self._tick()
            except asyncio.CancelledError:
                raise
            except Exception:
                # DB hiccup etc. Never let the poller die.
                logger.exception("Poller iteration failed; will retry")

            if claimed >= self._s.worker_batch_size:
                continue  # backlog: keep draining without sleeping
            try:
                await asyncio.wait_for(
                    self._wake.wait(), timeout=self._s.worker_poll_interval_seconds
                )
            except asyncio.TimeoutError:
                pass
            self._wake.clear()

    async def _tick(self) -> int:
        failed = await self._repo.fail_exhausted(self._s.max_attempts)
        if failed:
            logger.error("Marked %d transaction(s) FAILED after max attempts", failed)

        capacity = self._s.worker_max_in_flight - len(self._in_flight)
        if capacity <= 0:
            return 0

        batch = await self._repo.claim_batch(
            limit=min(capacity, self._s.worker_batch_size),
            lease_seconds=self._s.lease_seconds,
            max_attempts=self._s.max_attempts,
        )
        for item in batch:
            task = asyncio.create_task(self._process(item), name=f"txn-{item.transaction_id}")
            self._in_flight[item.transaction_id] = task
            task.add_done_callback(
                lambda _t, tid=item.transaction_id: self._in_flight.pop(tid, None)
            )
        return len(batch)

    # ------------------------------------------------------------- one job

    async def _process(self, item: ClaimedTransaction) -> None:
        tid = item.transaction_id
        try:
            await self._call_external_service(tid)
            await self._repo.mark_processed(tid)
            logger.info("Processed %s (attempt %d)", tid, item.attempts)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.exception("Processing %s failed (attempt %d)", tid, item.attempts)
            try:
                await self._repo.release_for_retry(
                    tid, repr(exc), self._s.retry_backoff_seconds * item.attempts
                )
            except Exception:
                # Lease expiry will make the row claimable again.
                logger.warning("Could not release %s for retry", tid, exc_info=True)

    async def _call_external_service(self, transaction_id: str) -> None:
        """Stand-in for a slow third-party call (bank / ledger / fraud check)."""
        await asyncio.sleep(self._s.processing_delay_seconds)
