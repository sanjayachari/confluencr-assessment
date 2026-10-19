# Transaction Webhook Service

A FastAPI service that receives transaction webhooks from payment processors,
acknowledges them instantly with `202 Accepted`, and processes them reliably
in the background (with a simulated 30-second external call).

---

## Requirements checklist

| Requirement | How it is met |
|---|---|
| `POST /v1/webhooks/transactions` → `202`, < 500 ms | One indexed `INSERT` in the request path, nothing else. |
| `GET /` health check | Returns `{"status":"HEALTHY","current_time":"…Z"}`. Does not touch the DB. |
| `GET /v1/transactions/{id}` | Returns a JSON array with one object (`PROCESSING` / `PROCESSED`, `processed_at` is `null` until done). `404` if unknown. |
| 30-second background processing | Background worker leases the row, waits 30 s (simulated external call), then marks it `PROCESSED`. |
| Persistent storage | PostgreSQL. The `transactions` table stores status and timing. |
| Idempotency | `transaction_id` is the primary key; `INSERT … ON CONFLICT DO NOTHING`. Atomic, so it holds even for concurrent duplicates. |
| Reliability: nothing lost | Row is committed **before** we return 202; the DB is also the work queue (leases, retries, crash recovery). |

---

## Technical choices

**FastAPI + asyncpg.** Async end-to-end: one event loop can accept hundreds of
webhooks while many 30-second waits are in flight (a sleeping asyncio task costs
almost nothing, unlike a sleeping thread). Pydantic gives strict validation and
auto-generated OpenAPI docs.

**PostgreSQL as both storage *and* queue.** The key reliability decision.
A common shortcut is FastAPI's `BackgroundTasks` or an in-memory queue, but those
are lost if the process restarts or is redeployed mid-delay, which would silently
drop acknowledged transactions. Here the webhook is durable the instant it is
acknowledged, and the worker simply resumes any unfinished rows. This avoids
running Redis/SQS/Celery for a task of this size.

**Idempotency in the database, not in code.** "Check if exists, then insert" has
a race window under concurrent duplicates. `ON CONFLICT DO NOTHING` on the
primary key is a single atomic statement, so exactly one request wins.
Duplicates are *acknowledged* (`202`, `"duplicate": true`) rather than rejected:
processors retry on non-2xx, so an error would cause even more retries.

**Worker with leases (at-least-once, effectively-once).**
- `claim_batch` uses `UPDATE … WHERE id IN (SELECT … FOR UPDATE SKIP LOCKED)`,
  so several workers/instances can run safely and never take the same row.
- A claimed row gets a **lease** (`locked_until`). If the process crashes, the
  lease expires and another worker re-claims the row. Nothing is stuck forever.
- `mark_processed` only updates rows still in `PROCESSING`, so a retry can never
  double-complete a transaction.
- Failures are retried with back-off up to `MAX_ATTEMPTS`, then marked `FAILED`
  with `last_error` recorded (no infinite retry loop).
- On graceful shutdown (deploys) in-flight rows are released immediately.

**503 instead of 202 when the DB is unavailable.** We only acknowledge after the
commit. If we can't persist, we return `503` + `Retry-After` so the processor
retries. A false `202` would mean a lost transaction.

**Validation.** `amount` is a `Decimal` (never `float`), > 0, max 2 decimal
places, stored as `NUMERIC(18,2)`; `currency` is a 3-letter code, normalised to
upper-case; IDs are restricted to a safe charset and length. The same rules are
backed by DB constraints (`CHECK`). Unknown extra fields in the payload are
ignored so the processor can evolve its schema without breaking us.

---

## Project layout

```
app/
  main.py         app factory, lifespan (pool, schema, worker), error handlers
  api.py          routes (thin: validate → repository → response)
  schemas.py      Pydantic request/response models and all validation rules
  repository.py   ALL SQL (insert, claim, mark processed, retry, …)
  worker.py       background processor
  db.py           connection pool + schema bootstrap
  config.py       env-based settings
  schema.sql      table + partial index
tests/            unit tests (fake repo) + integration tests (real Postgres)
```

---

## API

```bash
# Health
curl http://localhost:8000/

# Send a webhook  ->  202
curl -i -X POST http://localhost:8000/v1/webhooks/transactions \
  -H 'Content-Type: application/json' \
  -d '{"transaction_id":"txn_abc123def456","source_account":"acc_user_789",
       "destination_account":"acc_merchant_456","amount":1500,"currency":"INR"}'
# {"status":"ACCEPTED","transaction_id":"txn_abc123def456","duplicate":false}

# Status (PROCESSING for ~30 s, then PROCESSED)
curl http://localhost:8000/v1/transactions/txn_abc123def456
# [{"transaction_id":"txn_abc123def456","source_account":"acc_user_789",
#   "destination_account":"acc_merchant_456","amount":1500.0,"currency":"INR",
#   "status":"PROCESSED","created_at":"…Z","processed_at":"…Z"}]
```

Status codes: `202` accepted (including duplicates) · `422` invalid payload ·
`404` unknown transaction · `503` database unavailable (retry).

---

## Run locally

Requires Python 3.11+ and a PostgreSQL database.

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt

# Set DATABASE_URL in .env file
uvicorn app.main:app --reload
# Open http://localhost:8000/docs
```

The table is created automatically on start-up (`app/schema.sql`).

## Test

```bash
# Unit/API tests: no database needed
pytest tests/test_api.py

# Everything, including real-Postgres integration tests
TEST_DATABASE_URL=postgresql://user:pass@localhost:5432/testdb pytest
```

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `DATABASE_URL` | required | Postgres connection string |
| `PROCESSING_DELAY_SECONDS` | `30` | Simulated external-call duration |
| `LEASE_SECONDS` | `120` | Crash-recovery lease (must exceed the delay) |
| `MAX_ATTEMPTS` | `5` | Attempts before a transaction is marked `FAILED` |
| `RETRY_BACKOFF_SECONDS` | `5` | Back-off base after a failed attempt |
| `WORKER_POLL_INTERVAL_SECONDS` | `0.5` | Poll interval |
| `WORKER_MAX_IN_FLIGHT` | `1000` | Max concurrent in-progress transactions |
| `DB_POOL_MIN_SIZE` / `DB_POOL_MAX_SIZE` | `5` / `10` | Connection pool bounds |
| `LOG_LEVEL` | `INFO` | Logging level |
