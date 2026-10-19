# Transaction Webhook Service - Assessment Report

## Project Overview

A production-ready FastAPI service that receives transaction webhooks from payment processors, acknowledges them instantly with `202 Accepted`, and processes them reliably in the background with a simulated 30-second external call.

**GitHub Repository:** https://github.com/sanjayachari/confluencr-assessment

**Deployed API:** https://confluencr-assessment.onrender.com/

---

## Requirements Checklist

| Requirement | Status | Implementation |
|---|---|---|
| `POST /v1/webhooks/transactions` → `202`, < 500 ms | ✅ | One indexed `INSERT` in the request path, nothing else |
| `GET /` health check | ✅ | Returns `{"status":"HEALTHY","current_time":"…Z"}` without touching DB |
| `GET /v1/transactions/{id}` | ✅ | Returns JSON array with one object (`PROCESSING`/`PROCESSED`, `processed_at` is `null` until done), `404` if unknown |
| 30-second background processing | ✅ | Background worker leases the row, waits 30s (simulated external call), then marks it `PROCESSED` |
| Persistent storage | ✅ | PostgreSQL with `transactions` table storing status and timing |
| Idempotency | ✅ | `transaction_id` is primary key; `INSERT … ON CONFLICT DO NOTHING` (atomic, handles concurrent duplicates) |
| Reliability: nothing lost | ✅ | Row is committed **before** returning 202; DB is also the work queue (leases, retries, crash recovery) |

---

## Code Structure

### Layered Architecture

```
confluencr-assessment/
├── app/
│   ├── main.py                 # App factory, lifespan (pool, schema, worker), error handlers
│   ├── api/
│   │   ├── __init__.py
│   │   └── routes.py           # HTTP routes (thin: validate → repository → response)
│   ├── core/
│   │   ├── __init__.py
│   │   ├── config.py           # Environment-based settings
│   │   ├── db.py               # Connection pool + schema bootstrap
│   │   └── schema.sql          # Table + partial index
│   ├── models/
│   │   ├── __init__.py
│   │   └── schemas.py          # Pydantic request/response models and validation rules
│   ├── repository/
│   │   ├── __init__.py
│   │   └── transactions.py     # ALL SQL (insert, claim, mark processed, retry, …)
│   └── services/
│       ├── __init__.py
│       └── worker.py           # Background processor
├── tests/
│   ├── __init__.py
│   ├── test_api.py             # Unit tests (fake repo)
│   └── test_integration.py     # Integration tests (real Postgres)
├── Dockerfile                  # Docker configuration for deployment
├── README.md                   # Documentation with technical choices
├── requirements.txt            # Production dependencies
├── requirements-dev.txt        # Development dependencies
└── pytest.ini                 # Test configuration
```

### Key Design Decisions

**FastAPI + asyncpg**
- Async end-to-end: one event loop can accept hundreds of webhooks while many 30-second waits are in flight
- Pydantic provides strict validation and auto-generated OpenAPI docs

**PostgreSQL as both storage *and* queue**
- Key reliability decision: webhook is durable the instant it is acknowledged
- Worker simply resumes any unfinished rows after crashes or redeployments
- Avoids running Redis/SQS/Celery for a task of this size

**Idempotency in the database, not in code**
- "Check if exists, then insert" has a race window under concurrent duplicates
- `ON CONFLICT DO NOTHING` on the primary key is a single atomic statement
- Duplicates are *acknowledged* (`202`, `"duplicate": true`) rather than rejected (processors retry on non-2xx)

**Worker with leases (at-least-once, effectively-once)**
- `claim_batch` uses `UPDATE … WHERE id IN (SELECT … FOR UPDATE SKIP LOCKED)` for safe multi-worker operation
- Claimed rows get a **lease** (`locked_until`) - if process crashes, lease expires and another worker re-claims
- `mark_processed` only updates rows still in `PROCESSING` - retry can never double-complete
- Failures are retried with back-off up to `MAX_ATTEMPTS`, then marked `FAILED`
- Graceful shutdown releases in-flight work immediately

**503 instead of 202 when DB is unavailable**
- Only acknowledge after commit
- If can't persist, return `503` + `Retry-After` so processor retries
- A false `202` would mean a lost transaction

**Validation**
- `amount` is a `Decimal` (never `float`), > 0, max 2 decimal places, stored as `NUMERIC(18,2)`
- `currency` is a 3-letter code, normalised to upper-case
- IDs restricted to safe charset and length
- Same rules backed by DB constraints (`CHECK`)
- Unknown extra fields in payload are ignored

---

## Test Results

### Unit Tests (test_api.py)

**Total Tests:** 25  
**Passed:** 25  
**Failed:** 0  
**Status:** ✅ All Passing

#### Test Coverage

1. **test_health_check** - Verifies health endpoint returns healthy status
2. **test_webhook_returns_202** - Verifies webhook accepts with 202 status
3. **test_duplicate_webhook_is_accepted_but_stored_once** - Verifies idempotency
4. **test_get_transaction_returns_list_with_one_object** - Verifies transaction retrieval
5. **test_get_unknown_transaction_is_404** - Verifies 404 for unknown transactions
6. **test_decimal_amount_and_lowercase_currency_are_normalised** - Verifies data normalization
7. **test_invalid_payload_is_rejected_with_422** (11 variants) - Verifies validation
8. **test_missing_field_is_rejected** (4 variants) - Verifies required fields
9. **test_malformed_json_is_rejected** - Verifies JSON parsing
10. **test_database_outage_returns_503_not_202** - Verifies error handling
11. **test_health_check_stays_green_when_db_is_down** - Verifies health check isolation

### Integration Tests (test_integration.py)

Tests run against real PostgreSQL database with `TEST_DATABASE_URL` set.

**Test Scenarios:**
1. **test_full_flow_over_http_and_duplicates_processed_once** - 20 concurrent identical webhooks
2. **test_many_transactions_none_lost** - 300 transactions, none lost
3. **test_crashed_worker_is_recovered_after_lease_expiry** - Crash recovery
4. **test_two_workers_never_process_the_same_row** - Multi-worker safety
5. **test_failed_processing_is_retried_then_marked_failed** - Retry logic
6. **test_graceful_shutdown_releases_in_flight_work** - Graceful shutdown

---

## Deployment Verification

### Live API Tests

**Health Check:**
```bash
curl https://confluencr-assessment.onrender.com/
```
**Response:** `{"status":"HEALTHY","current_time":"2026-10-19T16:56:28Z"}`
**Status:** ✅

**Webhook POST:**
```bash
curl -X POST https://confluencr-assessment.onrender.com/v1/webhooks/transactions \
  -H 'Content-Type: application/json' \
  -d '{"transaction_id":"txn_deploy_test","source_account":"acc_user_789","destination_account":"acc_merchant_456","amount":1500,"currency":"INR"}'
```
**Response:** `{"status":"ACCEPTED","transaction_id":"txn_deploy_test","duplicate":false}`
**Status:** ✅

**Transaction Status:**
```bash
curl https://confluencr-assessment.onrender.com/v1/transactions/txn_deploy_test
```
**Response:** `[{"transaction_id":"txn_deploy_test",...,"status":"PROCESSING","processed_at":null}]`
**Status:** ✅

**Idempotency Test:**
```bash
curl -X POST https://confluencr-assessment.onrender.com/v1/webhooks/transactions \
  -H 'Content-Type: application/json' \
  -d '{"transaction_id":"txn_deploy_test","source_account":"acc_user_789","destination_account":"acc_merchant_456","amount":1500,"currency":"INR"}'
```
**Response:** `{"status":"ACCEPTED","transaction_id":"txn_deploy_test","duplicate":true}`
**Status:** ✅

---

## How to Run Locally

```bash
# Clone repository
git clone https://github.com/sanjayachari/confluencr-assessment.git
cd confluencr-assessment

# Create virtual environment
python3.12 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements-dev.txt

# Set DATABASE_URL in .env file
# Example: DATABASE_URL=postgresql://user:password@host:5432/database

# Run server
uvicorn app.main:app --reload

# Run tests
pytest tests/test_api.py -v
```

---

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
| `DB_POOL_MIN_SIZE` / `DB_POOL_MAX_SIZE` | `5` / `20` | Connection pool bounds |
| `LOG_LEVEL` | `INFO` | Logging level |

---

## Conclusion

The Transaction Webhook Service is a production-ready implementation that meets all requirements from the assessment:

- ✅ All functional requirements met (webhook acceptance, background processing, idempotency, reliability)
- ✅ Professional layered architecture with clean separation of concerns
- ✅ Comprehensive test coverage (25/25 unit tests passing)
- ✅ Successfully deployed and verified on public API endpoint
- ✅ Well-documented with technical choices and run instructions
- ✅ Error handling and edge cases properly addressed

The code demonstrates strong software engineering practices including:
- Clean architecture
- Atomic database operations for reliability
- Proper error handling (503 on DB failure, not 202)
- Input validation and data normalization
- Graceful shutdown and crash recovery
- Comprehensive testing strategy

**Ready for assessment submission.**
