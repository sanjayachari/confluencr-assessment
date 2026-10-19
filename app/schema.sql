-- Idempotent schema; executed on application start-up.
-- Also safe to run manually in the Supabase SQL editor.

CREATE TABLE IF NOT EXISTS transactions (
    -- Primary key doubles as the idempotency key: one row per transaction_id.
    transaction_id       TEXT PRIMARY KEY,
    source_account       TEXT           NOT NULL,
    destination_account  TEXT           NOT NULL,
    amount               NUMERIC(18, 2) NOT NULL CHECK (amount > 0),
    currency             CHAR(3)        NOT NULL,
    status               TEXT           NOT NULL DEFAULT 'PROCESSING'
                         CHECK (status IN ('PROCESSING', 'PROCESSED', 'FAILED')),

    -- Work-queue bookkeeping (see app/repository.py)
    attempts             INTEGER        NOT NULL DEFAULT 0,
    locked_until         TIMESTAMPTZ,
    last_error           TEXT,

    created_at           TIMESTAMPTZ    NOT NULL DEFAULT now(),
    processed_at         TIMESTAMPTZ
);

-- Partial index: only unfinished rows, so the worker's claim query stays cheap
-- no matter how many processed transactions accumulate.
CREATE INDEX IF NOT EXISTS idx_transactions_pending
    ON transactions (created_at)
    WHERE status = 'PROCESSING';
