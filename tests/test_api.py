"""API-level tests. Use an in-memory fake repository, so no database is needed."""
from datetime import datetime, timezone

import asyncpg
import pytest
from fastapi.testclient import TestClient

from app.main import create_app

VALID = {
    "transaction_id": "txn_abc123def456",
    "source_account": "acc_user_789",
    "destination_account": "acc_merchant_456",
    "amount": 1500,
    "currency": "INR",
}


class FakeRepo:
    def __init__(self) -> None:
        self.rows: dict[str, dict] = {}
        self.fail = False

    async def insert_if_absent(self, p) -> bool:
        if self.fail:
            raise asyncpg.PostgresConnectionError("db down")
        if p.transaction_id in self.rows:
            return False
        self.rows[p.transaction_id] = {
            **p.model_dump(),
            "status": "PROCESSING",
            "created_at": datetime.now(timezone.utc),
            "processed_at": None,
        }
        return True

    async def get(self, tid):
        return self.rows.get(tid)

    async def ping(self) -> None:
        if self.fail:
            raise asyncpg.PostgresConnectionError("db down")


@pytest.fixture
def repo() -> FakeRepo:
    return FakeRepo()


@pytest.fixture
def client(repo):
    with TestClient(create_app(repository=repo, run_worker=False)) as c:
        yield c


def test_health_check(client):
    r = client.get("/")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "HEALTHY"
    assert body["current_time"].endswith("Z")


def test_webhook_returns_202(client):
    r = client.post("/v1/webhooks/transactions", json=VALID)
    assert r.status_code == 202
    assert r.json() == {"status": "ACCEPTED", "transaction_id": VALID["transaction_id"], "duplicate": False}


def test_duplicate_webhook_is_accepted_but_stored_once(client, repo):
    first = client.post("/v1/webhooks/transactions", json=VALID)
    second = client.post("/v1/webhooks/transactions", json=VALID)
    third = client.post("/v1/webhooks/transactions", json=VALID)
    assert [first.status_code, second.status_code, third.status_code] == [202, 202, 202]
    assert first.json()["duplicate"] is False
    assert second.json()["duplicate"] is True
    assert len(repo.rows) == 1


def test_get_transaction_returns_list_with_one_object(client):
    client.post("/v1/webhooks/transactions", json=VALID)
    r = client.get(f"/v1/transactions/{VALID['transaction_id']}")
    assert r.status_code == 200
    body = r.json()
    assert isinstance(body, list) and len(body) == 1
    txn = body[0]
    assert txn["status"] == "PROCESSING"
    assert txn["processed_at"] is None
    assert txn["amount"] == 1500
    assert txn["currency"] == "INR"
    assert set(txn) == {
        "transaction_id", "source_account", "destination_account", "amount",
        "currency", "status", "created_at", "processed_at",
    }


def test_get_unknown_transaction_is_404(client):
    assert client.get("/v1/transactions/txn_nope").status_code == 404


def test_decimal_amount_and_lowercase_currency_are_normalised(client):
    r = client.post("/v1/webhooks/transactions", json={**VALID, "amount": 150.50, "currency": "usd"})
    assert r.status_code == 202
    txn = client.get(f"/v1/transactions/{VALID['transaction_id']}").json()[0]
    assert txn["amount"] == 150.5
    assert txn["currency"] == "USD"


@pytest.mark.parametrize(
    "override",
    [
        {"amount": 0},
        {"amount": -5},
        {"amount": "abc"},
        {"amount": 10.123},            # more than 2 decimal places
        {"currency": "RUPEES"},
        {"currency": "I1R"},
        {"currency": ""},
        {"transaction_id": ""},
        {"transaction_id": "has space"},
        {"transaction_id": "x" * 65},
        {"source_account": None},
    ],
)
def test_invalid_payload_is_rejected_with_422(client, repo, override):
    r = client.post("/v1/webhooks/transactions", json={**VALID, **override})
    assert r.status_code == 422
    assert repo.rows == {}


@pytest.mark.parametrize("missing", list(VALID))
def test_missing_field_is_rejected(client, missing):
    body = {k: v for k, v in VALID.items() if k != missing}
    assert client.post("/v1/webhooks/transactions", json=body).status_code == 422


def test_malformed_json_is_rejected(client):
    r = client.post(
        "/v1/webhooks/transactions", content="{not json", headers={"Content-Type": "application/json"}
    )
    assert r.status_code == 422


def test_database_outage_returns_503_not_202(client, repo):
    """If we cannot persist, we must NOT acknowledge: the sender has to retry."""
    repo.fail = True
    r = client.post("/v1/webhooks/transactions", json=VALID)
    assert r.status_code == 503
    assert r.headers["Retry-After"] == "5"


def test_health_check_stays_green_when_db_is_down(client, repo):
    repo.fail = True
    assert client.get("/").status_code == 200
    assert client.get("/ready").status_code == 503
