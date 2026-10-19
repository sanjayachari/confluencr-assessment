"""Request / response models. All input validation lives here."""
from datetime import datetime
from decimal import Decimal
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, field_validator

# IDs from payment processors: letters, digits and a few separators, max 64 chars.
ID_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9_.:\-]{0,63}$"

Identifier = Annotated[
    str, StringConstraints(strip_whitespace=True, pattern=ID_PATTERN)
]
# Money is a Decimal (never float) so we never accumulate rounding errors.
Amount = Annotated[Decimal, Field(gt=0, max_digits=18, decimal_places=2)]

TransactionStatus = Literal["PROCESSING", "PROCESSED", "FAILED"]


class WebhookPayload(BaseModel):
    """Body of POST /v1/webhooks/transactions."""

    # Unknown fields are ignored (not rejected) so the processor can add fields
    # to its payload without breaking us.
    model_config = ConfigDict(extra="ignore")

    transaction_id: Identifier
    source_account: Identifier
    destination_account: Identifier
    amount: Amount
    currency: str = Field(description="ISO 4217 alphabetic code, e.g. INR")

    @field_validator("currency", mode="before")
    @classmethod
    def _normalise_currency(cls, value: object) -> object:
        return value.strip().upper() if isinstance(value, str) else value

    @field_validator("currency")
    @classmethod
    def _check_currency(cls, value: str) -> str:
        if len(value) != 3 or not value.isascii() or not value.isalpha():
            raise ValueError("currency must be a 3-letter ISO 4217 code")
        return value


class WebhookAck(BaseModel):
    """Body of the 202 response."""

    status: Literal["ACCEPTED"] = "ACCEPTED"
    transaction_id: str
    duplicate: bool = Field(
        description="True if this transaction_id was already received earlier."
    )


class TransactionOut(BaseModel):
    """One element of the GET /v1/transactions/{id} response."""

    model_config = ConfigDict(from_attributes=True)

    transaction_id: str
    source_account: str
    destination_account: str
    amount: float
    currency: str
    status: TransactionStatus
    created_at: datetime
    processed_at: datetime | None


class HealthOut(BaseModel):
    status: Literal["HEALTHY"] = "HEALTHY"
    current_time: datetime


class ErrorOut(BaseModel):
    detail: str
