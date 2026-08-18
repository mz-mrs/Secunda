from decimal import Decimal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, HttpUrl

from secundatest.enums import Currency, PaymentStatus


class PaymentCreate(BaseModel):
    idempotency_key: str = Field(min_length=1, max_length=255)
    amount: Decimal = Field(gt=0)
    currency: Currency
    description: str = Field(min_length=1, max_length=1000)
    metadata: dict[str, str] = Field(default_factory=dict)
    webhook_url: HttpUrl


class PaymentResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    idempotency_key: str
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, str] = Field(validation_alias="payment_metadata")
    status: PaymentStatus
    webhook_url: HttpUrl