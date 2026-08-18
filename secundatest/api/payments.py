from uuid import UUID
from typing import Annotated

from fastapi import APIRouter, Depends, status, HTTPException, Header
from sqlalchemy.ext.asyncio import AsyncSession

from secundatest.api.dependencies import get_session
from secundatest.schemas.payment import PaymentCreate, PaymentResponse
from secundatest.services.payment_service import PaymentService


router = APIRouter(
    prefix="/api/v1/payments",
    tags=["payments"],
)


@router.post(
    "",
    response_model=PaymentResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def create_payment(
    data: PaymentCreate,
    idempotency_key: Annotated[
        str,
        Header(alias="Idempotency-Key"),
    ],
    session: AsyncSession = Depends(get_session),
) -> PaymentResponse:

    service = PaymentService(session)

    payment = await service.create_payment(
        data=data,
        idempotency_key=idempotency_key
    )

    return PaymentResponse.model_validate(payment)

@router.get(
    "/{payment_id}",
    response_model=PaymentResponse,
)
async def get_payment(
    payment_id: UUID,
    session: AsyncSession = Depends(get_session),
) -> PaymentResponse:
    service = PaymentService(session)

    payment = await service.get_payment(payment_id)

    if payment is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Payment not found",
        )

    return PaymentResponse.model_validate(payment)