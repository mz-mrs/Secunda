from fastapi import APIRouter, Depends, status
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
    session: AsyncSession = Depends(get_session),
) -> PaymentResponse:

    service = PaymentService(session)

    payment = await service.create_payment(data)

    return PaymentResponse.model_validate(payment)