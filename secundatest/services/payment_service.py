from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import IntegrityError
from sqlalchemy.dialects.postgresql import insert

from secundatest.models.outbox import Outbox
from secundatest.models.payment import Payment
from secundatest.schemas.payment import PaymentCreate


class PaymentService:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def create_payment(
            self,
            data: PaymentCreate,
            idempotency_key: str,
    ) -> Payment:
        async with self.session.begin():
            stmt = (
                insert(Payment)
                .values(
                    idempotency_key=idempotency_key,
                    amount=data.amount,
                    currency=data.currency,
                    description=data.description,
                    payment_metadata=data.metadata,
                    webhook_url=str(data.webhook_url),
                )
                .on_conflict_do_nothing(
                    index_elements=[Payment.idempotency_key]
                )
                .returning(Payment)
            )

            result = await self.session.execute(stmt)
            payment = result.scalar_one_or_none()

            if payment is None:
                payment = await self.session.scalar(
                    select(Payment).where(
                        Payment.idempotency_key == idempotency_key
                    )
                )

                if payment is None:
                    raise RuntimeError(
                        "Платеж не может быть создан"
                    )

                return payment

            outbox = Outbox(
                payment_id=payment.id,
                event_type="payment.created",
                payload={
                    "payment_id": str(payment.id),
                    "idempotency_key": payment.idempotency_key,
                    "webhook_url": str(payment.webhook_url),
                },
            )

            self.session.add(outbox)

        return payment


    async def get_payment(self, payment_id: UUID) -> Payment | None:
        result = await self.session.execute(
            select(Payment).where(Payment.id == payment_id)
        )

        return result.scalar_one_or_none()