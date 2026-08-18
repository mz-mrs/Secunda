from sqlalchemy.ext.asyncio import AsyncSession

from secundatest.models.outbox import Outbox
from secundatest.models.payment import Payment
from secundatest.schemas.payment import PaymentCreate


class PaymentService:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def create_payment(self, data: PaymentCreate) -> Payment:
        async with self.session.begin():
            payment = Payment(
                idempotency_key=data.idempotency_key,
                amount=data.amount,
                currency=data.currency,
                description=data.description,
                payment_metadata=data.metadata,
                webhook_url=str(data.webhook_url),
            )

            self.session.add(payment)

            # Получаем UUID payment до создания Outbox
            await self.session.flush()

            outbox = Outbox(
                payment_id=payment.id,
                event_type="payment.created",
                payload={
                    "payment_id": str(payment.id),
                    "idempotency_key": payment.idempotency_key,
                },
            )

            self.session.add(outbox)

        return payment