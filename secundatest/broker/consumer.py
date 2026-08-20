import asyncio
import random
from datetime import datetime, timezone

from faststream.rabbit import RabbitRouter
from sqlalchemy import select

from secundatest.broker.broker import broker, PAYMENTS_QUEUE
from secundatest.db.session import async_session_factory
from secundatest.enums import PaymentStatus, OutboxStatus
from secundatest.models import Outbox, Payment



router = RabbitRouter()


@router.subscriber("payments.new")
async def process_payment(message: dict) -> None:
    payment_id = message["payment_id"]

    async with async_session_factory() as session:
        payment = await session.scalar(
            select(Payment).where(Payment.id == payment_id)
        )

        if payment is None:
            return

        if payment.status != PaymentStatus.PENDING:
            return

        await asyncio.sleep(random.uniform(2, 5))

        if random.random() < 0.9:
            payment.status = PaymentStatus.SUCCEEDED
        else:
            payment.status = PaymentStatus.FAILED

        payment.processed_at = datetime.now(timezone.utc)

        await session.commit()

PUBLISH_INTERVAL = 1
MAX_ATTEMPTS = 3


async def publish_outbox() -> None:
    while True:
        try:
            async with async_session_factory() as session:
                async with session.begin():
                    result = await session.execute(
                        select(Outbox)
                        .where(
                            Outbox.status == OutboxStatus.PENDING,
                        )
                        .order_by(Outbox.created_at)
                        .limit(10)
                        .with_for_update(skip_locked=True)
                    )

                    events = result.scalars().all()

                    for event in events:
                        event.status = OutboxStatus.PROCESSING
                        event.attempts += 1

                for event in events:
                    try:
                        await broker.publish(
                            event.payload,
                            queue=PAYMENTS_QUEUE,
                        )

                        async with async_session_factory() as update_session:
                            async with update_session.begin():
                                db_event = await update_session.get(
                                    Outbox,
                                    event.id,
                                )

                                if db_event is not None:
                                    db_event.status = OutboxStatus.PUBLISHED
                                    db_event.published_at = datetime.now(
                                        timezone.utc
                                    )

                    except Exception:
                        async with async_session_factory() as update_session:
                            async with update_session.begin():
                                db_event = await update_session.get(
                                    Outbox,
                                    event.id,
                                )

                                if db_event is None:
                                    continue

                                if db_event.attempts >= MAX_ATTEMPTS:
                                    db_event.status = OutboxStatus.FAILED
                                else:
                                    db_event.status = OutboxStatus.PENDING

        except Exception as exc:
            print(f"Outbox publisher error: {exc}")

        await asyncio.sleep(PUBLISH_INTERVAL)


broker.include_router(router)


async def main() -> None:
    await broker.start()

    publisher_task = asyncio.create_task(
        publish_outbox()
    )

    try:
        await asyncio.Event().wait()
    finally:
        publisher_task.cancel()

        await broker.stop()


if __name__ == "__main__":
    asyncio.run(main())
