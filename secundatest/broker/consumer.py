import asyncio
import random
import logging

from datetime import datetime, timezone

from faststream.rabbit import RabbitRouter
from sqlalchemy import select

from secundatest.broker.broker import broker, PAYMENTS_QUEUE
from secundatest.core.logger import setup_logging
from secundatest.db.session import async_session_factory
from secundatest.enums import PaymentStatus, OutboxStatus
from secundatest.models import Outbox, Payment
from secundatest.services import WebhookService


router = RabbitRouter()
webhook_service = WebhookService()

logger = logging.getLogger("secundatest.broker.consumer")

PUBLISH_INTERVAL = 1
MAX_ATTEMPTS = 3


@router.subscriber("payments.new")
async def process_payment(message: dict) -> None:
    payment_id = message["payment_id"]

    logger.info(f"Получена операция из очереди payments.new {payment_id=}")

    async with async_session_factory() as session:
        payment = await session.scalar(
            select(Payment).where(Payment.id == payment_id)
        )

        if payment is None:
            logger.warning(f"Платеж {payment_id=} не найден")
            return

        if payment.status != PaymentStatus.PENDING:
            logger.info(f"Платеж {payment_id=} уже в обработке status={payment.status.value}")
            return

        logger.info(f"Начало обработки платежа {payment_id=}")

        await asyncio.sleep(random.uniform(2, 5)) # имитация

        success = random.random() < 0.9

        if success:
            payment.status = PaymentStatus.SUCCEEDED
        else:
            payment.status = PaymentStatus.FAILED

        payment.processed_at = datetime.now(timezone.utc)

        await session.commit()

        logger.info(f"Обработка платежа {payment_id=} завершена status={payment.status.value}")

        if payment.webhook_url:
            payload = {
                "payment_id": str(payment.id),
                "status": payment.status.value,
                "amount": str(payment.amount),
                "currency": payment.currency.value,
            }

            logger.info(f"Платеж {payment_id=}, отправка вебхука url={payment.webhook_url}")

            webhook = await webhook_service.send(
                webhook_url=payment.webhook_url,
                payload=payload,
            )

            if webhook:
                logger.info(f"Успешная отправка вебхука {payment_id=}")
            else:
                logger.error(f"Неудачная отправка вебхука {payment_id=}")


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

                    if events:
                        logger.info(f"Обнаружено {len(events)} событий для отправки")

                    for event in events:
                        event.status = OutboxStatus.PROCESSING
                        event.attempts += 1

                        logger.info(f"Событие {event.id} в обработке, попытка {event.attempts}")

                for event in events:
                    try:
                        await broker.publish(
                            event.payload,
                            queue=PAYMENTS_QUEUE,
                        )

                        logger.info(f"Событие {event.id} опубликовано")

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
                        logger.exception(f"Ошибка отправки события {event.id}, попытка {event.attempts}")

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

                                    logger.error(f"Ошибка отправки события {event.id}, попытка {db_event.attempts}")

                                else:
                                    db_event.status = OutboxStatus.PENDING

                                    logger.warning(f"Исчерпано количество попыток {db_event.attempts} после отправки события {event.id}")

        except Exception as exc:
            logger.exception(f"Ошибка публикации {exc}")

        await asyncio.sleep(PUBLISH_INTERVAL)


broker.include_router(router)


async def main() -> None:
    setup_logging()

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
