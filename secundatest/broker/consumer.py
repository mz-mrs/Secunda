import asyncio
import random
import logging

from datetime import datetime, timezone

from faststream.rabbit import RabbitRouter
from sqlalchemy import select

from secundatest.broker.broker import broker
from secundatest.broker.publisher import publish_outbox
from secundatest.core.logger import setup_logging
from secundatest.db.session import async_session_factory
from secundatest.enums import PaymentStatus
from secundatest.models import Payment
from secundatest.services import WebhookService



router = RabbitRouter()
broker.include_router(router)
webhook_service = WebhookService()

logger = logging.getLogger("secundatest.broker.consumer")

PUBLISH_INTERVAL = 1
MAX_ATTEMPTS = 3


@router.subscriber("payments.new")
async def payment_processing(message: dict) -> None:
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
