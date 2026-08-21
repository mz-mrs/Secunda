import asyncio
import random
import logging

from datetime import datetime, timezone

from faststream.rabbit import RabbitRouter
from sqlalchemy import select

from secundatest.broker.broker import (
    broker,
    payments_dlq,
    payments_retry_queue,
    payments_dlx,
)
from secundatest.broker.publisher import publish_from_outbox
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
    attempt = message.get("attempt", 1)

    logger.info(
        f"Получена операция из очереди payments.new " f"{payment_id=} {attempt=}"
    )

    async with async_session_factory() as session:
        payment = await session.scalar(select(Payment).where(Payment.id == payment_id))

        if payment is None:
            logger.warning(f"Платеж {payment_id=} не найден")
            return

        if payment.status != PaymentStatus.PENDING:
            logger.info(
                f"Платеж {payment_id=} уже в обработке status={payment.status.value}"
            )
            return

        logger.info(f"Начало обработки платежа {payment_id=}")

        await asyncio.sleep(random.uniform(2, 5))  # имитация

        success = random.random() < 0.9
        # success = False  # проверка 3 неудачных попыток для DLQ

        if not success:
            logger.error(
                f"Ошибка обработки платежа "
                f"{payment_id=}, попытка={attempt}/{MAX_ATTEMPTS}"
            )

            if attempt >= MAX_ATTEMPTS:
                payment.status = PaymentStatus.FAILED
                payment.processed_at = datetime.now(timezone.utc)

                await session.commit()

                logger.error(
                    f"Исчерпаны попытки обработки "
                    f"{payment_id=}. Отправка сообщения в DLQ"
                )

                dlq_message = {
                    **message,
                    "attempt": attempt,
                    "error": "payment processing failed",
                }

                await broker.publish(
                    dlq_message,
                    queue=payments_dlq,
                    persist=True,
                )

                return

            retry_message = {
                **message,
                "attempt": attempt + 1,
            }

            await broker.publish(
                retry_message,
                queue=payments_retry_queue,
                persist=True,
            )

            logger.warning(
                f"Платеж {payment_id=} отправлен на повторную обработку. "
                f"Следующая попытка={attempt + 1}"
            )

            return

        payment.status = PaymentStatus.SUCCEEDED
        payment.processed_at = datetime.now(timezone.utc)

        await session.commit()

        logger.info(
            f"Обработка платежа {payment_id=} завершена. "
            f"Статус {payment.status.value}"
        )

        if payment.webhook_url:
            payload = {
                "payment_id": str(payment.id),
                "status": payment.status.value,
                "amount": str(payment.amount),
                "currency": payment.currency.value,
            }

            logger.info(
                f"Платеж {payment_id=}, отправка вебхука url={payment.webhook_url}"
            )

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

    await broker.declare_exchange(payments_dlx)
    await broker.declare_queue(payments_retry_queue)
    await broker.declare_queue(payments_dlq)

    publisher_task = asyncio.create_task(publish_from_outbox())

    try:
        await asyncio.Event().wait()
    finally:
        publisher_task.cancel()

        await broker.stop()


if __name__ == "__main__":
    asyncio.run(main())
