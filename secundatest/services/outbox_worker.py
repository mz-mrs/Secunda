import asyncio
import logging

from secundatest.broker.publisher import publish_payment_event
from secundatest.db.session import async_session_factory
from secundatest.services.outbox_service import OutboxService


logger = logging.getLogger(__name__)

POLL_INTERVAL = 1


async def process_outbox() -> None:
    while True:
        try:
            async with async_session_factory() as session:
                service = OutboxService(session)

                async with session.begin():
                    outbox = await service.get_pending_outbox_event()

                    if outbox is not None:
                        await service.mark_outbox_event_processing(outbox)

                if outbox is None:
                    await asyncio.sleep(POLL_INTERVAL)
                    continue

                try:
                    await publish_payment_event(outbox.payload)

                except Exception:
                    logger.exception(
                        "Ошибка при публикации события",
                        extra={"outbox_id": str(outbox.id)},
                    )

                    async with session.begin():
                        await service.mark_outbox_event_failed(outbox)

                else:
                    async with session.begin():
                        await service.mark_outbox_event_published(outbox)

        except Exception:
            logger.exception("Ошибка воркера")
            await asyncio.sleep(POLL_INTERVAL)