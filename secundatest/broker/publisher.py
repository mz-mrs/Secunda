import asyncio
import logging

from datetime import datetime, timezone

from sqlalchemy import select

from secundatest.broker.broker import broker, PAYMENTS_QUEUE
from secundatest.db.session import async_session_factory
from secundatest.enums import OutboxStatus
from secundatest.models import Outbox

logger = logging.getLogger("secundatest.broker.publisher")

PUBLISH_INTERVAL = 1
MAX_ATTEMPTS = 3


async def publish_from_outbox() -> None:
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

                        logger.info(
                            f"Событие {event.id} в обработке, попытка {event.attempts}"
                        )

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
                                db_event.published_at = datetime.now(timezone.utc)

                except Exception:
                    logger.exception(
                        f"Ошибка отправки события {event.id}, попытка {event.attempts}"
                    )

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

                                logger.error(
                                    f"Ошибка отправки события {event.id}, попытка {db_event.attempts}"
                                )

                            else:
                                db_event.status = OutboxStatus.PENDING

                                logger.warning(
                                    f"Исчерпано количество попыток {db_event.attempts} после отправки события {event.id}"
                                )

        except Exception as exc:
            logger.exception(f"Ошибка публикации {exc}")

        await asyncio.sleep(PUBLISH_INTERVAL)
