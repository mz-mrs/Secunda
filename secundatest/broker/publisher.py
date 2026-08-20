import asyncio
from datetime import datetime, timezone

from sqlalchemy import select

from secundatest.broker.broker import broker, payments_queue
from secundatest.db.session import async_session_factory
from secundatest.enums import OutboxStatus
from secundatest.models.outbox import Outbox


PUBLISH_INTERVAL = 1
MAX_ATTEMPTS = 3


async def publish_outbox() -> None:
    while True:
        async with async_session_factory() as session:
            result = await session.execute(
                select(Outbox)
                .where(
                    Outbox.status == OutboxStatus.PENDING,
                )
                .order_by(Outbox.created_at)
                .limit(10)
            )

            events = result.scalars().all()

            for event in events:
                event.status = OutboxStatus.PROCESSING
                event.attempts += 1

                await session.commit()

                try:
                    await broker.publish(
                        event.payload,
                        queue=payments_queue,
                    )

                    event.status = OutboxStatus.PUBLISHED
                    event.published_at = datetime.now(timezone.utc)

                except Exception:
                    if event.attempts >= MAX_ATTEMPTS:
                        event.status = OutboxStatus.FAILED
                    else:
                        event.status = OutboxStatus.PENDING

                await session.commit()

        await asyncio.sleep(PUBLISH_INTERVAL)