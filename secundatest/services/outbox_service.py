from datetime import datetime, timedelta, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from secundatest.enums.outbox_status import OutboxStatus
from secundatest.models.outbox import Outbox

BASE_RETRY_DELAY = 5
MAX_ATTEMPTS = 3


class OutboxService:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_pending_outbox_event(self) -> Outbox | None:
        now = datetime.now(timezone.utc)

        stmt = (
            select(Outbox)
            .where(
                Outbox.status == OutboxStatus.PENDING,
                (Outbox.next_attempt_at.is_(None) | (Outbox.next_attempt_at <= now)),
            )
            .order_by(Outbox.created_at)
            .with_for_update(skip_locked=True)
            .limit(1)
        )

        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def mark_outbox_event_processing(self, outbox: Outbox) -> None:
        outbox.status = OutboxStatus.PROCESSING
        await self.session.flush()

    async def mark_outbox_event_published(self, outbox: Outbox) -> None:
        outbox.status = OutboxStatus.PUBLISHED
        outbox.published_at = datetime.now(timezone.utc)
        await self.session.flush()

    async def mark_outbox_event_failed(self, outbox: Outbox) -> None:
        outbox.attempts += 1

        if outbox.attempts >= MAX_ATTEMPTS:
            outbox.status = OutboxStatus.FAILED
            return

        outbox.status = OutboxStatus.PENDING
        outbox.next_attempt_at = datetime.now(timezone.utc) + timedelta(
            seconds=BASE_RETRY_DELAY * 2 ** (outbox.attempts - 1)
        )

        await self.session.flush()
