import asyncio
import logging

import httpx

logger = logging.getLogger(__name__)


class WebhookService:
    MAX_RETRIES = 3
    BASE_DELAY = 1

    async def send(
        self,
        webhook_url: str,
        payload: dict,
    ) -> bool:
        for attempt in range(1, self.MAX_RETRIES + 1):
            try:
                async with httpx.AsyncClient(timeout=5.0) as client:
                    response = await client.post(
                        webhook_url,
                        json=payload,
                    )

                response.raise_for_status()

                logger.info(f"Вебхук отправлен успешно: {webhook_url=} {attempt=}")

                return True

            except (httpx.HTTPError, httpx.TimeoutException) as exc:
                logger.warning(
                    f"Вебхук не отправлен: {webhook_url=} "
                    f"{attempt}/{self.MAX_RETRIES} попыток "
                    f"error={exc}"
                )

                if attempt == self.MAX_RETRIES:
                    logger.error(
                        f"Отправка вебхука {webhook_url=} "
                        f"провалена после {self.MAX_RETRIES} попыток"
                    )

                    return False

                delay = self.BASE_DELAY * (2 ** (attempt - 1))

                await asyncio.sleep(delay)

        return False