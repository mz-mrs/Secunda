import logging
from faststream.rabbit import RabbitQueue

from secundatest.broker.broker import broker

logger = logging.getLogger(__name__)


@broker.subscriber(RabbitQueue("payments.new"))
async def process_payment(message: dict):
    logger.info(
        "Payment event received: payment_id=%s",
        message.get("payment_id"),
    )