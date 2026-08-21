from aio_pika import ExchangeType
from faststream.rabbit import RabbitBroker, RabbitExchange, RabbitQueue

from secundatest.core.config import settings

PAYMENTS_QUEUE = "payments.new"

PAYMENTS_RETRY_QUEUE = "payments.retry"

PAYMENTS_DEAD_LETTER_EXCHANGE = "payments.dlx"
PAYMENTS_DEAD_LETTER_QUEUE = "payments.dlq"


payments_queue = RabbitQueue(
    name=PAYMENTS_QUEUE,
    durable=True,
    arguments={
        "x-dead-letter-exchange": PAYMENTS_DEAD_LETTER_EXCHANGE,
        "x-dead-letter-routing-key": PAYMENTS_DEAD_LETTER_QUEUE,
    },
)


payments_retry_queue = RabbitQueue(
    name=PAYMENTS_RETRY_QUEUE,
    durable=True,
    arguments={
        "x-message-ttl": 5000,
        "x-dead-letter-exchange": "",
        "x-dead-letter-routing-key": PAYMENTS_QUEUE,
    },
)


payments_dlx = RabbitExchange(
    name=PAYMENTS_DEAD_LETTER_EXCHANGE,
    type=ExchangeType.DIRECT,
    durable=True,
)


payments_dlq = RabbitQueue(
    name=PAYMENTS_DEAD_LETTER_QUEUE,
    durable=True,
)


broker = RabbitBroker(settings.rabbitmq_url)
