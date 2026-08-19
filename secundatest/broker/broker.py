from faststream.rabbit import RabbitBroker, RabbitQueue

from secundatest.core.config import settings


PAYMENTS_QUEUE = "payments.new"
PAYMENTS_DEAD_LETTER_QUEUE = "payments.dlq"


payments_queue = RabbitQueue(PAYMENTS_QUEUE)

payments_dlq = RabbitQueue(PAYMENTS_DEAD_LETTER_QUEUE)

broker = RabbitBroker(settings.rabbitmq_url)