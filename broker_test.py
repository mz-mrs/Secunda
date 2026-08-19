import asyncio

from faststream.rabbit import RabbitBroker, RabbitQueue

from secundatest.core.config import settings


PAYMENTS_QUEUE = "payments.new"

payments_queue = RabbitQueue(PAYMENTS_QUEUE)

broker = RabbitBroker(settings.rabbitmq_url)


@broker.subscriber(payments_queue)
async def handle_payment(message: dict):
    print("RECEIVED MESSAGE:", message)


async def main():
    print("RABBITMQ URL:", repr(settings.rabbitmq_url))

    await broker.start()
    print("FASTSTREAM RABBITMQ OK")

    await broker.publish(
        {
            "operation_id": "test-123",
            "amount": "100.00",
            "currency": "RUB",
        },
        queue=PAYMENTS_QUEUE,
    )

    print("MESSAGE PUBLISHED")

    await asyncio.sleep(2)

    await broker.stop()
    print("BROKER STOPPED")


asyncio.run(main())