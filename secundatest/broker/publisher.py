from secundatest.broker.broker import broker

PAYMENTS_QUEUE = "payments.new"

async def publish_payment_event(payload: dict) -> None:
    await broker.publish(
        payload,
        queue=PAYMENTS_QUEUE,
    )