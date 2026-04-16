import asyncio
from aio_pika import connect, Message
import os

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST","localhost")
RABBITMQ_PORT = os.getenv("RABBITMQ_PORT","5672")
RABBITMQ_USER = os.getenv("RABBITMQ_USER","user")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD","pass")
RABBITMQ_QUEUE_NAME = os.getenv("RABBITMQ_QUEUE_NAME","logs")
RABBITMQ_URI = f"amqp://{RABBITMQ_USER}:{RABBITMQ_PASSWORD}@{RABBITMQ_HOST}:{RABBITMQ_PORT}/logs"

async def main(loop):
    connection = await aio_pika.connect_robust(RABBITMQ_URI)

    async with connection:
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=10)
        queue = await channel.declare_queue(RABBITMQ_QUEUE_NAME, durable=True)
        print(f"[*] Waiting for logs in queue: {RABBITMQ_QUEUE_NAME}")
        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    print("Received:", message.body.decode())

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop))