import os
import json
import asyncio
import aiohttp
import aio_pika
import contextlib


RABBITMQ_USER = os.getenv("RABBITMQ_USER", "logger")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "logger")
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = os.getenv("RABBITMQ_PORT", "5672")
RABBITMQ_QUEUE_NAME = os.getenv("RABBITMQ_QUEUE_NAME", "logs")

MANTICORE_URI = os.getenv("MANTICORE_URI", "http://manticore:9308")
MANTICORE_TABLE = os.getenv("MANTICORE_TABLE", "logs")

RABBITMQ_URI = (
    f"amqp://{RABBITMQ_USER}:{RABBITMQ_PASSWORD}@{RABBITMQ_HOST}:{RABBITMQ_PORT}/logs"
)

BATCH_SIZE = 100
FLUSH_INTERVAL = 2.0


async def push_to_manticore(session: aiohttp.ClientSession, items: list[dict]) -> None:
    if not items:
        return
    ndjson_lines = []
    for item in items:
        ndjson_lines.append(
            json.dumps(
                {
                    "insert": {
                        "table": MANTICORE_TABLE,
                        "doc": item["doc"],
                    }
                }
            )
        )

    body = "\n".join(ndjson_lines) + "\n"
    url = f"{MANTICORE_URI}/bulk"
    async with session.post(
        url, data=body, headers={"Content-Type": "application/x-ndjson"}
    ) as resp:
        text = await resp.text()
        if resp.status >= 400:
            raise RuntimeError(f"Manticore bulk failed: {resp.status} {text}")


async def flush_batch(session: aiohttp.ClientSession, batch: list[dict]) -> None:
    if not batch:
        return
    await push_to_manticore(session, batch)
    for item in batch:
        await item["message"].ack()


async def periodic_flusher(
    session: aiohttp.ClientSession, batch: list[dict], batch_lock: asyncio.Lock
) -> None:
    while True:
        await asyncio.sleep(FLUSH_INTERVAL)
        async with batch_lock:
            if not batch:
                continue
            to_flush = batch[:]
            batch.clear()
        try:
            await flush_batch(session, to_flush)
        except Exception as e:
            async with batch_lock:
                batch[0:0] = to_flush


async def main() -> None:
    connection = await aio_pika.connect_robust(RABBITMQ_URI)
    batch: list[dict] = []
    batch_lock = asyncio.Lock()
    timeout = aiohttp.ClientTimeout(total=30)
    async with connection, aiohttp.ClientSession(timeout=timeout) as session:
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=BATCH_SIZE)
        queue = await channel.declare_queue(RABBITMQ_QUEUE_NAME, durable=True)
        flusher_task = asyncio.create_task(periodic_flusher(session, batch, batch_lock))
        print(f"[*] Waiting for logs in queue: {RABBITMQ_QUEUE_NAME}")
        try:
            async with queue.iterator() as queue_iter:
                async for message in queue_iter:
                    try:
                        payload = json.loads(message.body.decode())
                    except json.JSONDecodeError:
                        print(f"[!] Bad JSON, reject: {message.body!r}")
                        await message.reject(requeue=False)
                        continue
                    item = {
                        "doc": {
                            "timestamp": payload.get("timestamp"),
                            "source_ip": payload.get("source_ip"),
                            "source_tag": payload.get("source_tag"),
                            "data": payload,
                        },
                        "message": message,
                    }
                    need_flush = False
                    async with batch_lock:
                        batch.append(item)
                        print("Received:", payload)

                        if len(batch) >= BATCH_SIZE:
                            to_flush = batch[:]
                            batch.clear()
                            need_flush = True
                        else:
                            to_flush = []

                    if need_flush:
                        try:
                            await flush_batch(session, to_flush)
                        except Exception as e:
                            print(f"[!] Immediate flush failed: {e}")
                            async with batch_lock:
                                batch[0:0] = to_flush

        finally:
            flusher_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await flusher_task


if __name__ == "__main__":
    asyncio.run(main())
