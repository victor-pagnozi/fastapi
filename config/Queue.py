import os
import asyncio
from typing import Optional

from aiokafka import AIOKafkaProducer, AIOKafkaConsumer

_producer: Optional[AIOKafkaProducer] = None


def _bootstrap_servers() -> str:
    return os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

async def init_kafka() -> None:
    global _producer
    if _producer is not None:
        return

    _producer = AIOKafkaProducer(bootstrap_servers=_bootstrap_servers())
    max_retries = int(os.getenv("KAFKA_INIT_MAX_RETRIES", "60"))
    sleep_seconds = float(os.getenv("KAFKA_INIT_SLEEP_SECONDS", "0.5"))
    last_exc: Exception | None = None

    for _ in range(max_retries):
        try:
            await _producer.start()
            last_exc = None
            break
        except Exception as exc:
            last_exc = exc
            await asyncio.sleep(sleep_seconds)

    if last_exc is not None:
        raise last_exc


async def close_kafka() -> None:
    global _producer

    if _producer is not None:
        await _producer.stop()
    _producer = None


def get_kafka_producer() -> AIOKafkaProducer:
    if _producer is None:
        raise RuntimeError(
            "Kafka producer not initialized. Ensure init_kafka() ran on startup."
        )

    return _producer


def create_consumer(topic: str, group_id: str) -> AIOKafkaConsumer:
    return AIOKafkaConsumer(
        topic,
        bootstrap_servers=_bootstrap_servers(),
        group_id=group_id,
        enable_auto_commit=True,
        auto_offset_reset="earliest",
    )
