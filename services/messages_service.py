import asyncio
import json
import os
from typing import Any, List

from sqlalchemy import select

from config.DataBase import get_session_factory
from config.Queue import get_kafka_producer, create_consumer
from config.Cache import get_redis
from models import Message


def _kafka_topic() -> str:
    return os.getenv("KAFKA_TOPIC", "messages")


def _kafka_group_id() -> str:
    return os.getenv("KAFKA_GROUP_ID", "messages-consumer")


async def enqueue_message(payload: dict[str, Any]) -> None:
    producer = get_kafka_producer()
    await producer.send_and_wait(_kafka_topic(), json.dumps(payload).encode("utf-8"))


async def list_messages() -> List[dict[str, Any]]:
    redis = get_redis()
    cache_key = "messages:all"
    cached = await redis.get(cache_key)
    if cached is not None:
        return json.loads(cached)

    session_factory = await get_session_factory()
    async with session_factory() as session:
        result = await session.execute(select(Message).order_by(Message.id.desc()))
        rows = result.scalars().all()
        items = [
            {"id": m.id, "content": m.content, "created_at": m.created_at.isoformat()}
            for m in rows
        ]

    await redis.set(cache_key, json.dumps(items), ex=600)
    return items


async def _consume_messages() -> None:
    consumer = create_consumer(_kafka_topic(), _kafka_group_id())
    await consumer.start()
    try:
        session_factory = await get_session_factory()
        redis = get_redis()
        async for record in consumer:
            try:
                data = json.loads(record.value.decode("utf-8"))
                async with session_factory() as session:
                    msg = Message(content=str(data.get("content", "")))
                    session.add(msg)
                    await session.commit()
                await redis.delete("messages:all")
            except Exception:
                continue
    finally:
        await consumer.stop()


def start_consumer_background() -> asyncio.Task:
    return asyncio.create_task(_consume_messages())
