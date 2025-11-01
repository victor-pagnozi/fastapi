from typing import Union
from contextlib import asynccontextmanager
import contextlib
import os
import asyncio
import json

from fastapi import FastAPI
from pydantic import BaseModel
from sqlalchemy import select

from config.DataBase import init_db, close_db
from config.DataBase import get_engine, get_session_factory
from config.Cache import init_redis, close_redis, get_redis
from config.Queue import init_kafka, close_kafka
from config.Queue import get_kafka_producer, create_consumer
from models import init_models, Message


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    await init_redis()
    await init_kafka()
    await init_models(get_engine())
    app.state.kafka_consumer_task = asyncio.create_task(_consume_messages())
    try:
        yield
    finally:
        task = getattr(app.state, "kafka_consumer_task", None)
        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        await close_kafka()
        await close_redis()
        await close_db()


app = FastAPI(lifespan=lifespan)


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/items/{item_id}")
def read_item(item_id: int, q: Union[str, None] = None):
    return {"item_id": item_id, "q": q}


class MessageIn(BaseModel):
    content: str


def _kafka_topic() -> str:
    return os.getenv("KAFKA_TOPIC", "messages")


def _kafka_group_id() -> str:
    return os.getenv("KAFKA_GROUP_ID", "messages-consumer")


@app.post("/messages", status_code=202)
async def enqueue_message(payload: MessageIn):
    producer = get_kafka_producer()
    value_bytes = json.dumps(payload.model_dump()).encode("utf-8")
    await producer.send_and_wait(_kafka_topic(), value_bytes)
    return {"queued": True}


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


@app.get("/messages")
async def list_messages():
    redis = get_redis()
    cache_key = "messages:all"
    cached = await redis.get(cache_key)

    if cached is not None:
        return json.loads(cached)

    session_factory = await get_session_factory()

    async with session_factory() as session:
        result = await session.execute(
            select(Message).order_by(Message.id.desc()))
        rows = result.scalars().all()
        items = [{
            "id": m.id,
            "content": m.content,
            "created_at": m.created_at.isoformat()
        } for m in rows]

    await redis.set(cache_key, json.dumps(items), ex=600)
    return items
