from contextlib import asynccontextmanager
import contextlib
import asyncio

from fastapi import FastAPI

from config.DataBase import init_db, close_db
from config.DataBase import get_engine
from config.Cache import init_redis, close_redis
from config.Queue import init_kafka, close_kafka
from models import init_models
from controllers.messages_controller import router as messages_router
from controllers.core_controller import router as core_router
from services.messages_service import start_consumer_background


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    await init_redis()
    await init_kafka()
    await init_models(get_engine())
    app.state.kafka_consumer_task = start_consumer_background()
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

# Routers
app.include_router(core_router)
app.include_router(messages_router)
