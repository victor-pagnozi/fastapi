from typing import Union
from contextlib import asynccontextmanager

from fastapi import FastAPI

from config.DataBase import init_db, close_db
from config.Cache import init_redis, close_redis
from config.Queue import init_kafka, close_kafka


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    await init_redis()
    await init_kafka()
    try:
        yield
    finally:
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