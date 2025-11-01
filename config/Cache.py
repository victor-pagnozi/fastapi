import os
import asyncio
from typing import Optional

from redis.asyncio import Redis

_redis_client: Optional[Redis] = None

def _build_redis_params() -> dict:
    host = os.getenv("REDIS_HOST", "localhost")
    port = int(os.getenv("REDIS_PORT", "6379"))
    password = os.getenv("REDIS_PASSWORD", "dY5MRW2^!jxHX")
    return {"host": host, "port": port, "password": password, "decode_responses": True}

async def init_redis() -> None:
    global _redis_client
    if _redis_client is not None:
        return
    params = _build_redis_params()
    _redis_client = Redis(**params)
    # Validate connectivity with retries
    max_retries = int(os.getenv("REDIS_INIT_MAX_RETRIES", "40"))
    sleep_seconds = float(os.getenv("REDIS_INIT_SLEEP_SECONDS", "0.25"))
    last_exc: Exception | None = None
    for _ in range(max_retries):
        try:
            await _redis_client.ping()
            last_exc = None
            break
        except Exception as exc:  # pragma: no cover - defensive startup
            last_exc = exc
            await asyncio.sleep(sleep_seconds)
    if last_exc is not None:
        raise last_exc


async def close_redis() -> None:
    global _redis_client
    if _redis_client is not None:
        await _redis_client.close()
    _redis_client = None


def get_redis() -> Redis:
    if _redis_client is None:
        raise RuntimeError("Redis client not initialized. Ensure init_redis() ran on startup.")
    return _redis_client
