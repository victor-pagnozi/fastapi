from fastapi import APIRouter

from schemas import MessageIn
from services.messages_service import enqueue_message, list_messages

router = APIRouter()


@router.post("/messages", status_code=202)
async def post_message(payload: MessageIn):
    await enqueue_message(payload.model_dump())

    return {"queued": True}


@router.get("/messages")
async def get_messages():
    return await list_messages()
