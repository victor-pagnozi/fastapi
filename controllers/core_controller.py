from typing import Optional

from fastapi import APIRouter

from services.core_service import get_root, get_item


router = APIRouter()


@router.get("/")
async def read_root():
    return get_root()


@router.get("/items/{item_id}")
async def read_item(item_id: int, q: Optional[str] = None):
    return get_item(item_id, q)
