from typing import Optional, Dict, Any


def get_root() -> Dict[str, Any]:
    return {"Hello": "World"}


def get_item(item_id: int, q: Optional[str] = None) -> Dict[str, Any]:
    return {"item_id": item_id, "q": q}
