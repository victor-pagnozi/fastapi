from pydantic import BaseModel


class MessageIn(BaseModel):
    content: str
