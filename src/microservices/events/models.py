from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime, timezone

def utc_now():
    return datetime.now(timezone.utc)

class EventResponse(BaseModel):
    status: str
    partition: int
    offset: int
    event: dict

class UserEvent(BaseModel):
    user_id: int
    username: str
    email: Optional[str]
    action: str
    timestamp: Optional[datetime] = Field(default_factory=utc_now)

class PaymentEvent(BaseModel):
    payment_id: Optional[int] = 1
    user_id: int
    amount: float
    currency: Optional[str] = "USD"
    status: Optional[str] = "completed"
    method_type: Optional[str] = None
    timestamp: Optional[datetime] = Field(default_factory=utc_now)

class MovieEvent(BaseModel):
    movie_id: int
    title: str
    action: str
    user_id: Optional[int]
    rating: Optional[float]
    genres: Optional[List[str]] = []
    timestamp: Optional[datetime] = Field(default_factory=utc_now)
