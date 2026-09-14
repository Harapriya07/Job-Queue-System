from datetime import datetime
from pydantic import BaseModel


class JobCreate(BaseModel):
    task: str
    priority: str ="MEDIUM"
    scheduled_at: datetime | None = None