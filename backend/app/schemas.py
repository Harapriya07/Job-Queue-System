from pydantic import BaseModel


class JobCreate(BaseModel):
    task: str
    priority: str ="MEDIUM"