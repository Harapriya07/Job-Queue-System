from fastapi import FastAPI,Depends
from sqlalchemy.orm import Session
from app.database import SessionLocal
from app.models import Job
from app.schemas import JobCreate
from app.queue import enqueue_job

app = FastAPI()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.get("/")
def home():

    return {"message": "Job Queue System is running",
        }

@app.post("/jobs")
def create_job(job_data: JobCreate, db: Session = Depends(get_db)):
    job = Job(task=job_data.task)

    db.add(job)
    db.commit()
    db.refresh(job)
    enqueue_job(job.id)
    return job