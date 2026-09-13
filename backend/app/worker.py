import time
from app.redis_client import redis_client
from app.database import SessionLocal
from app.models import Job

QUEUE_NAME = "jobs_queue"


def run_worker():
    print("Worker started. Waiting for jobs...")

    while True:
        result = redis_client.blpop(QUEUE_NAME, timeout=5)
        if result is None:
           continue
        
        job_id = int(result[1])

        db = SessionLocal()

        try:
            job = db.get(Job, job_id)

            if not job:
                print(f"Job {job_id} not found")
                continue

            job.status = "RUNNING"
            db.commit()

            print(f"Processing Job {job_id}: {job.task}")

            time.sleep(3)

            job.status = "COMPLETED"
            db.commit()

            print(f"Job {job_id} completed")

        finally:
            db.close()


if __name__ == "__main__":
    run_worker()