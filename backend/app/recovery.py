from app.database import SessionLocal
from app.models import Job
from app.redis_client import redis_client

QUEUE_NAME = "jobs_queue"


def recover_stuck_jobs():
    db = SessionLocal()

    try:
        jobs = db.query(Job).filter(Job.status == "RUNNING").all()

        for job in jobs:
            heartbeat_key = f"worker:{job.worker_id}:heartbeat"

            if not redis_client.exists(heartbeat_key):
                print(f"Worker {job.worker_id} is dead")
                print(f"Recovering Job {job.id}")

                job.status = "QUEUED"
                job.worker_id = None

                db.commit()

                redis_client.rpush(QUEUE_NAME, job.id)

    finally:
        db.close()

if __name__ == "__main__":
    recover_stuck_jobs()        