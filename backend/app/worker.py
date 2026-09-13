import time
from app.redis_client import redis_client
from app.database import SessionLocal
from app.models import Job
import uuid

QUEUE_NAME = "jobs_queue"
WORKER_ID = str(uuid.uuid4())
HEARTBEAT_KEY = f"worker:{WORKER_ID}:heartbeat"


def run_worker():
    print("Worker started. Waiting for jobs...")

    while True:
        send_heartbeat()
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
            job.worker_id = WORKER_ID
            db.commit()

            print(f"Processing Job {job_id}: {job.task}")

            try:
                if job.task == "long_task":
                    time.sleep(60)
                elif job.task == "fail":
                    raise Exception("Task failed intentionally")
                else:
                    time.sleep(3)
                job.status = "COMPLETED"
                db.commit()

                print(f"Job {job_id} completed")

            except Exception as e:
                job.retry_count += 1

                if job.retry_count <= job.max_retries:
                    job.status = "QUEUED"
                    job.error_message = str(e)
                    db.commit()

                    delay = 2 ** (job.retry_count - 1)
                    print(f"Waiting {delay} seconds before retrying Job {job_id}")
                    time.sleep(delay)
                    redis_client.rpush(QUEUE_NAME, job.id)

                    print(
                       f"Job {job_id} failed. "
                       f"Retrying ({job.retry_count}/{job.max_retries})"
                    )
                else:
                    job.status = "FAILED"
                    job.error_message = str(e)
                    db.commit()

                    print(f"Job {job_id} permanently failed: {e}")
        finally:
            db.close()

def send_heartbeat():
    redis_client.set(
        HEARTBEAT_KEY,
        "alive",
        ex=15
    )
    print(f"Heartbeat sent: {WORKER_ID}")
if __name__ == "__main__":
    run_worker()