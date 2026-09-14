import time
from app.redis_client import redis_client
from app.database import SessionLocal
from app.models import Job
import uuid
import threading
from sqlalchemy import select
from app.queue import enqueue_job

HIGH_QUEUE = "high_priority_queue"
MEDIUM_QUEUE = "medium_priority_queue"
LOW_QUEUE = "low_priority_queue"
WORKER_ID = str(uuid.uuid4())
HEARTBEAT_KEY = f"worker:{WORKER_ID}:heartbeat"
high_count = 0

def get_next_job():
    global high_count

    if high_count < 3:
        result = redis_client.blpop(HIGH_QUEUE, timeout=1)

        if result:
            high_count += 1
            return result

    result = redis_client.blpop(MEDIUM_QUEUE, timeout=1)

    if result:
        high_count = 0
        return result

    result = redis_client.blpop(LOW_QUEUE, timeout=1)

    if result:
        high_count = 0
        return result

    return None
def run_worker():
    print("Worker started. Waiting for jobs...")

    heartbeat_thread = threading.Thread(
    target=heartbeat_loop,
    daemon=True
    )

    heartbeat_thread.start()

    while True:
        result = get_next_job()
        if result is None:
           continue
        
        job_id = int(result[1])

        db = SessionLocal()

        try:
            job = db.execute(
                    select(Job)
                   .where(Job.id == job_id)
                   .with_for_update()
                   ).scalar_one_or_none()


            if not job:
                print(f"Job {job_id} not found")
                continue

            if job.status != "QUEUED":
               print(f"Job {job_id} is already {job.status}; skipping")
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
                    redis_client.rpush( job.id,job.priority)

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

def heartbeat_loop():
    while True:
        send_heartbeat()
        time.sleep(5)
if __name__ == "__main__":
    run_worker()