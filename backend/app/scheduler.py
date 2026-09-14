import time
from datetime import datetime, timezone

from app.database import SessionLocal
from app.models import Job
from app.queue import enqueue_job


def run_scheduler():
    print("Scheduler started.")

    while True:
        db = SessionLocal()

        try:
            now = datetime.now(timezone.utc)

            jobs = db.query(Job).filter(
                Job.status == "QUEUED",
                Job.scheduled_at <= now
            ).all()

            for job in jobs:
                enqueue_job(job.id, job.priority)

                job.scheduled_at = None

                db.commit()

                print(f"Scheduled Job {job.id} added to Redis")

        finally:
            db.close()

        time.sleep(5)


if __name__ == "__main__":
    run_scheduler()