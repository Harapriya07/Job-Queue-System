from app.redis_client import redis_client

QUEUE_NAME = "jobs_queue"


def enqueue_job(job_id: int):
    redis_client.rpush(QUEUE_NAME, job_id)