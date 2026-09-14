from app.redis_client import redis_client

HIGH_QUEUE = "high_priority_queue"
MEDIUM_QUEUE = "medium_priority_queue"
LOW_QUEUE = "low_priority_queue"


def enqueue_job(job_id: int, priority: str):
    priority = priority.upper()
    if priority == "HIGH":
        queue = HIGH_QUEUE
    elif priority == "LOW":
        queue = LOW_QUEUE
    else:
        queue = MEDIUM_QUEUE
    redis_client.rpush(queue, job_id)