from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from fastapi.responses import Response
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST
import uuid
import asyncio
import time
from datetime import datetime

app = FastAPI(title="automation-gateway")

# -----------------------------
# Prometheus Metrics
# -----------------------------
job_counter = Counter(
    "automation_jobs_total",
    "Total automation jobs processed",
    ["action", "status"]
)

job_duration = Histogram(
    "automation_job_duration_seconds",
    "Duration of automation jobs in seconds",
    ["action"]
)

queue_depth = Gauge(
    "automation_job_queue_depth",
    "Number of jobs currently queued"
)

# -----------------------------
# In-memory Job Store
# -----------------------------
jobs = {}  # job_id -> job dict

VALID_ACTIONS = {"restart_service", "rotate_config", "health_check"}


class JobRequest(BaseModel):
    action: str
    target: str
    requested_by: str
    parameters: dict | None = None


# -----------------------------
# Health Endpoint
# -----------------------------
@app.get("/healthz")
async def healthz():
    return {"status": "ok"}


# -----------------------------
# Async Job Runner
# -----------------------------
async def run_job(job_id: str):
    job = jobs[job_id]
    action = job["action"]

    queue_depth.inc()
    job["status"] = "RUNNING"
    job["started_at"] = datetime.utcnow().isoformat()

    start_time = time.time()

    try:
        # Simulate work
        await asyncio.sleep(2)

        # Optional random failure simulation:
        # import random
        # if random.random() < 0.2:
        #     raise RuntimeError("Simulated failure")

        job["status"] = "SUCCESS"
        job["error"] = None
        status_label = "SUCCESS"

    except Exception as e:
        job["status"] = "FAILED"
        job["error"] = str(e)
        status_label = "FAILED"

    finally:
        duration = time.time() - start_time
        job["finished_at"] = datetime.utcnow().isoformat()

        # Record metrics
        job_duration.labels(action=action).observe(duration)
        job_counter.labels(action=action, status=status_label).inc()
        queue_depth.dec()


# -----------------------------
# Create Job Endpoint
# -----------------------------
@app.post("/v1/jobs", status_code=202)
async def create_job(req: JobRequest):
    if req.action not in VALID_ACTIONS:
        raise HTTPException(status_code=400, detail="Unsupported action")

    job_id = str(uuid.uuid4())

    jobs[job_id] = {
        "job_id": job_id,
        "action": req.action,
        "target": req.target,
        "requested_by": req.requested_by,
        "parameters": req.parameters or {},
        "status": "PENDING",
        "started_at": None,
        "finished_at": None,
        "error": None,
    }

    # Fire-and-forget async execution
    asyncio.create_task(run_job(job_id))

    return {"job_id": job_id, "status": "PENDING"}


# -----------------------------
# Get Job Status Endpoint
# -----------------------------
@app.get("/v1/jobs/{job_id}")
async def get_job(job_id: str):
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job


# -----------------------------
# Metrics Endpoint
# -----------------------------
@app.get("/metrics")
async def metrics():
    data = generate_latest()
    return Response(content=data, media_type=CONTENT_TYPE_LATEST)
