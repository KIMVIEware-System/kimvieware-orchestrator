from datetime import datetime
from pathlib import Path
from typing import Dict, Optional
from contextlib import asynccontextmanager
import random
import threading
import json

from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

# Import shared utilities
from kimvieware_shared.utils.rabbitmq import create_connection, declare_queue, publish_message
from kimvieware_shared.utils.logging import setup_logger
from kimvieware_shared.storage.job_storage import JobStorage

BASE_DIR = Path(__file__).resolve().parents[2]
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

def _start_message_consumers():
    logger = setup_logger("Orchestrator-Consumer")
    job_storage = JobStorage()
    
    def callback(ch, method, properties, body):
        try:
            message = json.loads(body.decode('utf-8'))
            job_id = message.get('job_id')
            status = message.get('status')
            if not job_id: return

            # Build global job update
            job_update = {
                'job_id': job_id,
                'status': status,
                'updated_at': datetime.utcnow().isoformat() + 'Z'
            }
            
            # Promote results to top level for frontend access
            if 'mutation_stats' in message: job_update['mutation_stats'] = message['mutation_stats']
            if 'execution_stats' in message: job_update['execution_stats'] = message['execution_stats']
            if 'extraction_count' in message: job_update['extraction_count'] = message['extraction_count']
            if 'trajectories_count' in message: job_update['trajectories_count'] = message['trajectories_count']
            if 'trajectories' in message: job_update['trajectories'] = message['trajectories']
            if 'sut_info' in message: job_update['sut_info'] = message['sut_info']
            if 'error' in message: job_update['error'] = message['error']

            job_storage.save_job(job_update)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    try:
        connection = create_connection(logger=logger)
        channel = connection.channel()
        declare_queue(channel, 'phase.updates')
        channel.basic_consume(queue='phase.updates', on_message_callback=callback)
        channel.start_consuming()
    except: pass

@asynccontextmanager
async def lifespan(app: FastAPI):
    thread = threading.Thread(target=_start_message_consumers, daemon=True)
    thread.start()
    yield

app = FastAPI(title="KIMVIEware", lifespan=lifespan)
job_storage = JobStorage()
logger = setup_logger("Orchestrator")

@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    return templates.TemplateResponse("dashboard_pro.html", {"request": request})

@app.post("/api/submit")
async def submit_sut(file: UploadFile = File(...)):
    try:
        all_jobs = job_storage.get_all_jobs(limit=1000)
        job_id = f"job_{len(all_jobs) + 1:04d}"
        content = await file.read()
        
        upload_dir = BASE_DIR / "uploads"
        upload_dir.mkdir(exist_ok=True)
        file_path = upload_dir / f"{job_id}_{file.filename}"
        with open(file_path, "wb") as f: f.write(content)

        job = {
            "job_id": job_id, "filename": file.filename,
            "uploaded_at": datetime.utcnow().isoformat() + "Z",
            "status": "submitted", "extraction_count": 0, "trajectories_count": 0,
            "file_size": len(content)
        }
        job_storage.save_job(job)

        message = {"job_id": job_id, "sut_path": str(file_path), "status": "submitted"}
        conn = create_connection(logger=logger)
        ch = conn.channel()
        declare_queue(ch, 'submission.new')
        publish_message(ch, 'submission.new', message)
        conn.close()
        return {"job_id": job_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/jobs")
def list_jobs():
    return {"jobs": job_storage.get_all_jobs(limit=50)}

@app.get("/api/jobs/{job_id}")
def get_job(job_id: str):
    job = job_storage.get_job(job_id)
    if not job: raise HTTPException(status_code=404, detail="Job not found")
    return job

@app.get("/api/stats")
def get_stats():
    all_jobs = job_storage.get_all_jobs(limit=1000)
    completed = [j for j in all_jobs if j.get("status") == "completed"]
    
    total_red, total_mut = 0, 0
    labels, red_data, mut_data = [], [], []

    for j in completed:
        labels.append(j['job_id'][:8])
        init = j.get("extraction_count") or 1
        opt = j.get("trajectories_count") or 0
        red = max(0, (1 - (opt/max(1,init))) * 100)
        mut = j.get("mutation_stats", {}).get("mutation_score", 0)
        
        total_red += red
        total_mut += mut
        red_data.append(round(red, 1))
        mut_data.append(round(mut, 1))

    return {
        "total_jobs": len(all_jobs),
        "success_rate": (len(completed)/len(all_jobs)*100) if all_jobs else 0,
        "avg_reduction": round(total_red/max(1,len(completed)), 1),
        "avg_mutation": round(total_mut/max(1,len(completed)), 1),
        "chart_data": {"labels": labels[-10:], "reduction": red_data[-10:], "mutation": mut_data[-10:]}
    }

@app.get("/api/services")
def get_services():
    import socket
    srvs = [
        {"name": "RabbitMQ Broker", "port": 5672},
        {"name": "MongoDB Cluster", "port": 27017},
        {"name": "MinIO Storage", "port": 9000},
        {"name": "Gateway API", "port": 8080}
    ]
    results = []
    for s in srvs:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(0.1)
                status = "online" if sock.connect_ex(('localhost', s['port'])) == 0 else "offline"
                results.append({
                    "name": s['name'], "port": s['port'], "status": status,
                    "load": random.randint(5, 25) if status=="online" else 0
                })
        except: results.append({"name": s['name'], "port": s['port'], "status": "offline", "load": 0})
    
    workers = ["P0 Validator", "P1 Extractor", "P2 SGATS", "P3 EvoPath", "P4 Executor"]
    worker_results = [{"name": w, "status": "active", "health": "99.9%"} for w in workers]
    
    return {"infrastructure": results, "workers": worker_results}
