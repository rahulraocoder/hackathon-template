from fastapi import FastAPI, HTTPException, Body
from celery_app import process_submission
from db_models import Session, SubmissionResult
from datetime import datetime
import uuid
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Retail Data Pipeline Hackathon Evaluator",
    description="API for evaluating participant submissions",
    version="2.0"
)

from pydantic import BaseModel

class SubmissionData(BaseModel):
    customers_path: str
    products_path: str
    orders_path: str
    shipments_path: str
    returns_path: str

class SubmissionRequest(BaseModel):
    data: SubmissionData
    team_name: str = None

@app.post("/api/submit")
async def submit_data(request: SubmissionRequest = Body(...)):
    """Submit evaluation results via Celery"""
    try:
        logger.info(f"Received submission from team: {request.team_name}")
        logger.info(f"Request data: {request.data}")
        job_id = str(uuid.uuid4())
        
        # Create pending record
        session = Session()
        session.add(SubmissionResult(
            id=job_id,
            participant_id=request.team_name,
            status='queued',
            timestamp=datetime.utcnow()
        ))
        session.commit()

        # Queue Celery task
        process_submission.delay({
            'job_id': job_id,
            'team_name': request.team_name,
            'data': request.data.dict()
        })
        
        return {
            "job_id": job_id,
            "status_url": f"/api/status/{job_id}"
        }
    except Exception as e:
        logger.error(f"Error processing submission: {str(e)}")
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/status/{job_id}")
async def get_status(job_id: str):
    """Check submission status"""
    session = Session()
    result = session.query(SubmissionResult).filter_by(id=job_id).first()
    
    if not result:
        raise HTTPException(404, "Job not found")
    
    return {
        "job_id": job_id,
        "status": result.status,
        "score": result.score,
        "timestamp": result.timestamp.isoformat()
    }

@app.get("/api/results/{job_id}")
async def get_results(job_id: str):
    """Get full results (evaluator only)"""
    session = Session()
    result = session.query(SubmissionResult).filter_by(id=job_id).first()
    
    if not result:
        raise HTTPException(404, "Job not found")
    
    return {
        "job_id": job_id,
        "status": result.status,
        "score": result.score,
        "details": result.details,
        "processing_time": result.processing_time,
        "timestamp": result.timestamp.isoformat()
    }
