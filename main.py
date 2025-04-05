from fastapi import FastAPI, HTTPException, Body, WebSocket
from fastapi.websockets import WebSocketDisconnect
import asyncio
import json
from sqlalchemy import func
from worker import process_submission
from db_models import Session, SubmissionResult
from datetime import datetime
import uuid
import logging
from typing import Dict, List

class ConnectionManager:
    def __init__(self):
        self.active_connections: Dict[str, List[WebSocket]] = {}

    async def connect(self, websocket: WebSocket, team: str):
        await websocket.accept()
        if team not in self.active_connections:
            self.active_connections[team] = []
        self.active_connections[team].append(websocket)

    def disconnect(self, websocket: WebSocket, team: str):
        if team in self.active_connections:
            self.active_connections[team].remove(websocket)
            if not self.active_connections[team]:
                del self.active_connections[team]

    async def broadcast(self, message: str, team: str):
        if team in self.active_connections:
            for connection in self.active_connections[team]:
                try:
                    await connection.send_text(message)
                except:
                    self.disconnect(connection, team)

manager = ConnectionManager()

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

@app.websocket("/ws/scores")
async def websocket_endpoint(websocket: WebSocket, team: str):
    await manager.connect(websocket, team)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket, team)

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
