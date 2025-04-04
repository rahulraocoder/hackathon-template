from celery import Celery
import os
import logging
from db_models import Session, SubmissionResult
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Create Celery app
app = Celery(
    'celery_app',
    broker=os.getenv('CELERY_BROKER_URL', 'redis://redis:6379/0'),
    backend=os.getenv('CELERY_RESULT_BACKEND', 'redis://redis:6379/0')
)

# Configure Celery
app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    task_track_started=True,
    task_time_limit=300,
    broker_connection_retry_on_startup=True
)

from worker import process_submission as worker_process_submission

@app.task(name='celery_app.process_submission')
def process_submission(submission_data):
    """Process submission data and store results"""
    try:
        import json
        # Handle both dict and JSON string inputs
        if isinstance(submission_data, str):
            data = json.loads(submission_data)
        else:
            data = submission_data
        
        # Call the actual processing function from worker
        result = worker_process_submission(data)
        
        session = Session()
        # Check if record exists (created by main.py)
        db_result = session.query(SubmissionResult).filter_by(
            id=data.get('job_id')
        ).first()
        
        if db_result:
            # Update existing record with actual results
            db_result.status = result.get('status', 'completed')
            db_result.score = result.get('score')
            db_result.details = result.get('details', {})
            db_result.processed_data = result.get('results', {})
            db_result.processing_time = result.get('processing_time', 0)
        else:
            # Create new record if not exists
            db_result = SubmissionResult(
                id=data.get('job_id'),
                participant_id=data.get('team_name'),
                status=result.get('status', 'completed'),
                score=result.get('score'),
                details=result.get('details', {}),
                processed_data=result.get('results', {}),
                processing_time=result.get('processing_time', 0),
                timestamp=datetime.utcnow()
            )
            session.add(db_result)
        
        session.commit()
        return True
    except Exception as e:
        logger.error(f"Failed to process submission: {str(e)}", exc_info=True)
        
        # Update status to failed
        session = Session()
        db_result = session.query(SubmissionResult).filter_by(
            id=data.get('job_id')
        ).first()
        if db_result:
            db_result.status = 'failed'
            db_result.details = {'error': str(e)}
            session.commit()
            
        return False
