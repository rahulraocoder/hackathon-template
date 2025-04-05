from celery.signals import worker_ready
import logging
import json
from db_models import Session, SubmissionResult
from datetime import datetime
from pipeline import DataPipeline
from celery_app import app  # Get Celery app instance

# Configure basic console logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(console_formatter)
logger.addHandler(console_handler)

logger.info("Initialized console logging")

@worker_ready.connect
def on_worker_ready(sender=None, **kwargs):
    """Log when worker is ready"""
    logger.info("Worker is ready to process tasks")

@app.task(name='process_submission', bind=True)
def process_submission(self, submission_data):
    """Process submission data and store results"""
    session = None
    try:
        # Ensure we have a dict
        if isinstance(submission_data, str):
            submission_data = json.loads(submission_data)
        
        # Use the task's ID as job_id if not provided
        job_id = submission_data.get('job_id', self.request.id)
        team_name = submission_data.get('team_name')
            
        # Extract and validate data paths
        data = submission_data.get('data', {})
        required_paths = ['customers_path', 'products_path', 'orders_path',
                        'shipments_path', 'returns_path']
        
        missing_paths = [path for path in required_paths if path not in data]
        if missing_paths:
            raise ValueError(f"Missing required data paths: {missing_paths}")
            
        # Initialize and run pipeline
        pipeline = DataPipeline()
        results = pipeline.process_data(
            data['customers_path'],
            data['products_path'],
            data['orders_path'],
            data['shipments_path'],
            data['returns_path']
        )
        
        if not results or 'score' not in results:
            raise ValueError("Pipeline returned invalid results")
        
        # Store/update results
        session = Session()
        result = session.query(SubmissionResult).filter_by(id=job_id).first()
        
        if result:
            # Update existing record
            result.status = 'completed'
            result.score = results.get('score')
            result.details = {
                'data_quality': vars(results.get('data_quality', {})),
                'insights': vars(results.get('insights', {}))
            }
            result.processing_time = results.get('processing_time', 0)
        else:
            # Create new record
            result = SubmissionResult(
                id=job_id,
                participant_id=team_name,
                status='completed',
                score=results.get('score'),
                details={
                    'data_quality': vars(results.get('data_quality', {})),
                    'insights': vars(results.get('insights', {}))
                },
                processing_time=results.get('processing_time', 0),
                timestamp=datetime.utcnow()
            )
            session.add(result)
        
        session.commit()
        return {
            'status': 'success',
            'job_id': job_id,
            'processing_time': results.get('processing_time', 0)
        }
        
    except Exception as e:
        logger.error(f"Failed to process submission: {str(e)}", exc_info=True)
        
        # Update status to failed if we have a job_id
        if session and 'job_id' in locals():
            try:
                result = session.query(SubmissionResult).filter_by(id=job_id).first()
                if result:
                    result.status = 'failed'
                    result.details = {'error': str(e)}
                    session.commit()
            except Exception as db_error:
                logger.error(f"Failed to update failed status: {str(db_error)}")
            
        return {
            'status': 'error',
            'error': str(e),
            'job_id': job_id if 'job_id' in locals() else None
        }
