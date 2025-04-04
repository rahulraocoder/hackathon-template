from celery import Celery
from celery.signals import worker_ready
import logging
import json
from db_models import Session, SubmissionResult
from datetime import datetime
from pipeline import DataPipeline

# Configure logging to both console and file
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Configure logging to console first as fallback
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Console handler as primary
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(console_formatter)
logger.addHandler(console_handler)

# Try to setup file logging as secondary
try:
    from pathlib import Path
    log_dir = Path('/app/logs')
    log_dir.mkdir(exist_ok=True, parents=True)
    
    file_handler = logging.FileHandler('/app/logs/worker.log')
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)
    logger.info("File logging configured successfully")
except Exception as e:
    logger.warning(f"Could not configure file logging: {str(e)}. Using console logging only")
file_handler.setLevel(logging.DEBUG)
file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(file_formatter)

# Console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(console_formatter)

# Add both handlers
logger.addHandler(file_handler)
logger.addHandler(console_handler)

logger.info("Initialized logging with both file and console output")

# Import Celery app from celery_app
from celery_app import app

@worker_ready.connect
def on_worker_ready(sender=None, **kwargs):
    """Log when worker is ready"""
    logger.info("Worker is ready to process tasks")

@app.task(name='process_submission', bind=True)
def process_submission(self, submission_data):
    """Process and store a single submission"""
    try:
        # Initialize pipeline with detailed logging
        logger.info("Initializing DataPipeline with debug logging")
        try:
            pipeline = DataPipeline()
            logger.info("Pipeline initialized successfully")
            logger.debug(f"Spark session active: {pipeline.spark.sparkContext.appName}")
            logger.debug(f"Spark version: {pipeline.spark.version}")
        except Exception as e:
            logger.error(f"Pipeline initialization failed: {str(e)}", exc_info=True)
            raise ValueError(f"Pipeline initialization error: {str(e)}")
        
        # Convert data to dict if it's a string
        if isinstance(submission_data, str):
            try:
                submission_data = json.loads(submission_data)
                logger.info("Successfully parsed submission data")
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON data: {str(e)}", exc_info=True)
                raise ValueError("Invalid JSON data")

        # Get data paths from submission with validation
        data = submission_data.get('data', {})
        required_paths = ['customers_path', 'products_path', 'orders_path', 
                        'shipments_path', 'returns_path']
        
        missing_paths = [path for path in required_paths if path not in data]
        if missing_paths:
            raise ValueError(f"Missing required data paths: {missing_paths}")
        
        logger.info(f"Processing data with paths: {data}")

        # Process data with detailed error handling
        try:
            logger.info("Starting data processing")
            results = pipeline.process_data(
                data['customers_path'],
                data['products_path'], 
                data['orders_path'],
                data['shipments_path'],
                data['returns_path']
            )
            
            if not results or 'score' not in results:
                raise ValueError("Pipeline returned empty or invalid results")
                
            logger.info(f"Processing completed successfully. Score: {results['score']}")
            logger.debug(f"Full results: {str(results)}")
            
        except Exception as e:
            logger.error(f"Data processing failed: {str(e)}", exc_info=True)
            raise ValueError(f"Data processing error: {str(e)}")
        
        # Store results
        session = Session()
        # Check if record exists (created by main.py)
        result = session.query(SubmissionResult).filter_by(
            id=submission_data.get('job_id')
        ).first()
        
        if result:
            # Update existing record with full results
            result.status = 'completed'
            score = float(results.get('score', 0))  # Ensure numeric type
            result.score = score if score > 0 else 0.0  # Explicitly set 0.0
            logger.debug(f"Setting score to: {result.score} (type: {type(result.score)})")
            try:
                session.flush()  # Force SQLAlchemy to send changes to database
                logger.debug("Score flushed to database")
            except Exception as e:
                logger.error(f"Failed to flush score: {str(e)}")
            result.details = {
                'data_quality': vars(results.get('data_quality', {})),
                'insights': vars(results.get('insights', {})), 
                'metrics': results.get('metrics', {})
            }
            result.processed_data = results.get('cleaned_data', [])
            result.processing_time = results.get('processing_time', 0)
        else:
            # Create new record if not exists
            result = SubmissionResult(
                id=submission_data.get('job_id'),
                participant_id=submission_data.get('team_name'),
                status='completed',
                score=results.get('score', 0),
                details={
                    'data_quality': vars(results.get('data_quality', {})),
                    'insights': vars(results.get('insights', {})),
                    'metrics': results.get('metrics', {})
                },
                processed_data=results.get('cleaned_data', []),
                processing_time=results.get('processing_time', 0),
                timestamp=datetime.utcnow()
            )
            session.add(result)
        
        session.commit()
        
        return {
            'status': 'success',
            'job_id': submission_data.get('job_id'),
            'processing_time': results.get('processing_time', 0)
        }
    except Exception as e:
        logger.error(f"Failed to process submission: {str(e)}")
        
        # Update status to failed
        session = Session()
        result = session.query(SubmissionResult).filter_by(
            id=submission_data.get('job_id')
        ).first()
        
        if result:
            result.status = 'failed'
            result.details = {'error': str(e)}
            session.commit()
            logger.debug(f"After commit - Score stored: {result.score}")
        
        return {
            'status': 'error',
            'error': str(e),
            'job_id': submission_data.get('job_id')
        }
