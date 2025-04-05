import logging
import json
from pipeline import DataPipeline

# Configure basic console logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(console_formatter)
logger.addHandler(console_handler)

logger.info("Initialized console logging")

def process_submission(submission_data):
    """Standalone implementation for testing without Celery"""
    try:
        # Initialize pipeline
        logger.info("Initializing DataPipeline")
        try:
            pipeline = DataPipeline()
            logger.info("DataPipeline initialized successfully")
        except Exception as e:
            logger.error(f"Pipeline initialization failed: {str(e)}")
            raise ValueError(f"Pipeline initialization error: {str(e)}")
        
        # Convert data to dict if it's a string
        if isinstance(submission_data, str):
            try:
                submission_data = json.loads(submission_data)
                logger.info("Successfully parsed submission data")
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON data: {str(e)}")
                raise ValueError("Invalid JSON data")

        # Validate data paths
        data = submission_data.get('data', {})
        required_paths = ['customers_path', 'products_path', 'orders_path', 
                        'shipments_path', 'returns_path']
        
        missing_paths = [path for path in required_paths if path not in data]
        if missing_paths:
            raise ValueError(f"Missing required data paths: {missing_paths}")
        
        logger.info(f"Processing data with paths: {data}")

        # Process data
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
            
            return {
                'status': 'success',
                'job_id': submission_data.get('job_id'),
                'processing_time': results.get('processing_time', 0)
            }
            
        except Exception as e:
            logger.error(f"Data processing failed: {str(e)}")
            raise ValueError(f"Data processing error: {str(e)}")
            
    except Exception as e:
        logger.error(f"Failed to process submission: {str(e)}")
        return {
            'status': 'error',
            'error': str(e),
            'job_id': submission_data.get('job_id')
        }