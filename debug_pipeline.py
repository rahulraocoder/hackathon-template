import logging
from pipeline import DataPipeline
import json
from pprint import pformat

# Configure detailed logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def debug_score_calculation(total_records, valid_records):
    score = int((valid_records / total_records) * 100) if total_records > 0 else 0
    logger.debug(f"Score Calculation: {valid_records}/{total_records} = {score}%")
    return score

def run_debug():
    pipeline = DataPipeline()
    
    # Sample data paths
    data_files = {
        "customers_path": "sample_data/new_customers.json",
        "products_path": "sample_data/new_products.json",
        "orders_path": "sample_data/new_orders.json",
        "shipments_path": "sample_data/new_shipments.json", 
        "returns_path": "sample_data/new_returns.json"
    }
    
    logger.info("=== STARTING DIRECT PIPELINE EXECUTION ===")
    try:
        logger.info("Processing data files:\n" + pformat(data_files))
        
        # Run pipeline
        results = pipeline.process_data(**data_files)
        
        # Detailed output
        logger.info("\n=== PIPELINE RESULTS ===")
        logger.info(f"Final Quality Score: {results.get('score')}")
        
        logger.info("\nData Quality Metrics:")
        logger.info(json.dumps(results['data_quality'].dict(), indent=2))
        
        logger.info("\nBusiness Insights:")
        logger.info(json.dumps(results['insights'].dict(), indent=2))
        
        logger.info("\nProcessing Metrics:")
        logger.info(f"Total Records Processed: {results['metrics']['total_records']}")
        logger.info(f"Valid Records: {results['metrics']['valid_records']}")
        logger.info(f"Processing Time: {results['processing_time']:.2f}s")
        
    except Exception as e:
        logger.error("Pipeline failed:", exc_info=True)
        raise

if __name__ == "__main__":
    run_debug()
