from test_worker_impl import process_submission
import json

def test_process_submission():
    """Test the process_submission function with sample data"""
    test_data = {
        "job_id": "test123",
        "team_name": "test_team",
        "data": {
            "customers_path": "sample_data/customers.json",
            "products_path": "sample_data/products.json",
            "orders_path": "sample_data/orders.json",
            "shipments_path": "sample_data/shipments.json",
            "returns_path": "sample_data/returns.json"
        }
    }

    # Call standalone process_submission implementation
    result = process_submission(test_data)
    
    print("\nTest Results:")
    print(f"Status: {result['status']}")
    print(f"Job ID: {result['job_id']}")
    if result['status'] == 'success':
        print(f"Processing Time: {result['processing_time']}s")
    else:
        print(f"Error: {result.get('error', 'Unknown error')}")

if __name__ == "__main__":
    test_process_submission()