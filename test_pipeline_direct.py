from pipeline import DataPipeline
import json

def test_pipeline():
    print("Testing DataPipeline directly...")
    pipeline = DataPipeline()
    
    # Use sample data paths
    paths = {
        "customers_path": "sample_data/customers.json",
        "products_path": "sample_data/products.json",
        "orders_path": "sample_data/orders.json",
        "shipments_path": "sample_data/shipments.json",
        "returns_path": "sample_data/returns.json"
    }
    
    # Process data
    results = pipeline.process_data(**paths)
    
    # Print results
    print("\nData Quality Report:")
    print(json.dumps(results["data_quality"].dict(), indent=2))
    
    print("\nBusiness Insights:")
    print(json.dumps(results["insights"].dict(), indent=2))
    
    print(f"\nFinal Score: {results['score']}%")
    print(f"Processing Time: {results['processing_time']:.2f}s")

if __name__ == "__main__":
    test_pipeline()
