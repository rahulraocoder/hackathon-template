import requests
import time
import json
from datetime import datetime

API_BASE = "http://localhost:8000/api"

def run_test():
    print("🚀 Starting API workflow test...")
    
    # Create submission matching API's expected format EXACTLY
    submission = {
        "data": {
            "customers_path": "sample_data/customers.json",
            "products_path": "sample_data/products.json",
            "orders_path": "sample_data/orders.json",
            "shipments_path": "sample_data/shipments.json", 
            "returns_path": "sample_data/returns.json"
        },
        "team_name": "TestTeam"
    }
    
    print("📤 Submitting test job...")
    try:
        response = requests.post(
            f"{API_BASE}/submit",
            json=submission,
            headers={"Content-Type": "application/json"}
        )
        response.raise_for_status()
        job_data = response.json()
        job_id = job_data["job_id"]
        print(f"✅ Job submitted - ID: {job_id}")
        
        # Check status periodically
        for i in range(10):
            print(f"\n🔍 Checking status ({i+1}/10)...")
            response = requests.get(f"{API_BASE}/status/{job_id}")
            response.raise_for_status()
            status_data = response.json()
            
            print(f"Status: {status_data['status']}")
            if status_data["status"] == "completed":
                print("🎉 Job completed successfully!")
                print(f"Score: {status_data.get('score', 'N/A')}")
                
                # Get full results
                print("\n📊 Getting full results...")
                results = requests.get(f"{API_BASE}/results/{job_id}").json()
                print(json.dumps(results, indent=2))
                return True
                
            time.sleep(2)
        
        print("❌ Job did not complete in expected time")
        return False

    except requests.exceptions.RequestException as e:
        print(f"❌ Failed to submit job: {str(e)}")
        if hasattr(e, 'response') and e.response:
            print("Error details:", e.response.text)
        return False

if __name__ == '__main__':
    run_test()
