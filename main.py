from fastapi import FastAPI, HTTPException, UploadFile, File
import json
import logging
from evaluate import Evaluator
from models import DataQualityReport, BusinessInsights, EvaluationResponse

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

evaluator = Evaluator()

app = FastAPI(
    title="Retail Data Pipeline Hackathon Evaluator",
    description="API for evaluating participant submissions"
)


# Evaluation Endpoints
@app.post("/evaluate", response_model=EvaluationResponse)
async def evaluate_submission(
    data_quality: UploadFile = File(...),
    insights: UploadFile = File(...),
    cleaned_data: UploadFile = File(...)
):
    """Main evaluation endpoint that validates participant submissions"""
    try:
        # Save uploaded files temporarily
        with open("temp_data_quality.json", "wb") as f:
            f.write(await data_quality.read())
        with open("temp_insights.json", "wb") as f:
            f.write(await insights.read())
        with open("temp_cleaned_data.json", "wb") as f:
            f.write(await cleaned_data.read())
            
        # For evaluation, we already have processed outputs
        pipeline = DataPipeline()
        result = {
            "data_quality": DataQualityReport.parse_file("temp_data_quality.json"),
            "insights": BusinessInsights.parse_file("temp_insights.json"),
            "cleaned_data": json.load(open("temp_cleaned_data.json")),
            "processing_time": 0  # Not tracking time for submissions
        }
        
        # Evaluate with processing time
        return await evaluator.evaluate(
            data_quality=UploadFile(filename="data_quality.json", file=open("temp_data_quality.json")),
            insights=UploadFile(filename="insights.json", file=open("temp_insights.json")),
            cleaned_data=UploadFile(filename="cleaned_data.json", file=open("temp_cleaned_data.json")),
            processing_time=result.get("processing_time")
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        # Clean up temp files
        import os
        for f in ["temp_data_quality.json", "temp_insights.json", "temp_cleaned_data.json"]:
            try:
                os.remove(f)
            except:
                pass

# Required Endpoints (participants must implement these)
@app.get("/api/schema")
async def get_schema():
    """Returns expected schema for all datasets"""
    return {
        "customers": ["id", "name", "email", "address", "phone"],
        "products": ["id", "name", "category", "price", "stock"],
        "orders": ["id", "customer_id", "product_id", "quantity", "date"],
        "shipments": ["id", "order_id", "carrier", "tracking", "status"],
        "returns": ["id", "order_id", "product_id", "reason", "refund"]
    }

from pipeline import DataPipeline

@app.post("/api/process")
async def process_data(
    customers: UploadFile = File(...),
    products: UploadFile = File(...),
    orders: UploadFile = File(...),
    shipments: UploadFile = File(...),
    returns: UploadFile = File(...)
):
    """Process raw retail data and generate reports"""
    try:
        pipeline = DataPipeline()
        
        # Save uploaded files temporarily
        with open("temp_customers.json", "wb") as f:
            f.write(await customers.read())
        with open("temp_products.json", "wb") as f:
            f.write(await products.read())
        with open("temp_orders.json", "wb") as f:
            f.write(await orders.read())
        with open("temp_shipments.json", "wb") as f:
            f.write(await shipments.read())
        with open("temp_returns.json", "wb") as f:
            f.write(await returns.read())
        
        # Process data
        result = pipeline.process_data(
            "temp_customers.json",
            "temp_products.json",
            "temp_orders.json",
            "temp_shipments.json",
            "temp_returns.json"
        )
        
        return {
            "data_quality": result["data_quality"].dict(),
            "insights": result["insights"].dict(),
            "cleaned_data": result["cleaned_data"]
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
