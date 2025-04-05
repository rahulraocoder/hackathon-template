import pandas as pd
import time
import logging
from models import DataQualityReport, BusinessInsights

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataPipeline:
    def __init__(self):
        logger.info("Initialized pandas DataPipeline")

    def process_data(self, customers_path, products_path, orders_path, shipments_path, returns_path):
        """Process all datasets with pandas"""
        start_time = time.time()
        
        try:
            # Load datasets
            customers = pd.read_json(customers_path)
            products = pd.read_json(products_path) 
            orders = pd.read_json(orders_path)
            shipments = pd.read_json(shipments_path)
            returns = pd.read_json(returns_path)

            # Validate required columns
            required = {
                'customers': ['id', 'name'],
                'products': ['id', 'name', 'price'],
                'orders': ['customer_id', 'product_id', 'quantity'],
                'shipments': ['id'],
                'returns': ['order_id', 'reason']
            }
            
            for df, name in [(customers, 'customers'), (products, 'products'),
                           (orders, 'orders'), (shipments, 'shipments'),
                           (returns, 'returns')]:
                missing = [col for col in required[name] if col not in df.columns]
                if missing:
                    raise ValueError(f"Missing columns in {name}: {missing}")

            # Merge data with explicit column naming
            merged = pd.merge(
                pd.merge(
                    orders,
                    customers.rename(columns={'name': 'customer_name'}),
                    left_on='customer_id',
                    right_on='id'
                ),
                products.rename(columns={'name': 'product_name'}),
                left_on='product_id',
                right_on='id'
            )
            
            # Calculate metrics
            merged['total_spend'] = merged['quantity'] * merged['price']
            
            # Top customers
            top_customers = (merged.groupby(['customer_id', 'customer_name'])
                           .total_spend.sum()
                           .sort_values(ascending=False)
                           .head(3)
                           .reset_index()
                           .rename(columns={'customer_name': 'name'}))
            
            # Top products
            top_products = (merged.groupby(['product_id', 'product_name'])
                          .total_spend.sum()
                          .sort_values(ascending=False)
                          .head(3)
                          .reset_index())
            
            # Return analysis
            return_reasons = returns['reason'].value_counts().to_dict()

            # Score calculation (simple version)
            score = 100  # Base score, could be enhanced
            
            return {
                "data_quality": DataQualityReport.parse_obj({
                    "missing_values": {},
                    "invalid_records": {},
                    "schema_violations": {} 
                }),
                "insights": BusinessInsights.parse_obj({
                    "top_customers": top_customers.to_dict('records'),
                    "top_products": top_products.to_dict('records'),
                    "return_analysis": return_reasons
                }),
                "score": score,
                "processing_time": time.time() - start_time
            }
            
        except Exception as e:
            logger.error(f"Processing failed: {str(e)}")
            raise
