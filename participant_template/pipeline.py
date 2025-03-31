from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from utils import validate_schema  # Provided helper

class RetailTechPipeline:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("RetailTechPipeline") \
            .config("spark.executor.memory", "1g") \
            .getOrCreate()
    
    def ingest(self, path):
        """Read data from path"""
        products = self.spark.read.parquet(f"{path}/products.parquet")
        transactions = self.spark.read.csv(
            f"{path}/transactions.csv", 
            header=True,
            inferSchema=True
        )
        
        validate_schema(products, "products")
        validate_schema(transactions, "transactions")
        
        return products, transactions
    
    def transform(self, products, transactions):
        """Core business logic"""
        joined = transactions.join(products, "product_id")
        
        metrics = joined.groupBy("product_id", "name").agg(
            sum("amount").alias("total_sales"),
            count("*").alias("transaction_count"),
            avg("amount").alias("avg_sale_amount")
        )
        
        return metrics
    
    def save(self, df, path):
        """Save results"""
        df.write.mode("overwrite").parquet(f"{path}/output")

if __name__ == "__main__":
    import sys
    pipeline = RetailTechPipeline()
    
    input_path = sys.argv[1] if len(sys.argv) > 1 else "/data/level_1"
    output_path = sys.argv[2] if len(sys.argv) > 2 else "/data/output"
    
    products, transactions = pipeline.ingest(input_path)
    metrics = pipeline.transform(products, transactions)
    pipeline.save(metrics, output_path)