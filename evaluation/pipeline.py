from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from utils import validate_schema, data_quality_check

class RetailTechPipeline:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("RetailTechPipeline") \
            .config("spark.executor.memory", "2g") \
            .getOrCreate()
    
    def ingest(self, path):
        """Read data with validation"""
        # Use the provided path directly
        data_path = path

        # Define explicit schema for products
        from pyspark.sql.types import StructType, StructField, StringType, DoubleType
        product_schema = StructType([
            StructField("id", StringType()),
            StructField("name", StringType()),
            StructField("category", StringType()),
            StructField("price", DoubleType())
        ])
        
        # Read with explicit schema and multiLine mode
        products = self.spark.read \
            .option("multiLine", True) \
            .schema(product_schema) \
            .json(f"{data_path}/products.json") \
            .withColumnRenamed("id", "product_id") \
            .select('product_id', 'name', 'price', 'category')
        # Define transactions schema to match the test data
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
        transaction_schema = StructType([
            StructField("order_id", StringType()),
            StructField("cust_id", StringType()),
            StructField("product_id", StringType()),
            StructField("quantity", IntegerType()),
            StructField("total", DoubleType()),
            StructField("order_date", StringType())
        ])
        
        # Read CSV with schema and header
        transactions = self.spark.read \
            .option("header", True) \
            .schema(transaction_schema) \
            .csv(f"{data_path}/transactions.csv") \
            .select("product_id", "quantity", "total", "order_date", "cust_id")
        # Define customer schema
        customer_schema = StructType([
            StructField("cust_id", StringType()),
            StructField("name", StringType()),
            StructField("email", StringType()),
            StructField("phone", StringType())
        ])
        
        # Read with explicit schema and multiLine mode
        customers = self.spark.read \
            .option("multiLine", True) \
            .schema(customer_schema) \
            .json(f"{data_path}/customers.json") \
            .select("cust_id", "name", "email", "phone")
        
        validate_schema(products, "products")
        validate_schema(transactions, "transactions")
        validate_schema(customers, "customers")
        
        return products, transactions, customers
    
    def transform(self, products, transactions, customers):
        """Enhanced business logic"""
        # Data quality checks
        transactions = data_quality_check(transactions)
        
        # Join with customer data
        joined = transactions.join(products, ["product_id"]) \
                    .join(customers, ["cust_id"])
        
        # Enhanced metrics
        metrics = joined.groupBy(
            "product_id", products["name"].alias("product_name"),
            "category", "cust_id"
        ).agg(
            sum("total").alias("total_sales"),
            count("*").alias("transaction_count"),
            avg("total").alias("avg_sale_amount"),
            first(customers["email"]).alias("customer_email"),
            first(customers["name"]).alias("customer_name")
        )
        
        return metrics
    
    def save(self, df, path):
        """Save results with partitioning"""
        df.write.mode("overwrite") \
           .partitionBy("category") \
           .parquet(f"{path}/metrics")

if __name__ == "__main__":
    import sys
    pipeline = RetailTechPipeline()
    
    input_path = sys.argv[1] if len(sys.argv) > 1 else "/data/level_1"
    output_path = sys.argv[2] if len(sys.argv) > 2 else "/results"
    
    products, transactions, customers = pipeline.ingest(input_path)
    metrics = pipeline.transform(products, transactions, customers)
    pipeline.save(metrics, output_path)