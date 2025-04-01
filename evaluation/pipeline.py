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
        # Define customer schema to match actual JSON structure
        customer_schema = StructType([
            StructField("id", StringType(), True),  # Note field is 'id' not 'cust_id'
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("phone", StringType(), True)
        ])
        
        # Read with explicit schema and multiLine mode
        customers = (self.spark.read
            .option("multiLine", True)
            .schema(customer_schema)
            .json(f"{data_path}/customers.json")
            .withColumnRenamed("id", "cust_id")  # Rename to match join key
            .select("cust_id", "name", "email", "phone")
        )
        
        validate_schema(products, "products")
        validate_schema(transactions, "transactions")
        validate_schema(customers, "customers")
        
        return products, transactions, customers
    
    def transform(self, products, transactions, customers):
        """Enhanced business logic"""
        print("\nDebugging transform step:")
        print(f"Products schema: {products.schema}")
        print(f"Transactions schema: {transactions.schema}")
        print(f"Customers schema: {customers.schema}")
        
        # Data quality checks
        transactions = data_quality_check(transactions)
        print(f"\nAfter data quality check - Transactions count: {transactions.count()}")
        
        # Debug join keys
        print("\nJoin keys verification:")
        print(f"Unique product_ids in products: {products.select('product_id').distinct().count()}")
        print(f"Unique product_ids in transactions: {transactions.select('product_id').distinct().count()}")
        print(f"Unique cust_ids in customers: {customers.select('cust_id').distinct().count()}")
        print(f"Unique cust_ids in transactions: {transactions.select('cust_id').distinct().count()}")
        
        # Join with customer data - ensure all DataFrames have matching key names
        joined = transactions.join(products, ["product_id"]) \
                    .join(customers, ["cust_id"], "left")  # Use left join to preserve all transactions
        print(f"\nAfter joins - Row count: {joined.count()}")
        if joined.count() == 0:
            print("Warning: Join produced no results!")
            print("Sample products:")
            products.show(5)
            print("Sample transactions:")
            transactions.show(5)
            print("Sample customers:")
            customers.show(5)
        
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
        print(f"\nFinal metrics count: {metrics.count()}")
        
        return metrics
    
    def save(self, df, path):
        """Save results with partitioning"""
        try:
            print(f"Attempting to save metrics to {path}/metrics")
            print(f"DataFrame schema: {df.schema}")
            print(f"Row count before save: {df.count()}")
            
            df.write.mode("overwrite") \
               .partitionBy("category") \
               .parquet(f"{path}/metrics")
               
            print("Successfully saved metrics")
        except Exception as e:
            print(f"Failed to save metrics: {str(e)}")
            raise

if __name__ == "__main__":
    import sys
    pipeline = RetailTechPipeline()
    
    input_path = sys.argv[1] if len(sys.argv) > 1 else "/data/test_data/level_1"
    output_path = sys.argv[2] if len(sys.argv) > 2 else "/results"
    
    try:
        print("Starting pipeline execution")
        products, transactions, customers = pipeline.ingest(input_path)
        print(f"Loaded {products.count()} products, {transactions.count()} transactions, {customers.count()} customers")
        
        metrics = pipeline.transform(products, transactions, customers)
        print(f"Transformed metrics contains {metrics.count()} rows")
        
        pipeline.save(metrics, output_path)
        print("Pipeline completed successfully")
    except Exception as e:
        print(f"Pipeline failed: {str(e)}")
        raise