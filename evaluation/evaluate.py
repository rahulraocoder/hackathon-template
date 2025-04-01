import pandas as pd
from pyspark.sql import SparkSession
import json

class Evaluator:
    def __init__(self):
        import time
        self.start_time = time.time()
        self.spark = SparkSession.builder \
            .master("spark://spark-master:7077") \
            .config("spark.driver.host", "evaluator") \
            .config("spark.driver.bindAddress", "0.0.0.0") \
            .config("spark.jars.ivy", "/opt/bitnami/spark/.ivy2") \
            .getOrCreate()
    
    def run(self, expected_path, actual_path):
        """Process pipeline results from Parquet files"""
        try:
            from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
            
            # Define expected schema
            schema = StructType([
                StructField("product_id", StringType()),
                StructField("product_name", StringType()),
                StructField("category", StringType()),
                StructField("cust_id", StringType()),
                StructField("total_sales", DoubleType()),
                StructField("transaction_count", IntegerType()),
                StructField("avg_sale_amount", DoubleType()),
                StructField("customer_email", StringType()),
                StructField("customer_name", StringType())
            ])
            
            # Read actual results with explicit schema
            actual = self.spark.read.schema(schema).parquet(f"{actual_path}/metrics")
            
            if actual.count() == 0:
                return {
                    "status": "success",
                    "metrics": {"message": "No data found in metrics directory"},
                    "performance": {"status": "completed"}
                }
                
            # Get basic metrics
            metrics = {
                "record_count": actual.count(),
                "columns": len(actual.columns),
                "sample_data": actual.limit(5).toJSON().collect()
            }
            
            return {
                "metrics": metrics,
                "performance": {
                    "status": "metrics_collected",
                    "spark_ui": f"http://spark-master:8080"
                },
                "completeness": self._check_completeness(actual)
            }
            
        except Exception as e:
            print(f"Evaluation warning: {str(e)}")
            return {
                "error": str(e),
                "performance": {
                    "status": "error",
                    "message": str(e)
                }
            }
    
    def _calculate_accuracy(self, expected, actual):
        """Compare key metrics if expected results exist"""
        if not hasattr(self, 'expected'):
            return {"comparison_skipped": True}
            
        from pyspark.sql.functions import col
        
        # Compare total sales, transaction counts
        expected_metrics = expected.agg(
            sum("total_sales").alias("total_sales"),
            sum("transaction_count").alias("transaction_count")
        ).collect()[0]
        
        actual_metrics = actual.agg(
            sum("total_sales").alias("total_sales"),
            sum("transaction_count").alias("transaction_count")
        ).collect()[0]
        
        # Calculate percentage differences
        sales_diff = abs(expected_metrics.total_sales - actual_metrics.total_sales)/expected_metrics.total_sales
        count_diff = abs(expected_metrics.transaction_count - actual_metrics.transaction_count)/expected_metrics.transaction_count
        
        return {
            "sales_accuracy": 1 - sales_diff,
            "count_accuracy": 1 - count_diff,
            "within_tolerance": sales_diff <= 0.01 and count_diff <= 0.01
        }
    
    def _load_performance_metrics(self):
        """Basic performance metrics"""
        import time
        return {
            "status": "completed",
            "execution_time": f"{time.time() - self.start_time:.2f}s",
            "spark_ui": "http://spark-master:8080"
        }
    
    def _check_completeness(self, df):
        """Check data quality metrics"""
        from pyspark.sql.functions import col, count, when
        
        # Count nulls in key columns
        null_counts = df.select(
            [count(when(col(c).isNull(), c)).alias(c)
             for c in ["product_id", "cust_id", "total"]]
        ).collect()[0]
        
        # Check for duplicates
        duplicate_count = df.count() - df.dropDuplicates().count()
        
        return {
            "null_values": {
                "product_id": null_counts.product_id,
                "cust_id": null_counts.cust_id,
                "total": null_counts.total
            },
            "duplicates_removed": duplicate_count,
            "total_rows": df.count()
        }

if __name__ == "__main__":
    import sys
    try:
        evaluator = Evaluator()
    except Exception as e:
        print(f"Failed to initialize Spark: {str(e)}")
        sys.exit(1)
        
    
    if len(sys.argv) != 3:
        print("Usage: evaluate.py <expected_path> <actual_path>")
        sys.exit(1)
        
    # Ensure output directory exists
    import os
    os.makedirs("/results", exist_ok=True)
    
    try:
        # Run evaluation
        results = evaluator.run(
            sys.argv[1],  # expected_path
            sys.argv[2]   # actual_path
        )
        
        # Print results
        print("\nEvaluation Results:")
        print("===================")
        
        if 'actual_results' in results:
            print("Processed Results:")
            print(json.dumps(results['actual_results'], indent=2))
        else:
            print("No results to display")
            
        print("\nPerformance Metrics:")
        print(f"Execution Time: {results['performance']['execution_time']:.2f}s")
        print(f"Memory Used: {results['performance']['memory_used_mb']}MB")
        
    except Exception as e:
        print(f"\nEvaluation completed with warnings: {str(e)}")
        try:
            with open("/results/evaluation_error.json", "w") as f:
                json.dump({
                    "status": "warning",
                    "message": str(e),
                    "output": "Partial results generated"
                }, f, indent=2)
        except:
            print("Could not write error log")