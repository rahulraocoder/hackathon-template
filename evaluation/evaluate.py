import pandas as pd
from pyspark.sql import SparkSession
import json

class Evaluator:
    def __init__(self):
        self.spark = SparkSession.builder.getOrCreate()
    
    def run(self, expected_path, actual_path):
        """Read input files, supporting multiple formats"""
        # Read expected data as JSON
        expected = self.spark.read.json(expected_path)
            
        # Read actual results as JSON
        actual = self.spark.read.json(actual_path)
        
        return {
            "correctness": self._calculate_accuracy(expected, actual),
            "performance": self._load_performance_metrics(),
            "completeness": self._check_completeness(actual)
        }
    
    def _calculate_accuracy(self, expected, actual):
        """Compare key metrics within 1% tolerance"""
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
        """Measure performance metrics from Spark UI"""
        import time
        start_time = time.time()
        
        # Get memory usage (simplified example)
        memory_used = self.spark.sparkContext.statusTracker().getExecutorInfos() \
            .map(lambda x: x.usedMemory()).sum() / (1024 * 1024)  # in MB
            
        return {
            "execution_time": time.time() - start_time,
            "memory_used_mb": f"{memory_used:.2f}",
            "cpu_cores_used": self.spark.sparkContext.defaultParallelism
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
    evaluator = Evaluator()
    
    if len(sys.argv) != 3:
        print("Usage: evaluate.py <expected_path> <actual_path>")
        sys.exit(1)
        
    try:
        # Ensure output directory exists
        import os
        os.makedirs("/results", exist_ok=True)
        
        # Run evaluation
        results = evaluator.run(
            sys.argv[1],  # expected_path
            sys.argv[2]   # actual_path
        )
        
        # Save results
        with open("/results/evaluation.json", "w") as f:
            json.dump({
                "status": "success",
                "metrics": results
            }, f, indent=2)
            
        print("Evaluation completed successfully")
        
    except Exception as e:
        print(f"Evaluation failed: {str(e)}")
        with open("/results/evaluation_error.json", "w") as f:
            json.dump({
                "status": "error",
                "error": str(e)
            }, f, indent=2)