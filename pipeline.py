from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, avg, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType
import json
from datetime import datetime
import logging
from models import DataQualityReport, BusinessInsights

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataPipeline:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("RetailDataPipeline") \
            .config("spark.driver.host", "localhost") \
            .config("spark.driver.bindAddress", "127.0.0.1") \
            .config("spark.ui.enabled", "false") \
            .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
            .getOrCreate()
        
        # Define expected schemas
        self.schemas = {
            "customers": StructType([
                StructField("id", StringType(), False),
                StructField("name", StringType(), True),
                StructField("email", StringType(), True),
                StructField("address", StringType(), True),
                StructField("phone", StringType(), True)
            ]),
            "products": StructType([
                StructField("id", StringType(), False),
                StructField("name", StringType(), True),
                StructField("category", StringType(), True),
                StructField("price", FloatType(), True),
                StructField("stock", IntegerType(), True)
            ]),
            "orders": StructType([
                StructField("id", StringType(), False),
                StructField("customer_id", StringType(), True),
                StructField("product_id", StringType(), True),
                StructField("quantity", IntegerType(), True),
                StructField("date", DateType(), True)
            ]),
            "shipments": StructType([
                StructField("id", StringType(), False),
                StructField("order_id", StringType(), True),
                StructField("carrier", StringType(), True),
                StructField("tracking", StringType(), True),
                StructField("status", StringType(), True)
            ]),
            "returns": StructType([
                StructField("id", StringType(), False),
                StructField("order_id", StringType(), True),
                StructField("product_id", StringType(), True),
                StructField("reason", StringType(), True),
                StructField("refund", FloatType(), True)
            ])
        }

    def process_data(self, customers_path, products_path, orders_path, shipments_path, returns_path):
        """Main pipeline method that processes all datasets"""
        logger.info("Starting data processing pipeline")
        
        # Load and clean each dataset
        customers = self._clean_customers(customers_path)
        products = self._clean_products(products_path)
        orders = self._clean_orders(orders_path)
        shipments = self._clean_shipments(shipments_path)
        returns = self._clean_returns(returns_path)
        
        # Generate data quality report
        dq_report = self._generate_data_quality_report(
            customers, products, orders, shipments, returns
        )
        
        # Generate business insights
        insights = self._generate_business_insights(
            customers, products, orders, shipments, returns
        )
        
        # Join all data for cleaned output
        enriched_orders = self._enrich_orders(
            orders, customers, products, shipments, returns
        )
        
        return {
            "data_quality": dq_report,
            "insights": insights,
            "cleaned_data": enriched_orders.toJSON().collect()
        }

    def _clean_customers(self, path):
        """Clean customers data"""
        df = self.spark.read.json(path, schema=self.schemas["customers"])
        
        # Handle missing emails
        df = df.withColumn("email", when(col("email").isNull(), "unknown@example.com").otherwise(col("email")))
        
        # Standardize phone formats
        df = df.withColumn("phone", 
            when(col("phone").rlike(r"^\d{10}$"), col("phone")) \
            .when(col("phone").rlike(r"^\+91\d{10}$"), col("phone").substr(4, 10)) \
            .otherwise("0000000000")
        )
        
        # Remove duplicates
        df = df.dropDuplicates(["id"])
        
        return df

    def _clean_products(self, path):
        """Clean products data"""
        df = self.spark.read.json(path, schema=self.schemas["products"])
        
        # Fill missing categories
        df = df.withColumn("category", 
            when(col("category").isNull(), "uncategorized").otherwise(col("category")))
        
        # Fix negative stock
        df = df.withColumn("stock", 
            when(col("stock") < 0, 0).otherwise(col("stock")))
        
        # Validate prices
        df = df.filter(col("price") > 0)
        
        return df

    def _clean_orders(self, path):
        """Clean orders data"""
        df = self.spark.read.json(path, schema=self.schemas["orders"])
        
        # Validate quantities
        df = df.filter(col("quantity") > 0)
        
        # Validate dates
        df = df.filter(col("date").isNotNull())
        
        return df

    def _clean_shipments(self, path):
        """Clean shipments data"""
        df = self.spark.read.json(path, schema=self.schemas["shipments"])
        
        # Validate tracking numbers
        df = df.filter(col("tracking").isNotNull())
        
        return df

    def _clean_returns(self, path):
        """Clean returns data"""
        df = self.spark.read.json(path, schema=self.schemas["returns"])
        
        # Validate refund amounts
        df = df.filter(col("refund") >= 0)
        
        return df

    def _generate_data_quality_report(self, customers, products, orders, shipments, returns):
        """Generate data quality metrics"""
        report = {
            "missing_values": {},
            "invalid_records": {},
            "schema_violations": {}
        }
        
        # Calculate missing values
        report["missing_values"]["customers_email"] = customers.filter(col("email") == "unknown@example.com").count()
        report["missing_values"]["products_category"] = products.filter(col("category") == "uncategorized").count()
        
        # Calculate invalid records (already filtered in cleaning)
        original_counts = {
            "customers": self.spark.read.json("sample_data/customers.json").count(),
            "products": self.spark.read.json("sample_data/products.json").count(),
            "orders": self.spark.read.json("sample_data/orders.json").count(),
            "shipments": self.spark.read.json("sample_data/shipments.json").count(),
            "returns": self.spark.read.json("sample_data/returns.json").count()
        }
        
        report["invalid_records"]["customers"] = original_counts["customers"] - customers.count()
        report["invalid_records"]["products"] = original_counts["products"] - products.count()
        report["invalid_records"]["orders"] = original_counts["orders"] - orders.count()
        report["invalid_records"]["shipments"] = original_counts["shipments"] - shipments.count()
        report["invalid_records"]["returns"] = original_counts["returns"] - returns.count()
        
        return DataQualityReport.parse_obj(report)

    def _generate_business_insights(self, customers, products, orders, shipments, returns):
        """Generate business insights"""
        insights = {}
        
        # Debug data types
        logger.info(f"Orders schema: {orders.schema}")
        logger.info(f"Products schema: {products.schema}")
        
        # Top customers by spend
        customer_spend = orders.join(customers, orders.customer_id == customers.id) \
            .join(products, orders.product_id == products.id) \
            .withColumn("quantity", col("quantity").cast("integer")) \
            .withColumn("price", col("price").cast("float")) \
            .groupBy("customer_id", customers["name"].alias("customer_name")) \
            .agg(sum(col("quantity") * col("price")).alias("total_spend")) \
            .orderBy("total_spend", ascending=False) \
            .limit(5)
        
        insights["top_customers"] = [
            {"customer_id": row["customer_id"], "name": row["name"], "total_spend": row["total_spend"]}
            for row in customer_spend.collect()
        ]
        
        # Top products by revenue
        product_revenue = orders.join(products, orders.product_id == products.id) \
            .withColumn("quantity", col("quantity").cast("integer")) \
            .withColumn("price", col("price").cast("float")) \
            .groupBy("product_id", products["name"].alias("product_name")) \
            .agg(sum(col("quantity") * col("price")).alias("revenue")) \
            .orderBy("revenue", ascending=False) \
            .limit(5)
        
        insights["top_products"] = [
            {"product_id": row["product_id"], "name": row["name"], "revenue": row["revenue"]}
            for row in product_revenue.collect()
        ]
        
        # Shipping performance not available in current dataset
        
        # Return analysis
        return_reasons = returns.groupBy("reason") \
            .agg(count("*").alias("count")) \
            .orderBy("count", ascending=False)
        
        insights["return_analysis"] = {
            row["reason"]: row["count"]
            for row in return_reasons.collect()
        }
        
        return BusinessInsights.parse_obj(insights)

    def _enrich_orders(self, orders, customers, products, shipments, returns):
        """Create enriched orders dataset"""
        return orders.join(customers, orders.customer_id == customers.id) \
            .join(products, orders.product_id == products.id) \
            .join(shipments, orders.id == shipments.order_id) \
            .join(returns, (orders.id == returns.order_id) & (orders.product_id == returns.product_id), "left_outer") \
            .select(
                orders["id"],
                orders["customer_id"],
                orders["product_id"],
                orders["quantity"].cast("integer").alias("quantity"),
                orders["date"],
                customers["name"].alias("customer_name"),
                products["name"].alias("product_name"),
                products["price"].cast("float").alias("price"),
                shipments["carrier"],
                shipments["tracking"],
                shipments["status"].alias("shipment_status"),
                returns["reason"].alias("return_reason"),
                returns["refund"].cast("float").alias("refund")
            )
