from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, avg, when, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType
import json
from datetime import datetime
import logging
import os
from pathlib import Path
from models import DataQualityReport, BusinessInsights

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataPipeline:
    def __init__(self):
        logger.info("Initializing SparkSession")
        try:
            self.spark = SparkSession.builder \
                .appName("RetailDataPipeline") \
                .master("local[*]") \
                .config("spark.driver.host", "worker") \
                .config("spark.driver.bindAddress", "0.0.0.0") \
                .config("spark.ui.enabled", "false") \
                .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
                .config("spark.sql.legacy.json.allowEmptyString.enabled", "true") \
                .config("spark.sql.jsonGenerator.ignoreNullFields", "false") \
                .config("spark.sql.sources.schemaStringLengthThreshold", "100000") \
                .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true") \
                .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.5.0") \
                .getOrCreate()
            logger.info("SparkSession initialized successfully")
            logger.info(f"Spark version: {self.spark.version}")
        except Exception as e:
            logger.error(f"Failed to initialize SparkSession: {str(e)}", exc_info=True)
            raise
        
        # Define expected schemas (all fields optional except id)
        self.schemas = {
            "customers": StructType([
                StructField("id", StringType(), True),
                StructField("name", StringType(), True),
                StructField("email", StringType(), True),
                StructField("address", StringType(), True),
                StructField("phone", StringType(), True)
            ]),
            "products": StructType([
                StructField("id", StringType(), True),
                StructField("name", StringType(), True),
                StructField("category", StringType(), True),
                StructField("price", FloatType(), True),
                StructField("stock", IntegerType(), True)
            ]),
            "orders": StructType([
                StructField("id", StringType(), True),
                StructField("customer_id", StringType(), True),
                StructField("product_id", StringType(), True),
                StructField("quantity", IntegerType(), True),
                StructField("date", StringType(), True)  # Changed from DateType
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
        import time
        start_time = time.time()
        logger.info("Starting data processing pipeline")
        
        try:
            # Validate all input files exist with verbose logging
            logger.info("Validating input files:")
            for path in [customers_path, products_path, orders_path, shipments_path, returns_path]:
                path_obj = Path(path)
                logger.info(f"Checking {path} - exists: {path_obj.exists()}")
                if not path_obj.exists():
                    raise ValueError(f"Input file not found: {path}")
                logger.info(f"File {path} readable: {os.access(path, os.R_OK)}")
        except Exception as e:
            logger.error(f"Input validation failed: {str(e)}", exc_info=True)
            raise
        
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
        
        processing_time = time.time() - start_time
        
        # Calculate data quality score (0-100 scale)
        total_records = (
            customers.count() +
            products.count() +
            orders.count() +
            shipments.count() +
            returns.count()
        )
        valid_records = (
            customers.filter(col("id").isNotNull()).count() +
            products.filter(col("id").isNotNull()).count() +
            orders.filter(col("id").isNotNull()).count() +
            shipments.filter(col("id").isNotNull()).count() +
            returns.filter(col("id").isNotNull()).count()
        )
        data_quality_score = int((valid_records / total_records) * 100) if total_records > 0 else 0

        return {
            "data_quality": dq_report,
            "insights": insights,
            "cleaned_data": enriched_orders.toJSON().collect(),
            "processing_time": processing_time,
            "score": data_quality_score,
            "metrics": {
                "total_records": total_records,
                "valid_records": valid_records,
                "data_quality_score": data_quality_score,
                "top_customers": insights.top_customers[:3],
                "top_products": insights.top_products[:3]
            }
        }

    def _clean_customers(self, path):
        """Clean customers data"""
        try:
            # First try reading with schema validation
            try:
                df = self.spark.read.schema(self.schemas["customers"]).json(path)
                
                # If we get here, schema validation succeeded
                corrupt_count = 0
            except Exception as schema_err:
                # If schema validation fails, fall back to permissive mode
                logger.warning("Schema validation failed, falling back to permissive mode")
                df = self.spark.read.option("mode", "PERMISSIVE") \
                    .option("columnNameOfCorruptRecord", "_corrupt_record") \
                    .json(path)
                
                # Count corrupt records
                corrupt_records = df.filter(~df["_corrupt_record"].isNull())
                corrupt_count = corrupt_records.count()
                if corrupt_count > 0:
                    logger.warning(f"Found {corrupt_count} corrupt customer records")
                
                # Get records that passed validation
                df = df.filter(df["_corrupt_record"].isNull()) \
                    .drop("_corrupt_record")
            
            # Ensure required columns exist
            for col_name in ["id", "name", "email", "phone"]:
                if col_name not in df.columns:
                    df = df.withColumn(col_name, lit(None))
            
            # Ensure required columns exist
            for col_name in ["id", "name", "email", "phone"]:
                if col_name not in df.columns:
                    df = df.withColumn(col_name, lit(None))
            
            # Apply schema with type conversion
            df = df.select(*self.schemas["customers"].fieldNames())  # Ensure proper column order
            
            # Handle missing values
            df = df.withColumn("email", when(col("email").isNull(), "unknown@example.com").otherwise(col("email")))
            df = df.withColumn("name", when(col("name").isNull(), "Unknown").otherwise(col("name")))
            
            # Standardize phone formats
            df = df.withColumn("phone", 
                when(col("phone").rlike(r"^\d{10}$"), col("phone")) \
                .when(col("phone").rlike(r"^\+91\d{10}$"), col("phone").substr(4, 10)) \
                .otherwise("0000000000")
            )
            
            # Remove duplicates
            df = df.dropDuplicates(["id"])
            
            return df
        except Exception as e:
            logger.error(f"Failed to clean customers data: {str(e)}")
            raise ValueError(f"Customer data validation failed: {str(e)}")

    def _clean_products(self, path):
        """Clean products data"""
        try:
            # First try reading with schema validation
            try:
                df = self.spark.read.schema(self.schemas["products"]).json(path)
                
                # If we get here, schema validation succeeded
                corrupt_count = 0
            except Exception as schema_err:
                # If schema validation fails, fall back to permissive mode
                logger.warning("Schema validation failed, falling back to permissive mode")
                df = self.spark.read.option("mode", "PERMISSIVE") \
                    .option("columnNameOfCorruptRecord", "_corrupt_record") \
                    .json(path)
                
                # Count corrupt records
                corrupt_records = df.filter(~df["_corrupt_record"].isNull())
                corrupt_count = corrupt_records.count()
                if corrupt_count > 0:
                    logger.warning(f"Found {corrupt_count} corrupt product records")
                
                # Get records that passed validation
                df = df.filter(df["_corrupt_record"].isNull()) \
                    .drop("_corrupt_record")
            
            # Ensure required columns exist
            for col_name in ["id", "name", "category", "price", "stock"]:
                if col_name not in df.columns:
                    df = df.withColumn(col_name, lit(None))
            
            # Apply schema with type conversion
            df = df.select(*self.schemas["products"].fieldNames())
            
            if df.count() == 0:
                logger.warning("No valid product records found")
                
            # Fill missing categories
            df = df.withColumn("category", 
                when(col("category").isNull(), "uncategorized").otherwise(col("category")))
            
            # Fix negative stock
            df = df.withColumn("stock", 
                when(col("stock") < 0, 0).otherwise(col("stock")))
            
            # Validate prices
            df = df.filter(col("price") > 0)
            
            return df
        except Exception as e:
            logger.error(f"Failed to clean products data: {str(e)}")
            raise ValueError(f"Products data validation failed: {str(e)}")

    def _clean_orders(self, path):
        """Clean orders data"""
        try:
            # First try reading with schema validation
            try:
                df = self.spark.read.schema(self.schemas["orders"]).json(path)
                
                # If we get here, schema validation succeeded
                corrupt_count = 0
            except Exception as schema_err:
                # If schema validation fails, fall back to permissive mode
                logger.warning("Schema validation failed, falling back to permissive mode")
                df = self.spark.read.option("mode", "PERMISSIVE") \
                    .option("columnNameOfCorruptRecord", "_corrupt_record") \
                    .json(path)
                
                # Count corrupt records
                corrupt_records = df.filter(~df["_corrupt_record"].isNull())
                corrupt_count = corrupt_records.count()
                if corrupt_count > 0:
                    logger.warning(f"Found {corrupt_count} corrupt order records")
                
                # Get records that passed validation
                df = df.filter(df["_corrupt_record"].isNull()) \
                    .drop("_corrupt_record")
            
            # Ensure required columns exist
            for col_name in ["id", "customer_id", "product_id", "quantity", "date"]:
                if col_name not in df.columns:
                    df = df.withColumn(col_name, lit(None))
            
            # Apply schema with type conversion
            df = df.select(*self.schemas["orders"].fieldNames())
            
            if df.count() == 0:
                logger.warning("No valid order records found")
            
            # Handle missing/negative quantities
            df = df.withColumn("quantity", 
                when((col("quantity").isNull()) | (col("quantity") <= 0), 1)
                .otherwise(col("quantity"))
            )
            
            # Handle missing dates
            df = df.withColumn("date",
                when(col("date").isNull(), "1970-01-01")
                .otherwise(col("date"))
            )
            
            return df
        except Exception as e:
            logger.error(f"Failed to clean orders data: {str(e)}")
            raise ValueError(f"Orders data validation failed: {str(e)}")

    def _clean_shipments(self, path):
        """Clean shipments data"""
        try:
            # First try reading with schema validation
            try:
                df = self.spark.read.schema(self.schemas["shipments"]).json(path)
                
                # If we get here, schema validation succeeded
                corrupt_count = 0
            except Exception as schema_err:
                # If schema validation fails, fall back to permissive mode
                logger.warning("Schema validation failed, falling back to permissive mode")
                df = self.spark.read.option("mode", "PERMISSIVE") \
                    .option("columnNameOfCorruptRecord", "_corrupt_record") \
                    .json(path)
                
                # Count corrupt records
                corrupt_records = df.filter(~df["_corrupt_record"].isNull())
                corrupt_count = corrupt_records.count()
                if corrupt_count > 0:
                    logger.warning(f"Found {corrupt_count} corrupt shipment records")
                
                # Get records that passed validation
                df = df.filter(df["_corrupt_record"].isNull()) \
                    .drop("_corrupt_record")
            
            # Ensure required columns exist
            for col_name in ["id", "order_id", "carrier", "tracking", "status"]:
                if col_name not in df.columns:
                    df = df.withColumn(col_name, lit(None))
            
            # Apply schema with type conversion
            df = df.select(*self.schemas["shipments"].fieldNames())
            
            if df.count() == 0:
                logger.warning("No valid shipment records found")
                return df
                
            original_count = df.count()
            
            # Validate tracking numbers - only remove records with null/invalid tracking
            original_count = df.count()
            df = df.filter(
                (col("tracking").isNotNull()) & 
                (col("tracking") != "invalid")
            )
            
            filtered_count = original_count - df.count()
            if filtered_count > 0:
                logger.warning(f"Filtered {filtered_count} shipments with invalid tracking numbers")
            
            return df
        except Exception as e:
            logger.error(f"Failed to clean shipments data: {str(e)}")
            raise ValueError(f"Shipments data validation failed: {str(e)}")

    def _clean_returns(self, path):
        """Clean returns data"""
        try:
            # First try reading with schema validation
            try:
                df = self.spark.read.schema(self.schemas["returns"]).json(path)
                
                # If we get here, schema validation succeeded
                corrupt_count = 0
            except Exception as schema_err:
                # If schema validation fails, fall back to permissive mode
                logger.warning("Schema validation failed, falling back to permissive mode")
                df = self.spark.read.option("mode", "PERMISSIVE") \
                    .option("columnNameOfCorruptRecord", "_corrupt_record") \
                    .json(path)
                
                # Count corrupt records
                corrupt_records = df.filter(~df["_corrupt_record"].isNull())
                corrupt_count = corrupt_records.count()
                if corrupt_count > 0:
                    logger.warning(f"Found {corrupt_count} corrupt return records")
                
                # Get records that passed validation
                df = df.filter(df["_corrupt_record"].isNull()) \
                    .drop("_corrupt_record")
            
            # Ensure required columns exist
            for col_name in ["id", "order_id", "product_id", "reason", "refund"]:
                if col_name not in df.columns:
                    df = df.withColumn(col_name, lit(None))
            
            # Apply schema with type conversion
            df = df.select(*self.schemas["returns"].fieldNames())
            
            if df.count() == 0:
                logger.warning("No valid return records found")
                return df
                
            original_count = df.count()
            
            # Validate refund amounts - only remove records with negative refunds
            original_count = df.count()
            df = df.filter(col("refund") >= 0)
            
            filtered_count = original_count - df.count()
            if filtered_count > 0:
                logger.warning(f"Filtered {filtered_count} returns with invalid refund amounts")
                
            logger.info(f"Filtered {original_count - df.count()} invalid returns")
            return df
        except Exception as e:
            logger.error(f"Failed to clean returns data: {str(e)}")
            raise ValueError(f"Returns data validation failed: {str(e)}")

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
        
        # Validate required fields exist
        required_fields = {
            "customers": ["id", "name"],
            "products": ["id", "name", "price"],
            "orders": ["customer_id", "product_id", "quantity"],
            "shipments": ["id"],
            "returns": ["order_id", "reason"]
        }
        
        for df, df_name in [(customers, "customers"), (products, "products"),
                           (orders, "orders"), (shipments, "shipments"),
                           (returns, "returns")]:
            missing = [field for field in required_fields[df_name] 
                      if field not in df.columns]
            if missing:
                raise ValueError(f"Missing required field(s) in {df_name}: {missing}")

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
