import unittest
from unittest.mock import patch, MagicMock
from pipeline import DataPipeline
from pyspark.sql import SparkSession
import tempfile
import os

class TestDataPipeline(unittest.TestCase):

    @classmethod 
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("TestPipeline") \
            .master("local[2]") \
            .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
            .config("spark.driver.host", "localhost") \
            .getOrCreate()
            
    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        self.pipeline = DataPipeline()

    def test_score_calculation_perfect_data(self):
        """Test 100% score with perfect data"""
        with patch.object(DataPipeline, '_clean_customers') as mock_clean, \
             patch.object(DataPipeline, '_clean_products') as mock_prod, \
             patch.object(DataPipeline, '_clean_orders') as mock_orders, \
             patch.object(DataPipeline, '_clean_shipments') as mock_ship, \
             patch.object(DataPipeline, '_clean_returns') as mock_returns, \
             patch('pathlib.Path.exists') as mock_exists, \
             patch('pathlib.Path.is_file') as mock_is_file, \
             patch('os.access') as mock_access:
            
            # Setup mock methods
            mock_clean.return_value = self._create_mock_df(100, 100)
            mock_prod.return_value = self._create_mock_df(100, 100)
            mock_orders.return_value = self._create_mock_df(100, 100)
            mock_ship.return_value = self._create_mock_df(100, 100)
            mock_returns.return_value = self._create_mock_df(100, 100)
            mock_exists.return_value = True
            mock_access.return_value = True
            
            results = self.pipeline.process_data(**{
                "customers_path": "dummy",
                "products_path": "dummy",
                "orders_path": "dummy",
                "shipments_path": "dummy",
                "returns_path": "dummy"
            })
            self.assertEqual(results["score"], 100)

    def _create_mock_df(self, total, valid):
        """Helper to create mock DataFrame with count/filter behavior"""
        mock_df = MagicMock()
        mock_df.count.return_value = total
        mock_df.filter().count.return_value = valid
        
        # Common columns for all datasets
        mock_df.withColumn.return_value = mock_df
        mock_df.select.return_value = mock_df
        mock_df.join.return_value = mock_df
        mock_df.groupBy.return_value = mock_df
        mock_df.agg.return_value = mock_df
        mock_df.orderBy.return_value = mock_df
        mock_df.limit.return_value = mock_df
        
        # Return concrete dicts instead of MagicMock for pydantic validation
        mock_df.collect.return_value = [{
            "id": "1",
            "name": "Test Product",
            "customer_id": "1",
            "product_id": "1",
            "quantity": 1,
            "order_id": "1",
            "reason": "defective",
            "price": 10.0,
            "total_spend": 100.0,
            "revenue": 50.0,
            "customer_name": "Test Customer",
            "product_name": "Test Product",
            "count": 1
        }]
        
        # Set columns based on dataset type
        if total == 100:  # Perfect data scenario
            mock_df.columns = [
                'id', 'name', 'email', 'phone',  # Customers
                'price', 'stock', 'category',    # Products
                'customer_id', 'product_id', 'quantity', 'date',  # Orders
                'order_id', 'carrier', 'tracking', 'status',  # Shipments
                'reason', 'refund'  # Returns
            ]
        else:  # Integration test
            mock_df.columns = [
                'id', 'name', 'email', 'phone',
                'price', 'stock', 'category',
                'customer_id', 'product_id', 'quantity', 'date',
                'order_id', 'carrier', 'tracking', 'status',
                'reason', 'refund'
            ]
            
        return mock_df

    def test_process_data_integration(self):
        """Test full process_data with sample files"""
        test_files = {
            "customers_path": "sample_data/customers.json",
            "products_path": "sample_data/products.json",
            "orders_path": "sample_data/orders.json",
            "shipments_path": "sample_data/shipments.json",
            "returns_path": "sample_data/returns.json"
        }
        
        results = self.pipeline.process_data(**test_files)
        self.assertIn("score", results)
        self.assertTrue(0 <= results["score"] <= 100)

if __name__ == '__main__':
    unittest.main()
