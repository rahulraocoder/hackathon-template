from pyspark.sql.functions import col

SCHEMAS = {
    "products": ["product_id", "name", "price", "category"],
    "transactions": ["product_id", "quantity", "total", "order_date"],
    "customers": ["cust_id", "name", "email", "phone"]
}

def validate_schema(df, dataset_name):
    """Validate DataFrame against expected schema"""
    missing = set(SCHEMAS[dataset_name]) - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns in {dataset_name}: {missing}")
    return True

def data_quality_check(df):
    """Perform data quality validations"""
    # Check for nulls in key fields
    for column_name in ['product_id', 'quantity', 'total']:
        if df.filter(col(column_name).isNull()).count() > 0:
            print(f"Warning: Null values found in {column_name}")
    
    # Validate numeric ranges
    if 'quantity' in df.columns:
        invalid_qty = df.filter((col('quantity') <= 0) | (col('quantity') > 10))
        if invalid_qty.count() > 0:
            print(f"Warning: {invalid_qty.count()} invalid quantities")
    
    return df

def log_execution_time(task_name):
    """Decorator to log execution time"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            start = time.time()
            result = func(*args, **kwargs)
            print(f"{task_name} took {time.time()-start:.2f}s")
            return result
        return wrapper
    return decorator