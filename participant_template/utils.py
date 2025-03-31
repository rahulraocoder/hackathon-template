SCHEMAS = {
    "products": ["product_id", "name", "price"],
    "transactions": ["transaction_id", "product_id", "amount", "date"]
}

def validate_schema(df, dataset_name):
    """Validate DataFrame against expected schema"""
    missing = set(SCHEMAS[dataset_name]) - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns in {dataset_name}: {missing}")
    return True

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