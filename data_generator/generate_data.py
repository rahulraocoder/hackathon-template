import pandas as pd
import numpy as np
import json
from faker import Faker
import os

fake = Faker()

def generate_level1():
    """Basic clean data"""
    products = pd.DataFrame({
        'product_id': [f'P{i}' for i in range(100)],
        'name': [fake.word() for _ in range(100)],
        'price': np.round(np.random.uniform(5, 100, 100), 2)
    })
    
    transactions = pd.DataFrame({
        'transaction_id': [f'T{i}' for i in range(1000)],
        'product_id': np.random.choice(products['product_id'], 1000),
        'amount': np.round(np.random.uniform(10, 200, 1000), 2),
        'date': pd.date_range('2023-01-01', periods=1000, freq='H')
    })
    
    return {'products': products, 'transactions': transactions}

def generate_level2():
    """Adds missing values and duplicates"""
    data = generate_level1()
    data['transactions'].loc[::20, 'amount'] = np.nan
    data['transactions'] = pd.concat([data['transactions'], data['transactions'].sample(50)])
    return data

def save_data(data, path):
    os.makedirs(path, exist_ok=True)
    data['products'].to_parquet(f'{path}/products.parquet')
    data['transactions'].to_csv(f'{path}/transactions.csv')
    with open(f'{path}/metadata.json', 'w') as f:
        json.dump({'generated_at': pd.Timestamp.now().isoformat()}, f)

if __name__ == '__main__':
    level = os.getenv('DATA_LEVEL', '1')
    # In generate_data.py:
    output_path = f'/data/level_{level}'
    'date': pd.date_range('2023-01-01', periods=1000, freq='h')  # lowercase 'h'
    
    if level == '1':
        save_data(generate_level1(), output_path)
    elif level == '2':
        save_data(generate_level2(), output_path)
    print(f"Generated level {level} data at {output_path}")