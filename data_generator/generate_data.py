import os
import json
import random
import pandas as pd
from faker import Faker

fake = Faker()

def generate_test_data(level=1):
    """
    Generate test data with specified difficulty level (1-4)
    
    Level 1: Clean data
    Level 2: Some null values
    Level 3: Duplicates + mixed formats  
    Level 4: Malformed entries + invalid data
    """
    # Generate test data based on level
    customers = []
    products = []
    orders = []
    
    complexity = {
        'null_rate': level * 0.1,
        'dup_rate': max(0, (level-2) * 0.15),
        'malform_rate': max(0, (level-3) * 0.1)
    }
    
    # Generate customers with data issues
    for i in range(10):
        cust = {
            'id': f'C{str(i+1).zfill(5)}',
            'name': fake.name() if random.random() > complexity['null_rate'] else None,
            'email': None if random.random() < complexity['null_rate'] else                     f"{fake.user_name()}{random.choice(['@','#',' '])}example.com" if                     random.random() < complexity['malform_rate'] else                     fake.email(),
            'phone': fake.phone_number() if random.random() > complexity['null_rate'] else None
        }
        customers.append(cust)
        
        # Add duplicates
        if random.random() < complexity['dup_rate']:
            customers.append(cust.copy())
            
    # Generate products
    for i in range(20):
        products.append({
            'id': f'P{str(i+1).zfill(5)}',
            'name': fake.catch_phrase(),
            'price': round(random.uniform(10,500)*(0.9 if random.random()<0.3 else 1), 2),
            'category': random.choice(['Electronics','Clothing','Home','Food'])
        })
            
    # Generate orders with possible issues
    for i in range(5 * level):
        orders.append({
            'order_id': f'O{str(i+1).zfill(5)}',
            'customer_id': random.choice([c['id'] for c in customers]),
            'products': random.sample([p['id'] for p in products], 3),
            'total': round(random.uniform(50, 1000), 2),
            'date': fake.date_between('-30d','today').isoformat()
        })
        
    return {'customers': customers, 'products': products, 'orders': orders}

def main():
    """Generate test datasets across all difficulty levels"""
    os.makedirs('test_data', exist_ok=True)
    
    for level in range(1, 5):
        # Create level directory
        level_dir = f'test_data/level_{level}'
        os.makedirs(level_dir, exist_ok=True)
        
        # Generate and save test data
        data = generate_test_data(level)
        with open(f'{level_dir}/metadata.json', 'w') as f:
            json.dump({'level': level, 'description': f'Test data level {level}'}, f, indent=2)
            
        # Save customers and products as JSON
        for dataset in ['customers', 'products']:
            with open(f'{level_dir}/{dataset}.json', 'w') as f:
                json.dump(data[dataset], f, indent=2)
                
        # Convert orders to transactions CSV format
        transactions = []
        for order in data['orders']:
            transactions.append({
                'order_id': order['order_id'],
                'cust_id': order['customer_id'],
                'product_id': order['products'][0],  # Take first product
                'quantity': random.randint(1,5),
                'total': order['total'],
                'order_date': order['date']
            })
        pd.DataFrame(transactions).to_csv(f'{level_dir}/transactions.csv', index=False)
        
        print(f"Generated Level {level} test data")

if __name__ == '__main__':
    main()
