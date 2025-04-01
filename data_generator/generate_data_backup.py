import os
import json
import datetime
import random
from faker import Faker
import pandas as pd
import numpy as np

# Initialize Faker
faker = Faker()
current_date = datetime.datetime.now().date()

def default_serializer(obj):
    if isinstance(obj, datetime.date):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")

# Generate Customer data
def generate_customers(num=10, level=1):
    """Generate customer data with varying difficulty levels"""
    customers = []
    for i in range(num):
        cust_id = str(i+1).zfill(6)
        # Introduce data quality issues based on level
        if random.random() < level * 0.1:  # 10% chance of null per level
            name = None
            email = None
        else:
            name = faker.name()
            email = f"{name.lower().replace(' ', '.')}@{faker.domain_name()}"
            
        # Random duplicates for higher levels
        if level > 1 and random.random() < level * 0.15:
            cust_id = str(random.randint(1, num)).zfill(6)
            
        phone = faker.phone_number() if random.random() > level * 0.15 else None

        customers.append({
            'cust_id': cust_id,
            'name': name,
            'email': email,
            'phone': phone
        })
    return customers

# Generate Product data
def generate_products(num=20):
    products = []
    for i in range(num):
        product_id = str(i+1).zfill(6)
        name = faker.name()
        category = random.choice(['Electronics', 'Fashion', 'Home & Living', 'Food & Beverage'])
        price = random.uniform(15.0, 300.0)
        stock = random.randint(1, 10)

        products.append({
            'product_id': product_id,
            'name': name,
            'category': category,
            'price': price,
            'stock': stock
        })
    return products

# Generate Order data
def generate_orders(customers, products, num=5):
    orders = []
    for i in range(num):
        order_id = str(i+1).zfill(6)
        cust_id = random.choice([c['cust_id'] for c in customers])
        selected_products = random.sample(products, k=3)  # Generate 3 random products
        product_ids = [p['product_id'] for p in selected_products]
        quantity = [random.randint(1,5) for _ in range(len(product_ids))]
        total = sum(q * get_product_price(pid, products) for q, pid in zip(quantity, product_ids))
        
        orders.append({
            'order_id': order_id,
            'cust_id': cust_id,
            'product_ids': product_ids,
            'quantity': quantity,
            'total': round(total, 2),
            'order_date': current_date - datetime.timedelta(days=random.randint(1,30))
        })
    return orders

# Function to get product price based on prod_id
def get_product_price(product_id, products):
    for p in products:
        if p['product_id'] == product_id:
            return p['price']
    return 0.0

# Generate Line Items data
def generate_line_items(orders, products, num=50):
    line_items = []
    for order in orders:
        for product_id, qty in zip(order['product_ids'], order['quantity']):
            product = next((p for p in products if p['product_id'] == product_id), None)
            line_item_id = str(order['order_id']) + '_' + str(product_id)
            uin = str(random.randint(1e6, 1e7))  # Unique Identifier
            description = faker.sentence()
            unit_price = get_product_price(product_id, products) if product_id else 0
            total = qty * unit_price
            tax = random.uniform(0.05, 0.15)  # Random tax rate
            discounted_price = unit_price * (1 - tax)
            
            line_items.append({
                'line_item_id': line_item_id,
                'product_id': product_id,
                'uin': uin,
                'description': description,
                'unit_price': unit_price,
                'qty': qty,
                'discounted_price': discounted_price,
                'tax': tax,
                'total': total
            })
    return line_items

# Generate Shipments data
def generate_shipments(orders, products, num=10):
    shipments = []
    for order in orders:
        shipment_id = str(order['order_id']) + '_ship'
        cust_id = order['cust_id']
        product_id = random.choice([p['product_id'] for p in products])
        city = faker.city()
        country = faker.country()
        postcode = f"{random.randint(10000, 99999)}{random.choice(['', ' ', '-'])}{random.randint(10000, 99999)}"
        status = random.choice(['pending', 'DELIVERED', 'SHIPPED'])
        tracking_number = str(random.randint(1e6, 1e7)) + '_SHIP'

        shipments.append({
            'shipment_id': shipment_id,
            'order_id': order['order_id'],
            'cust_id': cust_id,
            'product_id': product_id,
            'city': city,
            'country': country,
            'postcode': postcode,
            'status': status,
            'tracking_number': tracking_number
        })
    return shipments

# Generate Vouchers data (optional)
def generate_vouchers(num=10):
    vouchers = []
    for i in range(num):
        voucher_id = str(i+1).zfill(6)
        type = random.choice(['discount', 'gift_card'])
        amount = random.uniform(50.0, 200.0)
        expiry_date = datetime.datetime.now().date() + datetime.timedelta(days=random.randint(30, 90))
        
        vouchers.append({
            'voucher_id': voucher_id,
            'type': type,
            'amount': amount,
            'expiry_date': expiry_date
        })
    return vouchers

# Main function to execute data generation
def generate_complex_data(level=1):
    """Generate data with complexity based on level (1-3)"""
    num_customers = 10 * level
    num_products = 20 * level
    num_orders = 5 * level**2
    
    # Generate base data
    customers = generate_customers(num_customers)
    products = generate_products(num_products)
    orders = generate_orders(customers, products, num_orders)
    
    # Add complexity based on level
    if level > 1:
        # Add seasonality and promotions
        for product in products:
            if random.random() > 0.7:  # 30% of products have promotions
                product['promo_start'] = (datetime.now() - timedelta(days=random.randint(1,30))).isoformat()
                product['promo_end'] = (datetime.now() + timedelta(days=random.randint(1,30))).isoformat()
                product['discount'] = round(random.uniform(0.1, 0.3), 2)
    
    if level > 2:
        # Add data quality issues
        for customer in random.sample(customers, int(len(customers)*0.2)):
            if random.random() > 0.5:
                customer['email'] = None
            else:
                customer['phone'] = ''
    
    return customers, products, orders

def main():
    level = int(os.getenv('DATA_LEVEL', 1))
    
    # Generate data
    customers, products, orders = generate_complex_data(level)
    
    # Save as JSON
    with open('/data/customers.json', 'w') as f:
        json.dump(customers, indent=2, ensure_ascii=False, fp=f, default=default_serializer)

    # Generate products
    print("Generating Product Data...")
    products = generate_products(num=20)
    with open('/data/products.json', 'w') as f:
        json.dump(products, indent=2, ensure_ascii=False, fp=f, default=default_serializer)

    # Generate orders
    print("Generating Order Data...")
    orders = generate_orders(customers, products, num=5)
    with open('/data/orders.json', 'w') as f:
        json.dump(orders, indent=2, ensure_ascii=False, fp=f, default=default_serializer)

    # Generate line items
    print("Generating Line Items...")
    line_items = generate_line_items(orders, products, num=50)
    with open('/data/line_items.json', 'w') as f:
        json.dump(line_items, indent=2, ensure_ascii=False, fp=f, default=default_serializer)

    # Generate shipments
    print("Generating Shipments...")
    shipments = generate_shipments(orders, products, num=10)
    with open('/data/shipments.json', 'w') as f:
        json.dump(shipments, indent=2, ensure_ascii=False, fp=f, default=default_serializer)

    # Generate vouchers (optional)
    print("Generating Vouchers...")
    vouchers = generate_vouchers(num=5)
    with open('/data/vouchers.json', 'w') as f:
        json.dump(vouchers, indent=2, ensure_ascii=False, fp=f, default=default_serializer)
    
    # Also save as parquet for Spark
    pd.DataFrame(products).to_parquet('/data/products.parquet')
    pd.DataFrame([
        {**order, 'product_id': pid, 'quantity': qty}
        for order in orders
        for pid, qty in zip(order['product_ids'], order['quantity'])
    ]).to_csv('/data/transactions.csv', index=False)

if __name__ == "__main__":
    main()
