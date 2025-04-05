import json
import random
from faker import Faker
from datetime import datetime, timedelta

fake = Faker()

def generate_customers(count=5):
    customers = []
    for i in range(count):
        customer = {
            "id": f"cust{i}",
            "name": fake.name(),
            "email": fake.email(),
            "address": fake.address().replace("\n", ", "),
            "phone": f"+91-{random.randint(9000000000, 9999999999)}"  # Valid Indian format
        }
        customers.append(customer)
    return customers

def generate_products(count=5):
    products = []
    categories = ["Electronics", "Clothing", "Groceries", "Furniture", "Books"]
    for i in range(count):
        product = {
            "id": i,
            "name": fake.word().capitalize(),
            "category": random.choice(categories),
            "price": round(random.uniform(1, 1000), 2),
            "stock": random.randint(1, 100)
        }
        products.append(product)
    return products

def generate_orders(customer_ids, product_ids, count=5):
    orders = []
    for i in range(count):
        order_date = (datetime.now() - timedelta(days=random.randint(0, 30))).strftime("%Y-%m-%d")
        order = {
            "id": f"order{i}",
            "customer_id": random.choice(customer_ids),
            "product_id": random.choice(product_ids),
            "quantity": random.randint(1, 10),
            "date": order_date
        }
        orders.append(order)
    return orders

def generate_shipments(order_ids, count=5):
    shipments = []
    statuses = ["shipped", "delivered", "processing", "returned"]
    carriers = ["FedEx", "UPS", "DHL", "USPS"]
    
    for i in range(count):
        shipment = {
            "id": f"ship{i}",
            "order_id": random.choice(order_ids),
            "carrier": random.choice(carriers),
            "tracking": fake.uuid4(),
            "status": random.choice(statuses)
        }
        shipments.append(shipment)
    return shipments

def generate_returns(order_ids, product_ids, count=5):
    returns = []
    reasons = ["defective", "wrong item", "changed mind", "late delivery"]
    
    for i in range(count):
        return_data = {
            "id": f"ret{i}",
            "order_id": random.choice(order_ids),
            "product_id": random.choice(product_ids),
            "reason": random.choice(reasons),
            "refund": round(random.uniform(1, 500), 2)
        }
        returns.append(return_data)
    return returns

def save_data():
    customers = generate_customers()
    products = generate_products()
    orders = generate_orders(
        [c["id"] for c in customers],
        [p["id"] for p in products],
    )
    shipments = generate_shipments([o["id"] for o in orders])
    returns = generate_returns(
        [o["id"] for o in orders],
        [p["id"] for p in products]
    )

    # Create sample_data directory if it doesn't exist
    import os
    os.makedirs("sample_data", exist_ok=True)

    with open("sample_data/customers.json", "w") as f:
        json.dump(customers, f, indent=2)
    with open("sample_data/products.json", "w") as f:
        json.dump(products, f, indent=2)
    with open("sample_data/orders.json", "w") as f:
        json.dump(orders, f, indent=2)
    with open("sample_data/shipments.json", "w") as f:
        json.dump(shipments, f, indent=2)
    with open("sample_data/returns.json", "w") as f:
        json.dump(returns, f, indent=2)

if __name__ == "__main__":
    save_data()
    print("Generated clean test data (5 records each) in sample_data/ directory")
