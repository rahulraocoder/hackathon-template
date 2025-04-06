import json
import argparse
from collections import defaultdict
import requests

def analyze_data(customers_path, products_path, orders_path, shipments_path, returns_path):
    """Analyze datasets and compute required metrics"""
    # Load all data files
    with open(customers_path) as f: customers = json.load(f)
    with open(products_path) as f: products = json.load(f)
    with open(orders_path) as f: orders = json.load(f)
    with open(shipments_path) as f: shipments = json.load(f)
    with open(returns_path) as f: returns = json.load(f)

    # Convert to dicts for easy lookup
    customers_map = {c['id']: c for c in customers}
    products_map = {p['id']: p for p in products}
    
    # Compute customer spends
    customer_spends = defaultdict(float)
    for order in orders:
        customer_id = order['customer_id']
        product_id = order['product_id']
        quantity = order['quantity']
        price = products_map[product_id]['price']
        customer_spends[customer_id] += quantity * price

    # Top 5 customers
    top_customers = sorted(
        [{'customer_id': k, 'customer_name': customers_map[k]['name'], 'total_spent': v} 
         for k,v in customer_spends.items()],
        key=lambda x: x['total_spent'], reverse=True)[:5]

    # Compute product revenues
    product_revenues = defaultdict(float)
    for order in orders:
        product_id = order['product_id']
        quantity = order['quantity']
        price = products_map[product_id]['price']
        product_revenues[product_id] += quantity * price

    # Top 5 products
    top_products = sorted(
        [{'product_id': str(k), 'product_name': products_map[k]['name'], 'total_revenue': v} 
         for k,v in product_revenues.items()],
        key=lambda x: x['total_revenue'], reverse=True)[:5]

    # Shipping performance per carrier
    shipping_stats = defaultdict(lambda: {
        'total_shipments': 0,
        'on_time_deliveries': 0,
        'delayed_shipments': 0  
    })
    for shipment in shipments:
        carrier = shipment['carrier']
        shipping_stats[carrier]['total_shipments'] += 1
        if shipment['status'] == 'delivered':
            shipping_stats[carrier]['on_time_deliveries'] += 1
        elif shipment.get('late'):
            shipping_stats[carrier]['delayed_shipments'] += 1

    shipping_metrics = []
    for carrier, stats in shipping_stats.items():
        stats['carrier'] = carrier
        stats['undelivered_shipments'] = stats['total_shipments'] - stats['on_time_deliveries'] - stats['delayed_shipments']
        shipping_metrics.append(stats)

    # Return analysis
    return_reasons = defaultdict(lambda: {'count': 0, 'total_refund': 0.0})
    for ret in returns:
        return_reasons[ret['reason']]['count'] += 1
        return_reasons[ret['reason']]['total_refund'] += ret['refund']

    return_metrics = []
    for reason, stats in return_reasons.items():
        return_metrics.append({
            'reason': reason,
            'total_returns': stats['count'],
            'total_refund_amount': stats['total_refund']
        })

    return {
        'top_5_customers_by_total_spend': top_customers,
        'top_5_products_by_revenue': top_products,
        'shipping_performance_by_carrier': shipping_metrics,
        'return_reason_analysis': return_metrics
    }

def submit_to_evaluator(metrics, evaluator_url='http://localhost:9003'):
    """Submit computed metrics to evaluator"""
    endpoint = f"{evaluator_url}/api/submit"
    headers = {'Content-Type': 'application/json'}
    data = {
        'team_key': 'team_participant',
        'metrics': metrics
    }
    response = requests.post(endpoint, json=data, headers=headers)
    return response.json()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--customers', required=True)
    parser.add_argument('--products', required=True)
    parser.add_argument('--orders', required=True)
    parser.add_argument('--shipments', required=True)
    parser.add_argument('--returns', required=True)
    args = parser.parse_args()

    metrics = analyze_data(
        args.customers,
        args.products,
        args.orders,
        args.shipments,
        args.returns
    )
    
    print("Computed metrics:")
    print(json.dumps(metrics, indent=2))
    
    eval_response = submit_to_evaluator(metrics)
    print("\nEvaluator response:")
    print(json.dumps(eval_response, indent=2))
