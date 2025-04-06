import os
from datetime import datetime
import json
import argparse
import time
from collections import defaultdict
import requests
from monitoring import monitor
from dotenv import load_dotenv
load_dotenv()

@monitor(output_file="pipeline_metrics.json")
def run_pipeline(customers_path, products_path, orders_path, shipments_path, returns_path):
    # Add small delay to ensure measurable CPU usage
    time.sleep(0.5)
    """Execute pipeline and compute required metrics"""
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

def submit_to_evaluator(business_metrics, perf_metrics):
    """Submit computed metrics to evaluator"""
    evaluator_url = os.getenv('EVALUATOR_URL')
    api_key = os.getenv('API_KEY')
    
    if not evaluator_url or not api_key:
        raise ValueError("Missing required environment variables (EVALUATOR_URL, API_KEY)")
        
    headers = {
        'Content-Type': 'application/json',
        'Authorization': api_key
    }
    
    payload = {
        "business_metrics": {
            "top_5_customers_by_total_spend": business_metrics["top_5_customers_by_total_spend"],
            "top_5_products_by_revenue": business_metrics["top_5_products_by_revenue"],
            "shipping_performance_by_carrier": business_metrics["shipping_performance_by_carrier"],
            "return_reason_analysis": business_metrics["return_reason_analysis"]
        },
        "performance_metrics": {
            "duration_sec": perf_metrics["duration_sec"],
            "cpu_avg": perf_metrics["cpu_avg"],
            "memory_avg": perf_metrics["memory_avg"],
            "sample_count": perf_metrics["sample_count"],
            "status": perf_metrics["status"]
        }
    }
    
    try:
        response = requests.post(evaluator_url, json=payload, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error submitting to evaluator: {str(e)}")
        raise

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--customers', default=os.getenv('CUSTOMERS_PATH'))
    parser.add_argument('--products', default=os.getenv('PRODUCTS_PATH'))
    parser.add_argument('--orders', default=os.getenv('ORDERS_PATH'))
    parser.add_argument('--shipments', default=os.getenv('SHIPMENTS_PATH'))
    parser.add_argument('--returns', default=os.getenv('RETURNS_PATH'))
    args = parser.parse_args()

    metrics, perf_metrics = run_pipeline(
        args.customers,
        args.products,
        args.orders,
        args.shipments,
        args.returns
    )
    print("\nPerformance metrics:")
    print(json.dumps(perf_metrics, indent=2))
    
    print("Computed metrics:")
    print(json.dumps(metrics, indent=2))
    
    eval_response = submit_to_evaluator(metrics, perf_metrics)
    print("\nEvaluator response:")
    print(json.dumps(eval_response, indent=2))
