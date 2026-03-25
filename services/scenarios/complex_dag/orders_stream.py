"""Complex DAG: Orders Stream"""
import time
import json
from confluent_kafka import Producer
from streamlineage import LineageEnvelope

producer_conf = {
    'bootstrap.servers': 'localhost:19092',
    'client.id': 'orders-stream-v1'
}
producer = Producer(producer_conf)

print("[COMPLEX-DAG] Orders stream starting...")

order_id = 0
while True:
    event = {
        "order_id": f"ord-{order_id}",
        "user_id": f"usr-{order_id % 20}",
        "amount": (order_id % 10 + 1) * 100,
        "timestamp": time.time()
    }
    
    envelope = LineageEnvelope(
        data=event,
        dataset_id="dag.orders",
        producer_id="orders-stream-v1",
        operation="CREATE"
    )
    
    producer.produce(
        topic='dag-orders',
        value=envelope.to_json().encode('utf-8')
    )
    producer.flush()
    
    print(f"[ORDERS] Order {event['order_id']}: ${event['amount']}")
    order_id += 1
    time.sleep(3)
