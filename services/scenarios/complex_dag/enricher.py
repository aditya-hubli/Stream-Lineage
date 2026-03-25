"""Complex DAG: Enricher (joins orders with users)"""
import json
from confluent_kafka import Consumer, Producer
from streamlineage import LineageEnvelope
from collections import defaultdict

producer_conf = {
    'bootstrap.servers': 'localhost:19092',
    'client.id': 'enricher-v1'
}
producer = Producer(producer_conf)

# Separate consumers for each stream
orders_consumer = Consumer({
    'bootstrap.servers': 'localhost:19092',
    'group.id': 'enricher-orders-v1',
    'auto.offset.reset': 'earliest'
})
orders_consumer.subscribe(['dag-orders'])

users_consumer = Consumer({
    'bootstrap.servers': 'localhost:19092',
    'group.id': 'enricher-users-v1',
    'auto.offset.reset': 'earliest'
})
users_consumer.subscribe(['dag-users'])

print("[COMPLEX-DAG] Enricher starting...")

user_cache = {}
order_parents = {}  # Track lineage
user_parents = {}   # Track lineage

while True:
    # Poll users (fast - to build cache)
    user_msg = users_consumer.poll(0.1)
    if user_msg and not user_msg.error():
        message_str = user_msg.value().decode('utf-8')
        data, parent_envelope = LineageEnvelope.from_json(message_str)
        user_cache[data['user_id']] = data
        if parent_envelope:
            user_parents[data['user_id']] = parent_envelope
        print(f"[ENRICHER] Cached user: {data['user_id']}")
    
    # Poll orders
    order_msg = orders_consumer.poll(0.5)
    if order_msg and not order_msg.error():
        message_str = order_msg.value().decode('utf-8')
        order_data, order_parent = LineageEnvelope.from_json(message_str)
        
        # Join with user
        user = user_cache.get(order_data['user_id'], {"country": "unknown", "tier": "unknown"})
        user_parent = user_parents.get(order_data['user_id'])
        
        enriched = {
            **order_data,
            "country": user['country'],
            "tier": user['tier'],
            "enriched": True
        }
        
        # Create envelope with BOTH parent lineages (order + user)
        parents = []
        if order_parent:
            parents.append(order_parent)
        if user_parent:
            parents.append(user_parent)
        
        envelope = LineageEnvelope(
            data=enriched,
            dataset_id="dag.enriched",
            producer_id="enricher-v1",
            operation="JOIN",
            parent_envelopes=parents if parents else None
        )
        
        producer.produce(
            topic='dag-enriched',
            value=envelope.to_json().encode('utf-8')
        )
        producer.flush()
        
        print(f"[ENRICHER] Enriched order {order_data['order_id']}: {user['country']}")
