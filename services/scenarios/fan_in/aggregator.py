"""Fan-In Pattern: Aggregator (combines all 3 sources)"""
import json
import requests
from confluent_kafka import Consumer, Producer
from streamlineage import LineageEnvelope
import time

consumer_conf = {
    'bootstrap.servers': 'localhost:19092',
    'group.id': 'fanin-aggregator-v1',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(consumer_conf)
consumer.subscribe(['fanin-clicks', 'fanin-impressions', 'fanin-conversions'])

producer_conf = {
    'bootstrap.servers': 'localhost:19092',
    'client.id': 'fanin-aggregator-v1'
}
producer = Producer(producer_conf)

print("[FAN-IN] Aggregator starting...")

stats = {
    "clicks": 0,
    "impressions": 0,
    "conversions": 0,
    "total_revenue": 0
}
parent_envelopes = []

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        continue
    
    # Extract data and parent lineage
    message_str = msg.value().decode('utf-8')
    data, parent_envelope = LineageEnvelope.from_json(message_str)
    
    # Collect parent envelopes from all 3 sources for lineage
    if parent_envelope and parent_envelope.prov_id not in [p.prov_id for p in parent_envelopes]:
        parent_envelopes.append(parent_envelope)
        # Keep only last 10 parents to avoid memory bloat
        if len(parent_envelopes) > 10:
            parent_envelopes.pop(0)
    
    source = data['source']
    
    # Update stats
    if source == "clicks":
        stats["clicks"] += 1
    elif source == "impressions":
        stats["impressions"] += 1
    elif source == "conversions":
        stats["conversions"] += 1
        stats["total_revenue"] += data.get("amount", 0)
    
    # Create aggregated event with ALL parent lineages (fan-in join)
    agg = {
        "stats": stats.copy(),
        "last_source": source,
        "timestamp": time.time()
    }
    
    envelope = LineageEnvelope(
        data=agg,
        dataset_id="fanin.combined",
        producer_id="fanin-aggregator-v1",
        operation="JOIN",
        parent_envelopes=parent_envelopes if parent_envelopes else None
    )
    
    producer.produce(
        topic='fanin-combined',
        value=envelope.to_json().encode('utf-8')
    )
    producer.flush()
    
    print(f"[AGGREGATOR] {stats}")
    
    # Also POST full envelope to API
    try:
        requests.post(
            'http://localhost:8000/api/lineage/ingest',
            json=json.loads(envelope.to_json())['lineage']
        )
    except:
        pass
