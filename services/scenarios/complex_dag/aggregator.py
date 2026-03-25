"""Complex DAG: Aggregator (by country + risk level)"""
import json
from confluent_kafka import Consumer, Producer
from streamlineage import LineageEnvelope
from collections import defaultdict

consumer_conf = {
    'bootstrap.servers': 'localhost:19092',
    'group.id': 'dag-aggregator-v1',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(consumer_conf)
consumer.subscribe(['dag-risk-scored'])

producer_conf = {
    'bootstrap.servers': 'localhost:19092',
    'client.id': 'dag-aggregator-v1'
}
producer = Producer(producer_conf)

print("[COMPLEX-DAG] Aggregator starting...")

stats = defaultdict(lambda: {"count": 0, "total_amount": 0})

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        continue
    
    # Extract data and parent lineage
    message_str = msg.value().decode('utf-8')
    data, parent_envelope = LineageEnvelope.from_json(message_str)
    
    key = f"{data['country']}-{data['risk_level']}"
    stats[key]["count"] += 1
    stats[key]["total_amount"] += data['amount']
    
    agg = {
        "country": data['country'],
        "risk_level": data['risk_level'],
        "order_count": stats[key]["count"],
        "total_amount": stats[key]["total_amount"],
        "avg_amount": stats[key]["total_amount"] / stats[key]["count"]
    }
    
    envelope = LineageEnvelope(
        data=agg,
        dataset_id="dag.aggregated",
        producer_id="dag-aggregator-v1",
        operation="AGGREGATE",
        parent_envelopes=[parent_envelope] if parent_envelope else None
    )
    
    producer.produce(
        topic='dag-aggregated',
        value=envelope.to_json().encode('utf-8')
    )
    producer.flush()
    
    print(f"[AGGREGATOR] {data['country']} {data['risk_level']}: {stats[key]['count']} orders, ${stats[key]['total_amount']}")
