"""Complex DAG: Fraud Check"""
import json
from confluent_kafka import Consumer, Producer
from streamlineage import LineageEnvelope

consumer_conf = {
    'bootstrap.servers': 'localhost:19092',
    'group.id': 'fraud-check-v1',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(consumer_conf)
consumer.subscribe(['dag-enriched'])

producer_conf = {
    'bootstrap.servers': 'localhost:19092',
    'client.id': 'fraud-check-v1'
}
producer = Producer(producer_conf)

print("[COMPLEX-DAG] Fraud Check starting...")

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        continue
    
    # Extract data and parent lineage
    message_str = msg.value().decode('utf-8')
    data, parent_envelope = LineageEnvelope.from_json(message_str)
    
    # Simple fraud check: high amounts are suspicious
    fraud_score = min(data['amount'] / 100, 10)
    
    checked = {
        **data,
        "fraud_score": fraud_score,
        "suspicious": fraud_score > 7
    }
    
    envelope = LineageEnvelope(
        data=checked,
        dataset_id="dag.fraud_checked",
        producer_id="fraud-check-v1",
        operation="VALIDATE",
        parent_envelopes=[parent_envelope] if parent_envelope else None
    )
    
    producer.produce(
        topic='dag-fraud-checked',
        value=envelope.to_json().encode('utf-8')
    )
    producer.flush()
    
    print(f"[FRAUD-CHECK] Order {data['order_id']}: score={fraud_score:.1f}")
