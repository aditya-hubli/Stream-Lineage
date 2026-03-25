"""Complex DAG: Risk Scorer"""
import json
from confluent_kafka import Consumer, Producer
from streamlineage import LineageEnvelope

consumer_conf = {
    'bootstrap.servers': 'localhost:19092',
    'group.id': 'risk-scorer-v1',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(consumer_conf)
consumer.subscribe(['dag-fraud-checked'])

producer_conf = {
    'bootstrap.servers': 'localhost:19092',
    'client.id': 'risk-scorer-v1'
}
producer = Producer(producer_conf)

print("[COMPLEX-DAG] Risk Scorer starting...")

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        continue
    
    # Extract data and parent lineage
    message_str = msg.value().decode('utf-8')
    data, parent_envelope = LineageEnvelope.from_json(message_str)
    
    # Calculate risk based on fraud + tier
    tier_risk = {"bronze": 3, "silver": 2, "gold": 1, "unknown": 5}
    total_risk = data['fraud_score'] + tier_risk.get(data['tier'], 5)
    
    scored = {
        **data,
        "total_risk": total_risk,
        "risk_level": "HIGH" if total_risk > 10 else "MEDIUM" if total_risk > 5 else "LOW"
    }
    
    envelope = LineageEnvelope(
        data=scored,
        dataset_id="dag.risk_scored",
        producer_id="risk-scorer-v1",
        operation="SCORE",
        parent_envelopes=[parent_envelope] if parent_envelope else None
    )
    
    producer.produce(
        topic='dag-risk-scored',
        value=envelope.to_json().encode('utf-8')
    )
    producer.flush()
    
    print(f"[RISK-SCORER] Order {event['order_id']}: {scored['risk_level']} (risk={total_risk:.1f})")
