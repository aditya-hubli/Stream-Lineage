"""Complex DAG: Final Output"""
import json
import requests
from confluent_kafka import Consumer
from streamlineage import LineageEnvelope

consumer_conf = {
    'bootstrap.servers': 'localhost:19092',
    'group.id': 'dag-output-v1',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(consumer_conf)
consumer.subscribe(['dag-aggregated'])

print("[COMPLEX-DAG] Output starting...")

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        continue
    
    # Extract data and parent lineage
    message_str = msg.value().decode('utf-8')
    data, parent_envelope = LineageEnvelope.from_json(message_str)
    
    # Create final sink envelope with parent link
    sink_envelope = LineageEnvelope(
        data={'output_at': data['country'], 'risk': data['risk_level']},
        dataset_id="dag.final_output",
        producer_id="dag-output-v1",
        operation="SINK",
        parent_envelopes=[parent_envelope] if parent_envelope else None
    )
    
    # Send full envelope to API
    try:
        requests.post(
            'http://localhost:8000/api/lineage/ingest',
            json=json.loads(sink_envelope.to_json())['lineage']
        )
        print(f"[OUTPUT] {data['country']} {data['risk_level']}: {data['order_count']} orders, avg ${data['avg_amount']:.2f}")
    except Exception as e:
        print(f"[OUTPUT] API error: {e}")
