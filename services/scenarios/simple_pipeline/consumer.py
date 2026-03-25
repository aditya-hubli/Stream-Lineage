"""Simple Pipeline: Step 3 - Final Consumer"""
import json
import requests
from confluent_kafka import Consumer
from streamlineage import LineageEnvelope

# Kafka config
consumer_conf = {
    'bootstrap.servers': 'localhost:19092',
    'group.id': 'simple-consumer-v1',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(consumer_conf)
consumer.subscribe(['transformed-events'])

print("[SIMPLE-PIPELINE] Consumer starting...")

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        continue
    
    # Extract data and parent lineage
    message_str = msg.value().decode('utf-8')
    data, parent_envelope = LineageEnvelope.from_json(message_str)
    event = data
    
    # Create final sink envelope with parent link
    sink_envelope = LineageEnvelope(
        data={"consumed": True, "event_id": event.get("event_id")},
        dataset_id="final.output",
        producer_id="simple-consumer-v1",
        operation="SINK",
        parent_envelopes=[parent_envelope] if parent_envelope else None
    )
    
    # Post lineage to API (universal approach - works for ANY platform)
    try:
        response = requests.post(
            'http://localhost:8000/api/lineage/ingest',
            json=json.loads(sink_envelope.to_json())['lineage']
        )
        print(f"[CONSUMER] Consumed event {event.get('event_id')}, value={event.get('value')}")
    except Exception as e:
        print(f"[CONSUMER] API error: {e}")
