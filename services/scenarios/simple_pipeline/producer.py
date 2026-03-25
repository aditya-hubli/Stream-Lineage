"""Simple Pipeline: Step 1 - Raw Event Producer"""
import time
import json
from confluent_kafka import Producer
from streamlineage import LineageEnvelope

# Kafka config
producer_conf = {
    'bootstrap.servers': 'localhost:19092',
    'client.id': 'simple-producer-v1'
}
producer = Producer(producer_conf)

print("[SIMPLE-PIPELINE] Producer starting...")

event_count = 0
while True:
    # Simple events
    event = {
        "event_id": f"evt-{event_count}",
        "value": event_count * 10,
        "timestamp": time.time()
    }
    
    # Wrap with lineage
    envelope = LineageEnvelope(
        data=event,
        dataset_id="raw.events",
        producer_id="simple-producer-v1",
        operation="CREATE"
    )
    
    producer.produce(
        topic='raw-events',
        value=envelope.to_json().encode('utf-8')
    )
    producer.flush()
    
    print(f"[PRODUCER] Sent event {event_count}")
    event_count += 1
    time.sleep(3)
