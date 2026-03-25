"""Fan-Out Pattern: Single Producer"""
import time
import json
from confluent_kafka import Producer
from streamlineage import LineageEnvelope

producer_conf = {
    'bootstrap.servers': 'localhost:19092',
    'client.id': 'fanout-producer-v1'
}
producer = Producer(producer_conf)

print("[FAN-OUT] Producer starting...")

event_count = 0
while True:
    event = {
        "id": f"evt-{event_count}",
        "type": ["A", "B", "C"][event_count % 3],
        "value": event_count,
        "priority": event_count % 5
    }
    
    envelope = LineageEnvelope(
        data=event,
        dataset_id="fanout.source",
        producer_id="fanout-producer-v1",
        operation="CREATE"
    )
    
    producer.produce(
        topic='fanout-source',
        value=envelope.to_json().encode('utf-8')
    )
    producer.flush()
    
    print(f"[PRODUCER] Sent {event}")
    event_count += 1
    time.sleep(2)
