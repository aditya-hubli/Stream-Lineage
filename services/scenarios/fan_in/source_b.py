"""Fan-In Pattern: Source B (Impressions)"""
import time
import json
from confluent_kafka import Producer
from streamlineage import LineageEnvelope

producer_conf = {
    'bootstrap.servers': 'localhost:19092',
    'client.id': 'source-b-v1'
}
producer = Producer(producer_conf)

print("[FAN-IN] Source B (impressions) starting...")

count = 0
while True:
    event = {
        "source": "impressions",
        "event_id": f"imp-{count}",
        "ad_id": f"ad-{count % 5}",
        "timestamp": time.time()
    }
    
    envelope = LineageEnvelope(
        data=event,
        dataset_id="fanin.impressions",
        producer_id="source-b-v1",
        operation="CREATE"
    )
    
    producer.produce(
        topic='fanin-impressions',
        value=envelope.to_json().encode('utf-8')
    )
    producer.flush()
    
    print(f"[SOURCE-B] Impression: {event['ad_id']}")
    count += 1
    time.sleep(4)
