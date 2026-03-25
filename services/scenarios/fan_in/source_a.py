"""Fan-In Pattern: Source A (Clicks)"""
import time
import json
from confluent_kafka import Producer
from streamlineage import LineageEnvelope

producer_conf = {
    'bootstrap.servers': 'localhost:19092',
    'client.id': 'source-a-v1'
}
producer = Producer(producer_conf)

print("[FAN-IN] Source A (clicks) starting...")

count = 0
while True:
    event = {
        "source": "clicks",
        "event_id": f"click-{count}",
        "url": f"/page-{count % 10}",
        "timestamp": time.time()
    }
    
    envelope = LineageEnvelope(
        data=event,
        dataset_id="fanin.clicks",
        producer_id="source-a-v1",
        operation="CREATE"
    )
    
    producer.produce(
        topic='fanin-clicks',
        value=envelope.to_json().encode('utf-8')
    )
    producer.flush()
    
    print(f"[SOURCE-A] Click: {event['url']}")
    count += 1
    time.sleep(3)
