"""Fan-In Pattern: Source C (Conversions)"""
import time
import json
from confluent_kafka import Producer
from streamlineage import LineageEnvelope

producer_conf = {
    'bootstrap.servers': 'localhost:19092',
    'client.id': 'source-c-v1'
}
producer = Producer(producer_conf)

print("[FAN-IN] Source C (conversions) starting...")

count = 0
while True:
    event = {
        "source": "conversions",
        "event_id": f"conv-{count}",
        "amount": (count + 1) * 50,
        "timestamp": time.time()
    }
    
    envelope = LineageEnvelope(
        data=event,
        dataset_id="fanin.conversions",
        producer_id="source-c-v1",
        operation="CREATE"
    )
    
    producer.produce(
        topic='fanin-conversions',
        value=envelope.to_json().encode('utf-8')
    )
    producer.flush()
    
    print(f"[SOURCE-C] Conversion: ${event['amount']}")
    count += 1
    time.sleep(5)
