"""Fan-Out Pattern: Enrichment Processor"""
import json
from confluent_kafka import Consumer, Producer
from streamlineage import LineageEnvelope

consumer_conf = {
    'bootstrap.servers': 'localhost:19092',
    'group.id': 'enrich-processor-v1',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(consumer_conf)
consumer.subscribe(['fanout-source'])

producer_conf = {
    'bootstrap.servers': 'localhost:19092',
    'client.id': 'enrich-processor-v1'
}
producer = Producer(producer_conf)

print("[FAN-OUT] Enrichment processor starting...")

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        continue
    
    # Extract data and parent lineage
    message_str = msg.value().decode('utf-8')
    data, parent_envelope = LineageEnvelope.from_json(message_str)
    
    # Enrich with metadata
    enriched = {
        **data,
        "enriched_priority": "high" if data['priority'] >= 3 else "low",
        "category": f"cat-{data['type']}"
    }
    
    envelope = LineageEnvelope(
        data=enriched,
        dataset_id="fanout.enriched",
        producer_id="enrich-processor-v1",
        operation="ENRICH",
        parent_envelopes=[parent_envelope] if parent_envelope else None
    )
    
    producer.produce(
        topic='fanout-enriched',
        value=envelope.to_json().encode('utf-8')
    )
    producer.flush()
    print(f"[ENRICH] Enriched: {data['id']}")
