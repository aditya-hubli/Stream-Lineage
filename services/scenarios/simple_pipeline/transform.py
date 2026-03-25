"""Simple Pipeline: Step 2 - Transform Events"""
import json
from confluent_kafka import Consumer, Producer
from streamlineage import LineageEnvelope

# Kafka config
consumer_conf = {
    'bootstrap.servers': 'localhost:19092',
    'group.id': 'simple-transformer-v1',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(consumer_conf)
consumer.subscribe(['raw-events'])

producer_conf = {
    'bootstrap.servers': 'localhost:19092',
    'client.id': 'simple-transformer-v1'
}
producer = Producer(producer_conf)

print("[SIMPLE-PIPELINE] Transformer starting...")

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        continue
    
    # Extract data and parent lineage from message
    message_str = msg.value().decode('utf-8')
    data, parent_envelope = LineageEnvelope.from_json(message_str)
    event = data
    
    # Transform: double the value
    transformed = {
        **event,
        "value": event["value"] * 2,
        "transformed": True
    }
    
    # Create new envelope WITH parent link
    envelope = LineageEnvelope(
        data=transformed,
        dataset_id="transformed.events",
        producer_id="simple-transformer-v1",
        operation="TRANSFORM",
        parent_envelopes=[parent_envelope] if parent_envelope else None
    )
    
    producer.produce(
        topic='transformed-events',
        value=envelope.to_json().encode('utf-8')
    )
    producer.flush()
    
    print(f"[TRANSFORM] Transformed event {event['event_id']}: {event['value']} -> {transformed['value']}")
