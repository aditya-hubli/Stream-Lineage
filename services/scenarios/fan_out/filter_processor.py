"""Fan-Out Pattern: Filter Processor (Type A only)"""
import json
from confluent_kafka import Consumer, Producer
from streamlineage import LineageEnvelope

consumer_conf = {
    'bootstrap.servers': 'localhost:19092',
    'group.id': 'filter-processor-v1',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(consumer_conf)
consumer.subscribe(['fanout-source'])

producer_conf = {
    'bootstrap.servers': 'localhost:19092',
    'client.id': 'filter-processor-v1'
}
producer = Producer(producer_conf)

print("[FAN-OUT] Filter processor starting...")

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        continue
    
    # Extract data and parent lineage
    message_str = msg.value().decode('utf-8')
    data, parent_envelope = LineageEnvelope.from_json(message_str)
    
    # Filter: only type A
    if data['type'] == 'A':
        filtered = {**data, "filtered": "type_a"}
        
        envelope = LineageEnvelope(
            data=filtered,
            dataset_id="fanout.filtered",
            producer_id="filter-processor-v1",
            operation="FILTER",
            parent_envelopes=[parent_envelope] if parent_envelope else None
        )
        
        producer.produce(
            topic='fanout-filtered',
            value=envelope.to_json().encode('utf-8')
        )
        producer.flush()
        print(f"[FILTER] Passed: {data['id']}")
    else:
        print(f"[FILTER] Dropped: {data['id']} (type={data['type']})")
