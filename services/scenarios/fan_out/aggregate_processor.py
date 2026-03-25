"""Fan-Out Pattern: Aggregation Processor"""
import json
from confluent_kafka import Consumer, Producer
from streamlineage import LineageEnvelope

consumer_conf = {
    'bootstrap.servers': 'localhost:19092',
    'group.id': 'aggregate-processor-v1',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(consumer_conf)
consumer.subscribe(['fanout-source'])

producer_conf = {
    'bootstrap.servers': 'localhost:19092',
    'client.id': 'aggregate-processor-v1'
}
producer = Producer(producer_conf)

print("[FAN-OUT] Aggregation processor starting...")

count_by_type = {}

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        continue
    
    # Extract data and parent lineage
    message_str = msg.value().decode('utf-8')
    data, parent_envelope = LineageEnvelope.from_json(message_str)
    
    # Aggregate counts
    event_type = data['type']
    count_by_type[event_type] = count_by_type.get(event_type, 0) + 1
    
    agg = {
        "type": event_type,
        "count": count_by_type[event_type],
        "latest_id": data['id']
    }
    
    envelope = LineageEnvelope(
        data=agg,
        dataset_id="fanout.aggregated",
        producer_id="aggregate-processor-v1",
        operation="AGGREGATE",
        parent_envelopes=[parent_envelope] if parent_envelope else None
    )
    
    producer.produce(
        topic='fanout-aggregated',
        value=envelope.to_json().encode('utf-8')
    )
    producer.flush()
    print(f"[AGGREGATE] Type {event_type}: {count_by_type[event_type]} events")
