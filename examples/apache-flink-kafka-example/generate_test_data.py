"""
Test data generator for Flink pipeline

Generates sample events and publishes them to the flink-input topic.
"""
import json
import time
from confluent_kafka import Producer

def create_test_data():
    """Generate test data for the Flink pipeline."""
    producer_conf = {
        'bootstrap.servers': 'localhost:19092',
        'client.id': 'flink-test-data-generator'
    }
    producer = Producer(producer_conf)
    
    print("="*70)
    print("GENERATING TEST DATA FOR FLINK PIPELINE")
    print("="*70)
    print(f"Publishing to topic: flink-input")
    print(f"Kafka broker: localhost:19092")
    print("="*70)
    
    for i in range(20):
        event = {
            "id": i,
            "value": i * 10,
            "category": "test" if i % 2 == 0 else "demo",
            "timestamp": time.time()
        }
        
        producer.produce(
            topic='flink-input',
            value=json.dumps(event).encode('utf-8')
        )
        producer.flush()
        
        print(f"✓ Sent event {i}: id={event['id']}, value={event['value']}, category={event['category']}")
        time.sleep(0.3)
    
    print("\n" + "="*70)
    print(f"TEST DATA GENERATION COMPLETE: 20 events published")
    print("="*70)
    print("\nNext step: Run the Flink pipeline to process these events")
    print("  python lineage_enabled_pipeline.py")

if __name__ == '__main__':
    create_test_data()
