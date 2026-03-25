################################################################################
#  Apache Flink Kafka Example + StreamLineage Integration
#  
#  This is a modified version of the Apache Flink Kafka CSV example that
#  demonstrates StreamLineage integration with ONLY 3 lines changed.
#
#  Changes from original:
#  1. Import LineageStream
#  2. Use LineageStream.from_kafka() instead of direct Kafka consumer
#  3. Use .to_kafka() instead of direct Kafka sink
################################################################################
import logging
import sys
import json
from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment

# ADDED: Import StreamLineage wrapper
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'sdk'))
from streamlineage.pyflink_wrapper import LineageStream


def simple_kafka_pipeline(env):
    """
    Simple Kafka pipeline with lineage tracking.
    
    Flow:
      1. Read from 'flink-input' topic
      2. Transform the data (parse JSON, add field)
      3. Write to 'flink-output' topic
      
    All steps automatically tracked by StreamLineage.
    """
    
    # CHANGED: Use LineageStream.from_kafka() instead of native Kafka consumer
    stream = LineageStream.from_kafka(
        env=env,
        topic="flink-input",
        node_id="flink-csv-processor-v1",
        bootstrap_servers="localhost:19092"  # Using RedPanda on host
    )
    
    # Transform: Parse JSON and add enrichment
    def transform_event(event_str):
        try:
            event = json.loads(event_str)
            # Add processing timestamp and transform
            event['processed_by'] = 'flink-csv-processor-v1'
            event['doubled_value'] = event.get('value', 0) * 2
            return json.dumps(event)
        except Exception as e:
            logging.error(f"Failed to process event: {e}")
            return event_str
    
    transformed = stream.map(transform_event, op_name="enrich_csv_data")
    
    # CHANGED: Use .to_kafka() instead of native Kafka producer
    transformed.to_kafka(
        topic="flink-output",
        bootstrap_servers="localhost:19092"
    )
    
    # Execute the pipeline
    env.execute("Flink CSV Pipeline with Lineage Tracking")


def create_test_data_producer():
    """
    Helper function to generate test data for the pipeline.
    Run this first to populate the input topic.
    """
    from confluent_kafka import Producer
    import time
    
    producer_conf = {
        'bootstrap.servers': 'localhost:19092',
        'client.id': 'flink-test-data-generator'
    }
    producer = Producer(producer_conf)
    
    print("[TEST DATA] Generating events for flink-input topic...")
    
    for i in range(20):
        event = {
            "id": i,
            "value": i * 10,
            "category": "test" if i % 2 == 0 else "demo"
        }
        producer.produce(
            topic='flink-input',
            value=json.dumps(event).encode('utf-8')
        )
        producer.flush()
        print(f"[TEST DATA] Sent event {i}: {event}")
        time.sleep(0.5)
    
    print("[TEST DATA] Test data generation complete")


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    # Check if we should generate test data first
    if len(sys.argv) > 1 and sys.argv[1] == '--generate-data':
        create_test_data_producer()
    else:
        env = StreamExecutionEnvironment.get_execution_environment()
        env.set_parallelism(1)  # Single parallelism for simplicity
        
        print("=" * 70)
        print("FLINK PIPELINE WITH STREAMLINEAGE")
        print("=" * 70)
        print("This pipeline:")
        print("  1. Reads from 'flink-input' Kafka topic")
        print("  2. Transforms data (doubling values)")
        print("  3. Writes to 'flink-output' topic")
        print("  4. Tracks full lineage in StreamLineage API")
        print("=" * 70)
        print("\nStarting pipeline...")
        
        simple_kafka_pipeline(env)
