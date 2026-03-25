"""Complex DAG: Users Dimension Stream"""
import time
import json
from confluent_kafka import Producer
from streamlineage import LineageEnvelope

producer_conf = {
    'bootstrap.servers': 'localhost:19092',
    'client.id': 'users-stream-v1'
}
producer = Producer(producer_conf)

print("[COMPLEX-DAG] Users stream starting...")

user_id = 0
while True:
    event = {
        "user_id": f"usr-{user_id}",
        "country": ["US", "UK", "CA", "AU"][user_id % 4],
        "tier": ["bronze", "silver", "gold"][user_id % 3],
        "timestamp": time.time()
    }
    
    envelope = LineageEnvelope(
        data=event,
        dataset_id="dag.users",
        producer_id="users-stream-v1",
        operation="CREATE"
    )
    
    producer.produce(
        topic='dag-users',
        value=envelope.to_json().encode('utf-8')
    )
    producer.flush()
    
    print(f"[USERS] User {event['user_id']}: {event['country']} ({event['tier']})")
    user_id += 1
    time.sleep(5)
