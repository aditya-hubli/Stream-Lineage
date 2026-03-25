"""
Demo script showing surgical replay functionality

This demonstrates:
1. Lineage tracking with parent-child relationships
2. Identifying corrupted event chains
3. Surgical replay plan generation
4. Auto-generated consumer commands
"""

import requests
import json
import time
from datetime import datetime, timedelta

API_BASE = "http://localhost:8000"

def post_lineage(prov_id, producer_id, topic, dataset_id, kafka_partition, kafka_offset, parent_prov_ids=None):
    """Post a lineage event to the API"""
    import hashlib
    
    lineage = {
        "prov_id": prov_id,
        "parent_prov_ids": parent_prov_ids or [],
        "origin_id": parent_prov_ids[0] if parent_prov_ids else prov_id,  # Track corruption chain
        "producer_id": producer_id,
        "producer_hash": hashlib.sha256(producer_id.encode()).hexdigest()[:16],
        "dataset_id": dataset_id,
        "topic": topic,
        "partition": kafka_partition,
        "offset": kafka_offset,
        "operation": "TRANSFORM",
        "schema": {},
        "schema_id": "schema-v1",
        "schema_hash": hashlib.sha256(b"schema-v1").hexdigest()[:16],
        "transform_ops": [],
        "chain_hash": hashlib.sha256(prov_id.encode()).hexdigest()[:16],
        "ingested_at": datetime.utcnow().isoformat() + "Z"
    }
    
    resp = requests.post(f"{API_BASE}/api/lineage/ingest", json=lineage)
    if resp.status_code == 200:
        print(f"✓ Posted: {prov_id} (partition={kafka_partition}, offset={kafka_offset})")
    else:
        print(f"✗ Failed: {prov_id} - {resp.text}")
    return resp

def main():
    print("="*70)
    print("SURGICAL REPLAY DEMO - StreamLineage")
    print("="*70)
    
    # Timestamps
    base_time = datetime.utcnow()
    
    print("\n[1] Creating data pipeline: Source → Transform → Sink")
    print("-" * 70)
    
    # Simulate 100 source events (only 3 will be corrupted)
    source_offsets = []
    for i in range(100):
        prov_id = f"source-{i}"
        post_lineage(
            prov_id=prov_id,
            producer_id="data-source-v1",
            topic="raw-data",
            dataset_id="raw.data",
            kafka_partition=i % 3,  # Distribute across 3 partitions
            kafka_offset=i
        )
        source_offsets.append((i % 3, i, prov_id))
        time.sleep(0.01)
    
    print(f"\n✓ Created 100 source events across 3 partitions")
    
    # Transform layer - processes all source events
    print("\n[2] Transform layer processing...")
    transform_offsets = []
    for partition, offset, source_prov_id in source_offsets:
        transform_prov_id = f"transform-{source_prov_id}"
        post_lineage(
            prov_id=transform_prov_id,
            producer_id="data-transformer-v1",
            topic="transformed-data",
            dataset_id="transformed.data",
            kafka_partition=partition,
            kafka_offset=offset + 1000,  # Different offset range
            parent_prov_ids=[source_prov_id]
        )
        transform_offsets.append((partition, offset + 1000, transform_prov_id, source_prov_id))
        time.sleep(0.01)
    
    print(f"✓ Created 100 transform events")
    
    # Sink layer - consumes transformed events
    print("\n[3] Sink layer consuming...")
    for partition, offset, transform_prov_id, source_prov_id in transform_offsets:
        sink_prov_id = f"sink-{source_prov_id}"
        post_lineage(
            prov_id=sink_prov_id,
            producer_id="data-sink-v1",
            topic="final-output",
            dataset_id="final.output",
            kafka_partition=partition,
            kafka_offset=offset + 2000,  # Different offset range
            parent_prov_ids=[transform_prov_id]
        )
        time.sleep(0.01)
    
    print(f"✓ Created 100 sink events")
    
    # Check DAG
    print("\n[4] Checking DAG status...")
    print("-" * 70)
    dag = requests.get(f"{API_BASE}/api/v1/dag").json()
    print(f"Nodes: {dag['node_count']}, Edges: {dag['edge_count']}")
    for node in dag['nodes']:
        print(f"  - {node['producer_id']}: {node['event_count']} events")
    
    # Simulate corruption scenario
    print("\n[5] CORRUPTION DETECTED!")
    print("-" * 70)
    print("Scenario: 3 source events were corrupted (source-10, source-25, source-77)")
    print("This affects:")
    print("  - 3 transform events")
    print("  - 3 sink events")
    print("  = 9 total events in corruption chain")
    print("\nTraditional approach: Replay ALL 300 events (100%)")
    print("Surgical replay: Replay only 9 events (3%)")
    
    # Build surgical replay plan
    print("\n[6] Building surgical replay plan...")
    print("-" * 70)
    
    corruption_time = base_time.isoformat() + "Z"
    end_time = (base_time + timedelta(hours=1)).isoformat() + "Z"
    
    replay_request = {
        "producer_id": "data-source-v1",
        "corrupted_from": corruption_time,
        "corrupted_until": end_time,
        "include_join_partners": True
    }
    
    replay_resp = requests.post(
        f"{API_BASE}/api/v1/replay/plan",
        json=replay_request
    )
    
    if replay_resp.status_code == 200:
        plan = replay_resp.json()
        
        print(f"\n✓ Surgical Replay Plan Generated")
        print(f"  Total affected events: {plan['total_affected_events']}")
        print(f"  Topics to replay: {len(plan['partition_offsets'])}")
        
        print("\n[7] Partition-specific offsets to replay:")
        print("-" * 70)
        for topic, partitions in plan['partition_offsets'].items():
            print(f"\nTopic: {topic}")
            for partition, offsets in partitions.items():
                offset_list = [o['offset'] for o in offsets]
                print(f"  Partition {partition}: {len(offset_list)} events -> {offset_list[:10]}{'...' if len(offset_list) > 10 else ''}")
        
        print("\n[8] Auto-generated consumer commands:")
        print("-" * 70)
        print("\n--- Python Code ---")
        print(plan['consumer_commands']['python'])
        
        print("\n\n--- CLI Command ---")
        print(plan['consumer_commands']['cli'])
        
        print("\n\n--- Java Code ---")
        print(plan['consumer_commands']['java'][:500] + "...")
        
        print("\n" + "="*70)
        print("DEMO COMPLETE")
        print("="*70)
        print(f"\nSavings: Replay {plan['total_affected_events']} events instead of 300")
        print(f"Reduction: {100 - (plan['total_affected_events']/300*100):.1f}%")
        
    else:
        print(f"✗ Replay plan failed: {replay_resp.text}")

if __name__ == "__main__":
    main()
