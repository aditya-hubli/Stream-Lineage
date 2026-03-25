"""
Surgical Replay Router

Provenance-based replay: Instead of replaying entire topics, this generates
precise Kafka consumer commands to replay ONLY the corrupted events using
origin_id chains and exact partition+offset pairs.
"""

import json
import time
from datetime import datetime
from typing import List, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Response
from pydantic import BaseModel

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'lineage-ingester'))

from dependencies import get_graph_builder
from graph_builder import LineageGraphBuilder
from config import config


router = APIRouter()


# Pydantic models
class ReplayPlanRequest(BaseModel):
    """Request for surgical replay plan."""
    producer_id: str
    corrupted_from: str  # ISO8601 timestamp
    corrupted_until: str  # ISO8601 timestamp
    include_join_partners: bool = True


class PartitionSeek(BaseModel):
    """Kafka partition seek instruction."""
    partition: int
    exact_offsets: List[int]
    optimized_ranges: List[Dict[str, int]]  # [{"start": 100, "end": 105}]
    record_count: int


class ReplayCommand(BaseModel):
    """Generated consumer command for replay."""
    language: str  # "python", "cli", "java"
    description: str
    command: str
    script: Optional[str] = None


class ReplayPlanResponse(BaseModel):
    """Surgical replay plan with consumer commands."""
    producer_id: str
    corrupted_from: str
    corrupted_until: str
    total_affected_records: int
    total_events_in_window: int
    replay_efficiency_pct: float  # % of events that actually need replay
    affected_nodes: Dict[str, dict]
    partition_offsets: Dict[str, Dict[int, PartitionSeek]]
    topological_order: List[str]  # Order to replay nodes
    consumer_commands: List[ReplayCommand]
    computed_at: str


@router.post("/plan", response_model=ReplayPlanResponse)
async def generate_replay_plan(
    request: ReplayPlanRequest,
    response: Response,
    graph_builder: LineageGraphBuilder = Depends(get_graph_builder)
):
    """
    Generate surgical replay plan with exact partition+offset pairs.
    
    This is the core of provenance-based replay. Instead of replaying
    entire topics (which wastes resources), this:
    
    1. Finds all events affected by corruption using origin_id chains
    2. Returns exact partition+offset pairs for ONLY corrupted events
    3. Generates Kafka consumer commands to replay exactly those records
    4. Handles join partners (clean events needed for corrupted joins)
    5. Provides topological order to replay upstream before downstream
    
    Example:
    - Corrupted producer created 100 events in 1 hour window
    - Topic had 10,000 events in that window
    - Replay efficiency: 1% (only replay 100, skip 9,900)
    
    Args:
        request: Replay plan request
    
    Returns:
        Surgical replay plan with consumer commands
    """
    start_time = time.time()
    
    try:
        # Build surgical replay plan
        plan = graph_builder.build_surgical_replay_plan(
            producer_id=request.producer_id,
            corrupted_from=request.corrupted_from,
            corrupted_until=request.corrupted_until,
            include_join_partners=request.include_join_partners,
        )
        
        # Convert partition_offsets to Pydantic models
        partition_offsets_models = {}
        for topic, partitions in plan['partition_offsets'].items():
            partition_offsets_models[topic] = {}
            for partition_id, data in partitions.items():
                partition_offsets_models[topic][int(partition_id)] = PartitionSeek(**data)
        
        # Generate consumer commands
        consumer_commands = _generate_consumer_commands(
            plan['partition_offsets'],
            plan['topological_order'],
            bootstrap_servers=config.KAFKA_BROKERS
        )
        
        # Build response
        replay_response = ReplayPlanResponse(
            producer_id=plan['producer_id'],
            corrupted_from=plan['corrupted_from'],
            corrupted_until=plan['corrupted_until'],
            total_affected_records=plan['total_affected_records'],
            total_events_in_window=plan['total_events_in_window'],
            replay_efficiency_pct=plan['replay_efficiency_pct'],
            affected_nodes=plan['affected_nodes'],
            partition_offsets=partition_offsets_models,
            topological_order=plan['topological_order'],
            consumer_commands=consumer_commands,
            computed_at=plan['computed_at'],
        )
        
        # Add computation time header
        computation_time_ms = int((time.time() - start_time) * 1000)
        response.headers["X-Computation-Time-Ms"] = str(computation_time_ms)
        
        return replay_response
    
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Replay plan generation failed: {str(e)}"
        )


def _generate_consumer_commands(
    partition_offsets: Dict[str, Dict[int, dict]],
    topological_order: List[str],
    bootstrap_servers: str
) -> List[ReplayCommand]:
    """
    Generate Kafka consumer commands for surgical replay.
    
    Creates executable code/commands for Python, CLI, and Java.
    """
    commands = []
    
    # Python consumer script
    python_script = _generate_python_consumer(partition_offsets, bootstrap_servers)
    commands.append(ReplayCommand(
        language="python",
        description="Python consumer script for surgical replay",
        command=f"python replay_consumer.py",
        script=python_script
    ))
    
    # CLI commands (kafka-console-consumer)
    cli_commands = _generate_cli_commands(partition_offsets, bootstrap_servers)
    commands.append(ReplayCommand(
        language="cli",
        description="Kafka CLI commands (one per topic-partition)",
        command="\n".join(cli_commands),
        script=None
    ))
    
    # Kafka Streams / Java snippet
    java_snippet = _generate_java_consumer(partition_offsets, bootstrap_servers)
    commands.append(ReplayCommand(
        language="java",
        description="Java consumer code using KafkaConsumer API",
        command="// Compile and run as Java application",
        script=java_snippet
    ))
    
    return commands


def _generate_python_consumer(
    partition_offsets: Dict[str, Dict[int, dict]],
    bootstrap_servers: str
) -> str:
    """Generate Python consumer script."""
    
    script = f'''"""
Surgical Replay Consumer - Auto-generated by StreamLineage
This script replays ONLY the corrupted events, skipping clean data.
"""

from confluent_kafka import Consumer, TopicPartition
import json

# Kafka configuration
config = {{
    'bootstrap.servers': '{bootstrap_servers}',
    'group.id': 'surgical-replay-{{timestamp}}',  # Unique group
    'auto.offset.reset': 'none',
    'enable.auto.commit': False,
}}

consumer = Consumer(config)

# Partition+offset mapping (auto-generated from lineage analysis)
replay_plan = {json.dumps(partition_offsets, indent=4)}

print("🔧 Starting Surgical Replay...")
print(f"Topics to replay: {{len(replay_plan)}}")

total_replayed = 0

for topic, partitions in replay_plan.items():
    for partition_id, plan in partitions.items():
        partition_id = int(partition_id)
        ranges = plan['optimized_ranges']
        
        print(f"\\n📍 Topic: {{topic}}, Partition: {{partition_id}}")
        print(f"   Records to replay: {{plan['record_count']}}")
        print(f"   Optimized to {{len(ranges)}} seek operation(s)")
        
        for range_info in ranges:
            start_offset = range_info['start']
            end_offset = range_info['end']
            
            # Assign partition and seek to start
            tp = TopicPartition(topic, partition_id, start_offset)
            consumer.assign([tp])
            
            print(f"   ↪ Seeking to offset {{start_offset}}, consuming {{end_offset - start_offset}} records...")
            
            consumed = 0
            while consumed < (end_offset - start_offset):
                msg = consumer.poll(timeout=5.0)
                
                if msg is None:
                    print(f"   ⚠️ Timeout at offset {{start_offset + consumed}}")
                    break
                
                if msg.error():
                    print(f"   ❌ Error: {{msg.error()}}")
                    break
                
                # Process the message (your business logic here)
                # data = json.loads(msg.value().decode('utf-8'))
                # reprocess(data)
                
                consumed += 1
                total_replayed += 1
                
                if consumed % 100 == 0:
                    print(f"   Progress: {{consumed}}/{{end_offset - start_offset}} records")

consumer.close()

print(f"\\n✅ Surgical Replay Complete!")
print(f"Total records replayed: {{total_replayed}}")
'''
    
    return script


def _generate_cli_commands(
    partition_offsets: Dict[str, Dict[int, dict]],
    bootstrap_servers: str
) -> List[str]:
    """Generate Kafka CLI commands."""
    
    commands = []
    
    for topic, partitions in partition_offsets.items():
        for partition_id, plan in partitions.items():
            ranges = plan['optimized_ranges']
            
            for range_info in ranges:
                start = range_info['start']
                end = range_info['end']
                
                # kafka-console-consumer command
                cmd = (
                    f"kafka-console-consumer.sh \\\n"
                    f"  --bootstrap-server {bootstrap_servers} \\\n"
                    f"  --topic {topic} \\\n"
                    f"  --partition {partition_id} \\\n"
                    f"  --offset {start} \\\n"
                    f"  --max-messages {end - start}"
                )
                commands.append(cmd)
    
    return commands


def _generate_java_consumer(
    partition_offsets: Dict[str, Dict[int, dict]],
    bootstrap_servers: str
) -> str:
    """Generate Java consumer code."""
    
    code = f'''
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import java.util.*;
import java.time.Duration;

public class SurgicalReplayConsumer {{
    
    public static void main(String[] args) {{
        // Kafka configuration
        Properties props = new Properties();
        props.put("bootstrap.servers", "{bootstrap_servers}");
        props.put("group.id", "surgical-replay-" + System.currentTimeMillis());
        props.put("enable.auto.commit", "false");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        
        // Auto-generated replay plan
        Map<String, Map<Integer, ReplayPlan>> replayMap = buildReplayPlan();
        
        int totalReplayed = 0;
        
        for (Map.Entry<String, Map<Integer, ReplayPlan>> topicEntry : replayMap.entrySet()) {{
            String topic = topicEntry.getKey();
            
            for (Map.Entry<Integer, ReplayPlan> partEntry : topicEntry.getValue().entrySet()) {{
                int partition = partEntry.getKey();
                ReplayPlan plan = partEntry.getValue();
                
                System.out.println("Replaying " + topic + " partition " + partition);
                
                for (Range range : plan.ranges) {{
                    TopicPartition tp = new TopicPartition(topic, partition);
                    consumer.assign(Collections.singletonList(tp));
                    consumer.seek(tp, range.start);
                    
                    long consumed = 0;
                    long targetCount = range.end - range.start;
                    
                    while (consumed < targetCount) {{
                        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
                        
                        for (ConsumerRecord<String, String> record : records) {{
                            // Process record (your business logic)
                            // reprocess(record.value());
                            
                            consumed++;
                            totalReplayed++;
                        }}
                        
                        if (records.isEmpty()) break;
                    }}
                }}
            }}
        }}
        
        consumer.close();
        System.out.println("Surgical replay complete: " + totalReplayed + " records");
    }}
    
    static class Range {{
        long start;
        long end;
        Range(long s, long e) {{ start = s; end = e; }}
    }}
    
    static class ReplayPlan {{
        List<Range> ranges;
        ReplayPlan(List<Range> r) {{ ranges = r; }}
    }}
    
    static Map<String, Map<Integer, ReplayPlan>> buildReplayPlan() {{
        // TODO: Parse from JSON or inject at compile time
        return new HashMap<>();
    }}
}}
'''
    
    return code
