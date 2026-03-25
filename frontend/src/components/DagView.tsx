import { useMemo, useCallback, useState, useEffect } from 'react';
import {
  ReactFlow,
  Background,
  Controls,
  MiniMap,
  useNodesState,
  useEdgesState,
  type Node,
  type Edge,
  type NodeMouseHandler,
  BackgroundVariant,
} from '@xyflow/react';
import '@xyflow/react/dist/style.css';

import { ProducerNode, type ProducerNodeData } from './ProducerNode';
import { AnimatedEdge } from './AnimatedEdge';
import { NodeDetailPanel } from './NodeDetailPanel';
import type { DagResponse, DagNode } from '../types/api';
import { fetchProducers } from '../lib/api';
import { Particles } from './ui/particles';

const nodeTypes = { producer: ProducerNode };
const edgeTypes = { animated: AnimatedEdge };

interface DagViewProps {
  dag: DagResponse | null;
  impactedNodes: string[];
  failedNodes?: string[];
}

export function DagView({ dag, impactedNodes, failedNodes = [] }: DagViewProps) {
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null);
  const [producerStatus, setProducerStatus] = useState<Record<string, 'active' | 'stale' | 'dead'>>({});

  useEffect(() => {
    const load = () => {
      fetchProducers().then((producers) => {
        const map: Record<string, 'active' | 'stale' | 'dead'> = {};
        producers.forEach((p: import('../types/api').ProducerInfo) => { map[p.producer_id] = p.status as 'active' | 'stale' | 'dead'; });
        setProducerStatus(map);
      }).catch(() => {});
    };
    load();
    const interval = setInterval(load, 30_000);
    return () => clearInterval(interval);
  }, []);

  const selectedDagNode: DagNode | null = useMemo(() => {
    if (!selectedNodeId || !dag) return null;
    return dag.nodes.find((n) => n.id === selectedNodeId) ?? null;
  }, [selectedNodeId, dag]);

  // Convert DAG data to React Flow nodes/edges
  const { flowNodes, flowEdges } = useMemo(() => {
    if (!dag) return { flowNodes: [], flowEdges: [] };

    // Auto-layout: position nodes in layers using a simple algorithm
    const nodeMap = new Map(dag.nodes.map((n) => [n.id, n]));
    const inDegree = new Map<string, number>();
    const outEdges = new Map<string, string[]>();

    dag.nodes.forEach((n) => {
      inDegree.set(n.id, 0);
      outEdges.set(n.id, []);
    });

    dag.edges.forEach((e) => {
      inDegree.set(e.target, (inDegree.get(e.target) ?? 0) + 1);
      outEdges.get(e.source)?.push(e.target);
    });

    // BFS layering
    const layers: string[][] = [];
    const assigned = new Set<string>();
    let queue = dag.nodes.filter((n) => (inDegree.get(n.id) ?? 0) === 0).map((n) => n.id);

    // Handle case where all nodes have incoming edges (cycle or no roots)
    if (queue.length === 0) {
      queue = dag.nodes.map((n) => n.id);
    }

    while (queue.length > 0) {
      const layer = queue.filter((id) => !assigned.has(id));
      if (layer.length === 0) break;
      layers.push(layer);
      layer.forEach((id) => assigned.add(id));

      const nextQueue: string[] = [];
      layer.forEach((id) => {
        outEdges.get(id)?.forEach((target) => {
          if (!assigned.has(target)) nextQueue.push(target);
        });
      });
      queue = nextQueue;
    }

    // Add any unassigned nodes
    const unassigned = dag.nodes.filter((n) => !assigned.has(n.id)).map((n) => n.id);
    if (unassigned.length > 0) layers.push(unassigned);

    // Position nodes
    const X_GAP = 280;
    const Y_GAP = 100;

    const flowNodes: Node[] = [];
    layers.forEach((layer, layerIdx) => {
      const layerHeight = layer.length * Y_GAP;
      const startY = -layerHeight / 2;

      layer.forEach((nodeId, nodeIdx) => {
        const dagNode = nodeMap.get(nodeId);
        if (!dagNode) return;

        flowNodes.push({
          id: nodeId,
          type: 'producer',
          position: { x: layerIdx * X_GAP, y: startY + nodeIdx * Y_GAP },
          data: {
            label: dagNode.producer_id,
            eventCount: dagNode.event_count,
            topics: dagNode.topics,
            lastSeenAt: dagNode.last_seen_at,
            isSelected: nodeId === selectedNodeId,
            isImpacted: impactedNodes.includes(nodeId),
            status: producerStatus[dagNode.producer_id],
          } satisfies ProducerNodeData,
        });
      });
    });

    const flowEdges: Edge[] = dag.edges.map((e, i) => ({
      id: `e-${e.source}-${e.target}-${i}`,
      source: e.source,
      target: e.target,
      type: 'animated',
      data: {
        eventCount: e.event_count,
        isImpacted:
          impactedNodes.includes(e.source) && impactedNodes.includes(e.target),
      },
    }));

    return { flowNodes, flowEdges };
  }, [dag, selectedNodeId, impactedNodes, producerStatus]);

  const [nodes, setNodes, onNodesChange] = useNodesState(flowNodes);
  const [edges, setEdges, onEdgesChange] = useEdgesState(flowEdges);

  // Sync when dag changes
  useMemo(() => {
    setNodes(flowNodes);
    setEdges(flowEdges);
  }, [flowNodes, flowEdges, setNodes, setEdges]);

  const onNodeClick: NodeMouseHandler = useCallback((_event, node) => {
    setSelectedNodeId((prev) => (prev === node.id ? null : node.id));
  }, []);

  return (
    <div className="relative w-full h-full">
      <ReactFlow
        nodes={nodes}
        edges={edges}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        onNodeClick={onNodeClick}
        onPaneClick={() => setSelectedNodeId(null)}
        nodeTypes={nodeTypes}
        edgeTypes={edgeTypes}
        fitView
        fitViewOptions={{ padding: 0.3 }}
        minZoom={0.2}
        maxZoom={2}
        proOptions={{ hideAttribution: true }}
      >
        <Background
          variant={BackgroundVariant.Dots}
          gap={20}
          size={1}
          color="#27272a"
        />
        <Controls />
        <MiniMap
          nodeColor={(n) => {
            if (failedNodes.includes(n.id)) return '#dc2626';   // bright red — hash failure
            if (impactedNodes.includes(n.id)) return '#ef4444'; // red — impact
            if (n.id === selectedNodeId) return '#8b5cf6';
            return '#3f3f46';
          }}
          maskColor="rgba(0,0,0,0.7)"
          pannable
          zoomable
        />
      </ReactFlow>

      {/* Node detail slide-out */}
      <NodeDetailPanel
        node={selectedDagNode}
        onClose={() => setSelectedNodeId(null)}
        onRunImpact={() => {}}
        onTrace={() => {}}
      />

      {/* MagicUI Particles background on the DAG view */}
      <Particles
        className="absolute inset-0 -z-10 pointer-events-none"
        quantity={50}
        ease={80}
        color="#8b5cf6"
        size={0.3}
        staticity={60}
      />

      {/* Empty state */}
      {dag && dag.nodes.length === 0 && (
        <div className="absolute inset-0 flex items-center justify-center pointer-events-none">
          <div className="text-center">
            <p className="text-zinc-500 text-sm">No lineage data yet.</p>
            <p className="text-zinc-600 text-[13px] mt-1">
              Start a scenario or produce events to see the DAG.
            </p>
          </div>
        </div>
      )}
    </div>
  );
}
