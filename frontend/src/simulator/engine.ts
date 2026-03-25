// StreamLineage Simulator Engine
// Pure in-browser simulation — no backend required

export type NodeStatus = 'idle' | 'active' | 'corrupted' | 'tainted';

export interface TopologyNodeDef {
  id: string;
  label: string;
  topic: string;
  x: number;
  y: number;
}

export interface TopologyDef {
  id: string;
  name: string;
  description: string;
  nodes: TopologyNodeDef[];
  edges: { source: string; target: string }[];
}

export const TOPOLOGIES: TopologyDef[] = [
  {
    id: 'simple',
    name: 'Simple Pipeline',
    description: '3-stage linear transform',
    nodes: [
      { id: 'raw-ingest', label: 'raw-ingest', topic: 'raw.events', x: 60, y: 160 },
      { id: 'transformer', label: 'transformer', topic: 'processed.events', x: 360, y: 160 },
      { id: 'output-sink', label: 'output-sink', topic: 'output.events', x: 660, y: 160 },
    ],
    edges: [
      { source: 'raw-ingest', target: 'transformer' },
      { source: 'transformer', target: 'output-sink' },
    ],
  },
  {
    id: 'ecommerce',
    name: 'E-Commerce',
    description: 'Order fulfillment with fan-out',
    nodes: [
      { id: 'orders', label: 'orders', topic: 'orders.placed', x: 50, y: 200 },
      { id: 'fraud-check', label: 'fraud-check', topic: 'fraud.signals', x: 290, y: 80 },
      { id: 'inventory', label: 'inventory', topic: 'inventory.reserve', x: 290, y: 320 },
      { id: 'payment', label: 'payment', topic: 'payments.out', x: 530, y: 200 },
      { id: 'fulfillment', label: 'fulfillment', topic: 'orders.fulfilled', x: 730, y: 80 },
      { id: 'analytics', label: 'analytics', topic: 'analytics.events', x: 730, y: 320 },
    ],
    edges: [
      { source: 'orders', target: 'fraud-check' },
      { source: 'orders', target: 'inventory' },
      { source: 'fraud-check', target: 'payment' },
      { source: 'inventory', target: 'payment' },
      { source: 'payment', target: 'fulfillment' },
      { source: 'payment', target: 'analytics' },
    ],
  },
  {
    id: 'financial',
    name: 'Financial Risk',
    description: 'Fan-in risk scoring pipeline',
    nodes: [
      { id: 'trades', label: 'trades', topic: 'trades.raw', x: 50, y: 120 },
      { id: 'market-data', label: 'market-data', topic: 'market.feed', x: 50, y: 300 },
      { id: 'risk-engine', label: 'risk-engine', topic: 'risk.scores', x: 310, y: 200 },
      { id: 'compliance', label: 'compliance', topic: 'compliance.flags', x: 550, y: 100 },
      { id: 'settlement', label: 'settlement', topic: 'trades.settled', x: 750, y: 100 },
      { id: 'reporting', label: 'reporting', topic: 'risk.reports', x: 550, y: 300 },
    ],
    edges: [
      { source: 'trades', target: 'risk-engine' },
      { source: 'market-data', target: 'risk-engine' },
      { source: 'risk-engine', target: 'compliance' },
      { source: 'risk-engine', target: 'reporting' },
      { source: 'compliance', target: 'settlement' },
    ],
  },
];

function simHash(input: string): string {
  let h = 0x811c9dc5;
  for (let i = 0; i < input.length; i++) {
    h ^= input.charCodeAt(i);
    h = (h * 0x01000193) >>> 0;
  }
  return h.toString(16).padStart(8, '0');
}

function makeId(): string {
  return `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
}

export interface SimNodeState extends TopologyNodeDef {
  status: NodeStatus;
  eventCount: number;
  taintedCount: number;
  lastEventAt: number;
}

export interface SimEdgeState {
  id: string;
  source: string;
  target: string;
  eventCount: number;
  active: boolean;
  tainted: boolean;
}

export interface SimEvent {
  id: string;
  provId: string;
  originId: string;
  parentProvIds: string[];
  producerId: string;
  topic: string;
  partition: number;
  offset: number;
  chainHash: string;
  tainted: boolean;
  tampered: boolean;
  chainValid: boolean;
  timestamp: number;
}

export interface ReplayPlan {
  active: boolean;
  taintedEvents: number;
  totalEventsInWindow: number;
  efficiencyPct: number;
  offsets: { topic: string; partition: number; offsets: number[] }[];
}

export interface SimState {
  nodes: SimNodeState[];
  edges: SimEdgeState[];
  events: SimEvent[];
  replayPlan: ReplayPlan;
  running: boolean;
  eventsPerSec: number;
}

export class SimEngine {
  private topo: TopologyDef;
  private nodes = new Map<string, SimNodeState>();
  private edges = new Map<string, SimEdgeState>();
  private events: SimEvent[] = [];
  private offsets = new Map<string, Map<number, number>>();
  private corruptedNodes = new Set<string>();
  private taintOriginIds = new Set<string>();
  private replayPlan: ReplayPlan = { active: false, taintedEvents: 0, totalEventsInWindow: 0, efficiencyPct: 0, offsets: [] };
  private windowStart = 0;
  private running = false;
  private tickTimer: ReturnType<typeof setInterval> | null = null;
  private corruptTimer: ReturnType<typeof setTimeout> | null = null;
  private destroyed = false;
  private onUpdate: (s: SimState) => void;
  private recentTicks: number[] = [];

  constructor(topo: TopologyDef, onUpdate: (s: SimState) => void) {
    this.topo = topo;
    this.onUpdate = onUpdate;
    this.init();
  }

  private init() {
    this.nodes.clear();
    this.edges.clear();
    this.events = [];
    this.offsets.clear();
    this.corruptedNodes.clear();
    this.taintOriginIds.clear();
    this.replayPlan = { active: false, taintedEvents: 0, totalEventsInWindow: 0, efficiencyPct: 0, offsets: [] };

    for (const n of this.topo.nodes) {
      this.nodes.set(n.id, { ...n, status: 'idle', eventCount: 0, taintedCount: 0, lastEventAt: 0 });
    }
    for (const e of this.topo.edges) {
      const id = `${e.source}__${e.target}`;
      this.edges.set(id, { id, source: e.source, target: e.target, eventCount: 0, active: false, tainted: false });
    }
  }

  destroy() {
    this.destroyed = true;
    this.stop();
  }

  setTopology(topo: TopologyDef) {
    const was = this.running;
    this.stop();
    this.topo = topo;
    this.init();
    this.emit();
    if (was) this.start();
  }

  start(speedMs = 700) {
    if (this.running) return;
    this.running = true;
    this.tickTimer = setInterval(() => this.tick(), speedMs);
    this.emit();
  }

  stop() {
    this.running = false;
    if (this.tickTimer) { clearInterval(this.tickTimer); this.tickTimer = null; }
    this.emit();
  }

  isRunning() { return this.running; }

  private nextOffset(topic: string, partition: number): number {
    if (!this.offsets.has(topic)) this.offsets.set(topic, new Map());
    const m = this.offsets.get(topic)!;
    const v = m.get(partition) ?? 0;
    m.set(partition, v + 1);
    return v;
  }

  private sources(): string[] {
    const hasIn = new Set(this.topo.edges.map(e => e.target));
    return this.topo.nodes.filter(n => !hasIn.has(n.id)).map(n => n.id);
  }

  private children(nodeId: string): string[] {
    return this.topo.edges.filter(e => e.source === nodeId).map(e => e.target);
  }

  private processNode(nodeId: string, parentEvent: SimEvent | null, originId: string | null) {
    if (this.destroyed) return;
    const node = this.nodes.get(nodeId);
    if (!node) return;

    const isTainted = (parentEvent?.tainted ?? false) || this.corruptedNodes.has(nodeId);
    const provId = makeId();
    const thisOriginId = originId ?? provId;

    if (isTainted) this.taintOriginIds.add(thisOriginId);

    const partition = Math.floor(Math.random() * 3);
    const offset = this.nextOffset(node.topic, partition);
    const parentHash = parentEvent?.chainHash ?? '';
    const chainHash = simHash(`${parentHash}${node.topic}${partition}${offset}`);

    const event: SimEvent = {
      id: crypto.randomUUID(),
      provId,
      originId: thisOriginId,
      parentProvIds: parentEvent ? [parentEvent.provId] : [],
      producerId: nodeId,
      topic: node.topic,
      partition,
      offset,
      chainHash,
      tainted: isTainted,
      tampered: false,
      chainValid: true,
      timestamp: Date.now(),
    };

    this.events.unshift(event);
    if (this.events.length > 300) this.events.length = 300;

    node.status = this.corruptedNodes.has(nodeId) ? 'corrupted' : isTainted ? 'tainted' : 'active';
    node.eventCount++;
    if (isTainted) node.taintedCount++;
    node.lastEventAt = Date.now();

    if (parentEvent) {
      const edgeId = `${parentEvent.producerId}__${nodeId}`;
      const edge = this.edges.get(edgeId);
      if (edge) {
        edge.eventCount++;
        edge.active = true;
        edge.tainted = isTainted;
        setTimeout(() => { if (edge && !this.destroyed) { edge.active = false; this.emit(); } }, 600);
      }
    }

    const kids = this.children(nodeId);
    kids.forEach((kid, i) => {
      setTimeout(() => {
        if (!this.destroyed) {
          this.processNode(kid, event, thisOriginId);
          this.emit();
        }
      }, 220 + i * 120);
    });
  }

  injectCorruption(nodeId: string, durationMs = 6000) {
    this.corruptedNodes.add(nodeId);
    this.windowStart = Date.now();
    this.replayPlan = { ...this.replayPlan, active: true };
    if (this.corruptTimer) clearTimeout(this.corruptTimer);
    this.corruptTimer = setTimeout(() => {
      if (this.destroyed) return;
      this.corruptedNodes.delete(nodeId);
      const n = this.nodes.get(nodeId);
      if (n) n.status = 'active';
      this.calcReplay();
      this.emit();
    }, durationMs);
    this.emit();
  }

  tamperLatestEvent() {
    const ev = this.events.find(e => !e.tampered && !e.tainted);
    if (!ev) return;
    ev.tampered = true;
    ev.chainValid = false;
    this.emit();
  }

  private calcReplay() {
    const now = Date.now();
    const window = this.events.filter(e => e.timestamp >= this.windowStart && e.timestamp <= now);
    const tainted = window.filter(e => e.tainted);
    const offsetMap = new Map<string, Map<number, number[]>>();
    for (const e of tainted) {
      if (!offsetMap.has(e.topic)) offsetMap.set(e.topic, new Map());
      const pm = offsetMap.get(e.topic)!;
      if (!pm.has(e.partition)) pm.set(e.partition, []);
      pm.get(e.partition)!.push(e.offset);
    }
    const offsets: ReplayPlan['offsets'] = [];
    for (const [topic, pm] of offsetMap) {
      for (const [partition, offs] of pm) {
        offsets.push({ topic, partition, offsets: offs.sort((a, b) => a - b) });
      }
    }
    const total = window.length;
    const tc = tainted.length;
    this.replayPlan = {
      active: true,
      taintedEvents: tc,
      totalEventsInWindow: total,
      efficiencyPct: total > 0 ? +((1 - tc / total) * 100).toFixed(1) : 0,
      offsets,
    };
  }

  private tick() {
    if (this.destroyed) return;
    const now = Date.now();
    this.recentTicks.push(now);
    this.recentTicks = this.recentTicks.filter(t => now - t < 1000);

    const srcs = this.sources();
    const count = Math.random() > 0.4 ? 2 : 1;
    for (let i = 0; i < count; i++) {
      const src = srcs[Math.floor(Math.random() * srcs.length)];
      this.processNode(src, null, null);
    }

    if (this.replayPlan.active) this.calcReplay();

    for (const [, node] of this.nodes) {
      if (node.status === 'active' && now - node.lastEventAt > 2500) node.status = 'idle';
    }

    this.emit();
  }

  private emit() {
    if (this.destroyed) return;
    this.onUpdate(this.getState());
  }

  getState(): SimState {
    const now = Date.now();
    const recentEvents = this.events.filter(e => now - e.timestamp < 1000).length;
    return {
      nodes: Array.from(this.nodes.values()),
      edges: Array.from(this.edges.values()),
      events: this.events.slice(0, 60),
      replayPlan: { ...this.replayPlan },
      running: this.running,
      eventsPerSec: recentEvents,
    };
  }
}
