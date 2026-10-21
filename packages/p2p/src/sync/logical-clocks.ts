/**
 * Unit III – §2: Logical Clocks — Lamport Scalar Clock & Vector Clock
 *
 * APMC use-cases:
 *
 * Lamport Scalar Clock (total order):
 *   ─ Every cross-mandi BID event carries a Lamport timestamp.
 *   ─ Rule: L = max(L_self, L_msg) + 1
 *   ─ Tie-breaking by node-ID gives a strict total order on all bids even
 *     when physical clocks disagree — guaranteeing the same "highest bid"
 *     result at every mandi node.
 *
 * Vector Clock (causal order + conflict detection):
 *   ─ Each mandi node maintains V[i] for itself and V[j] for every peer.
 *   ─ Gossip-propagated commodity price updates carry vector timestamps.
 *   ─ The receiver can determine:
 *       • V_a < V_b  → a causally precedes b (safe to merge)
 *       • V_a ∥ V_b  → concurrent / conflicting updates (needs resolution)
 *   ─ Conflict resolution: last-writer-wins by highest Lamport scalar
 *     (or by node-ID tiebreak), which is safe because APMC prices only need
 *     eventual convergence across mandis.
 */

import { logger } from '@apmc/middleware';

// ────────────────────────────────────────────────────────────
// Lamport Scalar Clock
// ────────────────────────────────────────────────────────────

export interface LamportStampedMessage {
  type: string;
  from: string;
  lamportTime: number;
  payload: Record<string, unknown>;
}

/**
 * LamportClock — one instance per mandi service node.
 *
 * On SEND:   clock.tick() then attach to message.
 * On RECEIVE: clock.receive(msg.lamportTime) then process.
 */
export class LamportClock {
  private time: number = 0;
  private nodeId: string;

  constructor(nodeId: string, initial: number = 0) {
    this.nodeId = nodeId;
    this.time = initial;
  }

  /** Increment for an internal event or before sending. */
  tick(): number {
    this.time += 1;
    return this.time;
  }

  /**
   * Update on message receipt:
   *   L = max(L_self, L_msg) + 1
   */
  receive(msgTime: number): number {
    this.time = Math.max(this.time, msgTime) + 1;
    return this.time;
  }

  /** Current Lamport time. */
  getTime(): number {
    return this.time;
  }

  /** Stamp an outgoing message (mutates clock). */
  stamp<T extends object>(msg: T): T & { lamportTime: number; from: string } {
    const t = this.tick();
    return { ...msg, lamportTime: t, from: this.nodeId };
  }

  /**
   * Total-order comparator for two stamped messages.
   * Primary key: lamportTime; secondary: nodeId (lexicographic) for tiebreak.
   * Returns negative if a < b, 0 if equal, positive if a > b.
   */
  static compare(
    a: { lamportTime: number; from: string },
    b: { lamportTime: number; from: string }
  ): number {
    if (a.lamportTime !== b.lamportTime) return a.lamportTime - b.lamportTime;
    return a.from < b.from ? -1 : a.from > b.from ? 1 : 0;
  }
}

// ────────────────────────────────────────────────────────────
// Vector Clock
// ────────────────────────────────────────────────────────────

/** A vector clock is a map from nodeId → logical time. */
export type VectorTimestamp = Record<string, number>;

export interface VectorStampedMessage {
  type: string;
  from: string;
  vectorTime: VectorTimestamp;
  payload: Record<string, unknown>;
}

/** Causal relationship between two vector timestamps. */
export type CausalRelation = 'before' | 'after' | 'concurrent' | 'equal';

export class VectorClock {
  private nodeId: string;
  private vector: VectorTimestamp = {};

  constructor(nodeId: string, peers: string[] = []) {
    this.nodeId = nodeId;
    this.vector[nodeId] = 0;
    for (const p of peers) this.vector[p] = 0;
  }

  /** Add a new peer to the known-peer set (used when a mandi joins). */
  addPeer(peerId: string): void {
    if (!(peerId in this.vector)) this.vector[peerId] = 0;
  }

  /** Increment own component — call before sending. */
  tick(): VectorTimestamp {
    this.vector[this.nodeId] = (this.vector[this.nodeId] ?? 0) + 1;
    return this.snapshot();
  }

  /**
   * Merge on receipt:
   *   V_self[k] = max(V_self[k], V_msg[k])  for all k
   *   V_self[self] += 1
   */
  receive(incoming: VectorTimestamp): VectorTimestamp {
    const allKeys = new Set([...Object.keys(this.vector), ...Object.keys(incoming)]);
    for (const k of allKeys) {
      this.vector[k] = Math.max(this.vector[k] ?? 0, incoming[k] ?? 0);
    }
    this.vector[this.nodeId] = (this.vector[this.nodeId] ?? 0) + 1;
    return this.snapshot();
  }

  /** Return a deep copy of the current vector. */
  snapshot(): VectorTimestamp {
    return { ...this.vector };
  }

  /** Stamp an outgoing message (ticks first). */
  stamp<T extends object>(msg: T): T & { vectorTime: VectorTimestamp; from: string } {
    const vt = this.tick();
    return { ...msg, vectorTime: vt, from: this.nodeId };
  }

  /** Node ID this clock belongs to. */
  getNodeId(): string { return this.nodeId; }

  /**
   * Determine the causal relationship between two vector timestamps.
   *
   * a < b  (a happened-before b): ∀k, a[k] ≤ b[k] ∧ ∃k, a[k] < b[k]
   * a > b  (b happened-before a): symmetric
   * a ∥ b  (concurrent):          ¬(a<b) ∧ ¬(b<a) ∧ a≠b
   * a = b  (equal):               ∀k, a[k] = b[k]
   */
  static compare(a: VectorTimestamp, b: VectorTimestamp): CausalRelation {
    const keys = new Set([...Object.keys(a), ...Object.keys(b)]);
    let aLeB = true; // a ≤ b
    let bLeA = true; // b ≤ a

    for (const k of keys) {
      const av = a[k] ?? 0;
      const bv = b[k] ?? 0;
      // a[k] > b[k] is evidence that a is NOT ≤ b (and vice versa).
      if (av > bv) aLeB = false;
      if (bv > av) bLeA = false;
    }

    if (aLeB && bLeA) return 'equal';
    if (aLeB) return 'before';
    if (bLeA) return 'after';
    return 'concurrent';
  }

  /**
   * Merge two price update records received at this node.
   * Returns the winner and a flag indicating if it was a conflict.
   *
   * Conflict-resolution rule:
   *   • Concurrent? Use Lamport scalar (if present) or nodeId tiebreak.
   *   • This gives deterministic LWW (Last-Write-Wins) without coordination.
   */
  static resolvePriceConflict(
    a: { from: string; vectorTime: VectorTimestamp; value: number; lamportTime?: number },
    b: { from: string; vectorTime: VectorTimestamp; value: number; lamportTime?: number }
  ): { winner: typeof a; relation: CausalRelation } {
    const relation = VectorClock.compare(a.vectorTime, b.vectorTime);

    if (relation === 'before' || relation === 'equal') {
      return { winner: b, relation };
    }
    if (relation === 'after') {
      return { winner: a, relation };
    }

    // Concurrent — LWW tiebreak
    const aL = a.lamportTime ?? 0;
    const bL = b.lamportTime ?? 0;
    if (aL !== bL) {
      return { winner: aL > bL ? a : b, relation };
    }
    return { winner: a.from > b.from ? a : b, relation };
  }
}

// ────────────────────────────────────────────────────────────
// LogicalClockManager – convenience per-node wrapper
// ────────────────────────────────────────────────────────────

/**
 * Bundles a LamportClock (for bid total-ordering) and a VectorClock
 * (for price-gossip causal tracking) together under one API so the
 * GossipProtocol and auction bidding path can share a single manager.
 */
export class LogicalClockManager {
  readonly nodeId: string;
  readonly lamport: LamportClock;
  readonly vector: VectorClock;

  constructor(nodeId: string, peers: string[] = []) {
    this.nodeId = nodeId;
    this.lamport = new LamportClock(nodeId);
    this.vector = new VectorClock(nodeId, peers);
  }

  /** Call when a new mandi peer joins the overlay. */
  addPeer(peerId: string): void {
    this.vector.addPeer(peerId);
    logger.debug({ nodeId: this.nodeId, newPeer: peerId }, 'LogicalClockManager: peer added');
  }

  /** Stamp outgoing bid or gossip message with both clocks. */
  stampMessage<T extends object>(msg: T): T & {
    lamportTime: number;
    vectorTime: VectorTimestamp;
    from: string;
  } {
    const lTime = this.lamport.tick();
    const vTime = this.vector.tick();
    return { ...msg, lamportTime: lTime, vectorTime: vTime, from: this.nodeId };
  }

  /** Process an incoming message: update both clocks. */
  receiveMessage(msg: { lamportTime?: number; vectorTime?: VectorTimestamp }): void {
    if (msg.lamportTime !== undefined) this.lamport.receive(msg.lamportTime);
    if (msg.vectorTime) this.vector.receive(msg.vectorTime);
  }

  getStats() {
    return {
      nodeId: this.nodeId,
      lamportTime: this.lamport.getTime(),
      vectorSnapshot: this.vector.snapshot(),
    };
  }
}
