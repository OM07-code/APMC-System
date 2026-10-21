/**
 * Unit III – §1: Clock Synchronization
 *
 * Two algorithms demonstrated in the APMC auction context:
 *
 * 1. Cristian's Algorithm (external clock sync)
 *    ─ A mandi node queries a designated APMC Reference Clock Server.
 *    ─ Offset = T_server + RTT/2 - T_client
 *    ─ Use-case: all 6 mandi nodes align their "auction-close" timers to the
 *      same APMC reference so that a farmer's lot isn't prematurely ended by a
 *      slow local clock.
 *
 * 2. Berkeley Algorithm (internal / averaging sync)
 *    ─ An elected "time daemon" mandi node collects clock readings from peers,
 *      discards outliers (fault-tolerant), computes the average, and broadcasts
 *      per-node correction deltas.
 *    ─ Use-case: multi-mandi coordination of settlement windows where no
 *      external authority is reachable.
 *
 * This file is purely computational (no I/O) so it can be driven by a
 * standalone demo script with no infrastructure dependencies.
 */

import { logger } from '@apmc/middleware';
import { now, sleep } from '@apmc/shared';

// ────────────────────────────────────────────────────────────
// Shared Types
// ────────────────────────────────────────────────────────────

export interface ClockReading {
  nodeId: string;
  localTime: number;   // ms since epoch (local clock)
  receivedAt?: number; // time at which the master received this reading
}

export interface ClockCorrection {
  nodeId: string;
  delta: number;       // ms to add to the node's local clock
}

// ────────────────────────────────────────────────────────────
// Cristian's Algorithm
// ────────────────────────────────────────────────────────────

/**
 * Simulates an APMC Reference Clock Server.
 * In a real deployment this would be an NTP server or a GPS-disciplined clock
 * at the APMC headquarters; here we simulate it with an optional artificial
 * drift to show the correction mechanism.
 */
export class APMCReferenceClockServer {
  private driftMs: number; // artificial drift added to real time

  constructor(driftMs: number = 0) {
    this.driftMs = driftMs;
  }

  /** Returns the server's current "true" time (Date.now() ± drift). */
  getTime(): number {
    return Date.now() + this.driftMs;
  }
}

export interface CristianSyncResult {
  nodeId: string;
  T0: number;         // request sent (local)
  T1: number;         // server time received in response
  T2: number;         // response received (local)
  rtt: number;        // round-trip time  T2 - T0
  offset: number;     // estimated clock offset (positive → local is behind)
  correctedTime: number;
}

/**
 * Cristian's Algorithm: single round-trip, assumes symmetric network latency.
 *
 *   offset = T_server + RTT/2 − T_client_at_midpoint
 *   where T_client_at_midpoint ≈ (T0 + T2) / 2
 *
 * The mandi node simulates sending a TIME_REQUEST, the reference server
 * responds with its clock, and the node computes the offset.
 */
export async function cristianSync(
  nodeId: string,
  server: APMCReferenceClockServer,
  simulatedNetworkLatencyMs: number = 20
): Promise<CristianSyncResult> {
  const T0 = now();

  // Simulate network round-trip
  await sleep(simulatedNetworkLatencyMs / 2);
  const T1 = server.getTime();          // server's clock at midpoint
  await sleep(simulatedNetworkLatencyMs / 2);

  const T2 = now();
  const rtt = T2 - T0;
  const offset = T1 + rtt / 2 - T2;   // classic Cristian formula
  const correctedTime = T2 + offset;

  logger.debug(
    { nodeId, T0, T1, T2, rtt, offset },
    'Cristian sync completed'
  );

  return { nodeId, T0, T1, T2, rtt, offset, correctedTime };
}

// ────────────────────────────────────────────────────────────
// Berkeley Algorithm
// ────────────────────────────────────────────────────────────

export interface BerkeleyNode {
  id: string;
  localTime: number;
}

export interface BerkeleyRoundResult {
  masterId: string;
  masterTime: number;
  readings: ClockReading[];
  outliers: string[];         // node IDs excluded as outliers
  averageTime: number;        // fault-tolerant average
  corrections: ClockCorrection[];
}

/**
 * Berkeley Algorithm (master-based, fault-tolerant):
 *
 * 1. Master (time-daemon mandi) broadcasts TIME_REQUEST.
 * 2. Each slave replies with its local time.
 * 3. Master receives times, computes RTT-corrected readings, discards
 *    nodes whose offset from the master exceeds `outlierThresholdMs`.
 * 4. Master computes average of non-outlier clocks (including its own).
 * 5. Master sends each slave its individual correction delta.
 *
 * @param master      - The mandi node elected as time-daemon (by Bully / Ring).
 * @param slaves      - Remaining mandi nodes (their reported local clocks).
 * @param networkLatencyMs - Simulated per-leg network latency for RTT adjustment.
 * @param outlierThresholdMs - Nodes differing by more than this are excluded.
 */
export function berkeleySync(
  master: BerkeleyNode,
  slaves: BerkeleyNode[],
  networkLatencyMs: number = 20,
  outlierThresholdMs: number = 500
): BerkeleyRoundResult {
  // Step 1 – Master collects readings; RTT-adjust slave times by +½ latency
  const readings: ClockReading[] = [
    { nodeId: master.id, localTime: master.localTime, receivedAt: master.localTime },
    ...slaves.map(s => ({
      nodeId: s.id,
      localTime: s.localTime,
      receivedAt: master.localTime + networkLatencyMs / 2, // when master "got" it
    })),
  ];

  // Step 2 – Compute each node's offset from the master clock
  const masterTime = master.localTime;
  const offsets = readings.map(r => ({
    nodeId: r.nodeId,
    offset: r.localTime - masterTime,
  }));

  // Step 3 – Fault-tolerant: discard outliers
  const outliers: string[] = offsets
    .filter(o => Math.abs(o.offset) > outlierThresholdMs)
    .map(o => o.nodeId);

  const validOffsets = offsets.filter(o => !outliers.includes(o.nodeId));

  // Step 4 – Average clock = master + mean(offsets of valid nodes)
  const meanOffset = validOffsets.reduce((s, o) => s + o.offset, 0) / validOffsets.length;
  const averageTime = masterTime + meanOffset;

  // Step 5 – Per-node corrections: delta = averageTime − nodeLocalTime
  const corrections: ClockCorrection[] = readings.map(r => ({
    nodeId: r.nodeId,
    delta: averageTime - r.localTime,
  }));

  logger.info(
    { masterId: master.id, outliers, averageTime, corrections },
    'Berkeley sync round completed'
  );

  return {
    masterId: master.id,
    masterTime,
    readings,
    outliers,
    averageTime,
    corrections,
  };
}

// ────────────────────────────────────────────────────────────
// ClockManager  – per-node runtime helper
// ────────────────────────────────────────────────────────────

/**
 * Lightweight wrapper that applies Cristian / Berkeley corrections to this
 * node's logical "corrected time" without touching the system clock.
 */
export class ClockManager {
  private nodeId: string;
  private offsetMs: number = 0;  // cumulative correction from sync rounds
  private syncHistory: Array<{ algorithm: string; offset: number; ts: number }> = [];

  constructor(nodeId: string) {
    this.nodeId = nodeId;
  }

  /** Return corrected wall-clock time for this node. */
  getCorrectedTime(): number {
    return Date.now() + this.offsetMs;
  }

  /** Apply a Cristian sync result. */
  applyCristian(result: CristianSyncResult): void {
    this.offsetMs += result.offset;
    this.syncHistory.push({ algorithm: 'Cristian', offset: result.offset, ts: Date.now() });
    logger.info({ nodeId: this.nodeId, offset: result.offset, cumulative: this.offsetMs }, 'Cristian offset applied');
  }

  /** Apply a Berkeley correction for this node. */
  applyBerkeley(correction: ClockCorrection): void {
    this.offsetMs += correction.delta;
    this.syncHistory.push({ algorithm: 'Berkeley', offset: correction.delta, ts: Date.now() });
    logger.info({ nodeId: this.nodeId, delta: correction.delta, cumulative: this.offsetMs }, 'Berkeley correction applied');
  }

  getStats() {
    return {
      nodeId: this.nodeId,
      offsetMs: this.offsetMs,
      correctedTime: this.getCorrectedTime(),
      syncCount: this.syncHistory.length,
      history: this.syncHistory.slice(-5),
    };
  }
}
