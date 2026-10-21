/**
 * Unit III – §6 Self-Study: Knapp's Classification of Distributed Deadlock Detection
 *
 * APMC scenario — lock contention that can deadlock:
 *   • Buyer B1 holds a WALLET lock (to commit payment) and requests a LOT lock
 *     (to claim the won lot).
 *   • Farmer F1 holds a LOT lock (to update inventory) and requests a WALLET
 *     lock (to receive settlement).
 *   → Cycle: B1 → LOT ← F1 → WALLET ← B1 — classic deadlock.
 *
 * Knapp (1987) classifies distributed deadlock detection algorithms into four
 * orthogonal categories based on what global state information they propagate:
 *
 *  Category 1 – PATH PUSHING   (Obermarck's algorithm)
 *    Each node maintains a local Wait-For Graph (WFG) and exports "extended
 *    TWF paths" (transaction wait-for paths) to peer sites on edges that cross
 *    site boundaries.  A deadlock is detected when an exported path loops back.
 *
 *  Category 2 – EDGE CHASING   (Chandy–Misra–Haas for resource model)
 *    A "probe" message [initiator, sender, receiver] chases edges in the WFG.
 *    A deadlock is confirmed when the probe returns to its initiator.
 *    Minimal message complexity — probes are tiny and follow existing waits.
 *
 *  Category 3 – DIFFUSING COMPUTATION  (ECHO / Dijkstra–Scholten variant)
 *    A "query" diffuses along WFG edges; when all recipients reply, the
 *    initiator knows it is (transitively) blocked.  Uses an ECHO tree.
 *
 *  Category 4 – GLOBAL STATE DETECTION (snapshot-based)
 *    A consistent global snapshot (Chandy–Lamport) is taken and a WFG is
 *    constructed from the collected local states.  Cycle detection in the
 *    combined WFG reveals deadlocks.  Uses the existing GlobalSnapshot module.
 *
 * All four algorithms are implemented here as self-contained simulations.
 */

import { logger } from '@apmc/middleware';

// ────────────────────────────────────────────────────────────
// Shared Wait-For Graph Primitives
// ────────────────────────────────────────────────────────────

/** A directed edge P_i → P_j meaning "process i is waiting for process j". */
export interface WFGEdge {
  from: string; // waiting process
  to: string;   // blocking process
  resource: string;
}

/** A site's local Wait-For Graph. */
export class LocalWFG {
  readonly siteId: string;
  private edges: WFGEdge[] = [];

  constructor(siteId: string) {
    this.siteId = siteId;
  }

  addEdge(from: string, to: string, resource: string): void {
    if (!this.edges.find(e => e.from === from && e.to === to && e.resource === resource)) {
      this.edges.push({ from, to, resource });
    }
  }

  removeEdge(from: string, to: string): void {
    this.edges = this.edges.filter(e => !(e.from === from && e.to === to));
  }

  getEdges(): WFGEdge[] { return [...this.edges]; }

  /** All processes that `processId` is directly waiting for. */
  blockedBy(processId: string): string[] {
    return this.edges.filter(e => e.from === processId).map(e => e.to);
  }

  /** All processes that are waiting for `processId`. */
  waitersOf(processId: string): string[] {
    return this.edges.filter(e => e.to === processId).map(e => e.from);
  }
}

// ────────────────────────────────────────────────────────────
// Category 1: PATH PUSHING (Obermarck)
// ────────────────────────────────────────────────────────────

/** An "extended TWF path" that is exported between sites. */
export interface TWFPath {
  path: string[];   // sequence of process IDs (first waits for next, …)
  fromSite: string;
  toSite: string;
}

export class PathPushingDetector {
  readonly siteId: string;
  private localWFG: LocalWFG;
  private receivedPaths: TWFPath[] = [];

  onExportPath: ((path: TWFPath, toSite: string) => void) | null = null;
  onDeadlockDetected: ((cycle: string[]) => void) | null = null;

  constructor(siteId: string, localWFG: LocalWFG) {
    this.siteId = siteId;
    this.localWFG = localWFG;
  }

  /**
   * Export cross-site TWF paths: for every chain P1→P2→…→Pn where
   * Pn is on a remote site, export the path to that site.
   */
  exportPaths(remoteSites: string[], processToSite: Record<string, string>): void {
    for (const edge of this.localWFG.getEdges()) {
      const targetSite = processToSite[edge.to];
      if (targetSite && targetSite !== this.siteId && remoteSites.includes(targetSite)) {
        const path: TWFPath = {
          path: [edge.from, edge.to],
          fromSite: this.siteId,
          toSite: targetSite,
        };
        logger.debug({ from: this.siteId, path: path.path }, 'PATH PUSHING: exporting TWF path');
        this.onExportPath?.(path, targetSite);
      }
    }
  }

  /** Receive an exported path from another site. */
  receivePath(twfPath: TWFPath): void {
    this.receivedPaths.push(twfPath);

    // Check if path loops back — deadlock!
    const path = twfPath.path;
    const first = path[0]!;
    const last = path[path.length - 1]!;

    // If the last process waits for a process that is on this site,
    // and following local edges eventually reaches the first, we have a cycle.
    const visited = new Set<string>();
    const detectCycle = (current: string): boolean => {
      if (current === first) return true;
      if (visited.has(current)) return false;
      visited.add(current);
      for (const next of this.localWFG.blockedBy(current)) {
        if (detectCycle(next)) {
          path.push(next);
          return true;
        }
      }
      return false;
    };

    if (last && detectCycle(last)) {
      logger.warn({ path }, 'PATH PUSHING: *** DEADLOCK DETECTED ***');
      this.onDeadlockDetected?.(path);
    }
  }
}

// ────────────────────────────────────────────────────────────
// Category 2: EDGE CHASING — Chandy–Misra–Haas
// ────────────────────────────────────────────────────────────

export interface CMHProbe {
  initiator: string;
  sender: string;
  receiver: string;
  hopCount: number;
}

export class EdgeChasingDetector {
  readonly processId: string;
  private localWFG: LocalWFG;
  private outstandingProbes: Map<string, Set<string>> = new Map(); // initiator → senders seen

  onSendProbe: ((probe: CMHProbe, to: string) => void) | null = null;
  onDeadlockDetected: ((cycle: string, probe: CMHProbe) => void) | null = null;

  constructor(processId: string, localWFG: LocalWFG) {
    this.processId = processId;
    this.localWFG = localWFG;
  }

  /** Start a probe to detect if this process is in a deadlock. */
  initiateProbe(): void {
    for (const blockerPid of this.localWFG.blockedBy(this.processId)) {
      const probe: CMHProbe = {
        initiator: this.processId,
        sender: this.processId,
        receiver: blockerPid,
        hopCount: 0,
      };
      logger.debug({ initiator: this.processId, to: blockerPid }, 'CMH: initiating probe');
      this.onSendProbe?.(probe, blockerPid);
    }
  }

  /** Handle an incoming probe. */
  receiveProbe(probe: CMHProbe): void {
    const { initiator, sender } = probe;

    // Deadlock detected: probe has returned to its initiator
    if (probe.receiver === this.processId && initiator === this.processId) {
      logger.warn({ probe }, 'EDGE CHASING: *** DEADLOCK DETECTED — probe returned to initiator ***');
      this.onDeadlockDetected?.(`Deadlock cycle involving ${initiator}`, probe);
      return;
    }

    // Forward probe if this process is blocked
    for (const blockerPid of this.localWFG.blockedBy(this.processId)) {
      const forwarded: CMHProbe = {
        initiator,
        sender: this.processId,
        receiver: blockerPid,
        hopCount: probe.hopCount + 1,
      };
      logger.debug({ from: this.processId, to: blockerPid, initiator }, 'CMH: forwarding probe');
      this.onSendProbe?.(forwarded, blockerPid);
    }
  }
}

// ────────────────────────────────────────────────────────────
// Category 3: DIFFUSING COMPUTATION (ECHO)
// ────────────────────────────────────────────────────────────

export interface EchoQuery {
  queryId: string;
  initiator: string;
  from: string;
  to: string;
}

export interface EchoReply {
  queryId: string;
  from: string;
  to: string;
  isBlocked: boolean; // true if all children replied "blocked"
}

export class DiffusingComputationDetector {
  readonly processId: string;
  private localWFG: LocalWFG;
  private activeQueries: Map<string, { parent: string | null; totalChildren: number; engagedCount: number; blockedCount: number }> = new Map();
  // Guard: track queries we already forwarded to prevent infinite ECHO ping-pong in cycles
  private seenQueries: Set<string> = new Set();

  onSendQuery: ((query: EchoQuery, to: string) => void) | null = null;
  onSendReply: ((reply: EchoReply, to: string) => void) | null = null;
  onDeadlockDetected: ((queryId: string, initiator: string) => void) | null = null;

  constructor(processId: string, localWFG: LocalWFG) {
    this.processId = processId;
    this.localWFG = localWFG;
  }

  /** Initiate a query to find out if this process is transitively blocked. */
  initiateQuery(queryId: string): void {
    const blockers = this.localWFG.blockedBy(this.processId);
    if (blockers.length === 0) {
      logger.debug({ processId: this.processId }, 'ECHO: not blocked, no query needed');
      return;
    }

    this.seenQueries.add(`${queryId}:${this.processId}`);
    this.activeQueries.set(queryId, {
      parent: null,
      totalChildren: blockers.length,
      engagedCount: blockers.length,
      blockedCount: 0,
    });

    for (const blocker of blockers) {
      const q: EchoQuery = { queryId, initiator: this.processId, from: this.processId, to: blocker };
      logger.debug({ initiator: this.processId, to: blocker, queryId }, 'ECHO: sending query');
      this.onSendQuery?.(q, blocker);
    }
  }

  /** Receive a query from another process. */
  receiveQuery(query: EchoQuery): void {
    // Guard: if we've already processed this query, send a "blocked" reply up
    // to break the cycle (prevents infinite ping-pong in deadlock scenarios).
    const seenKey = `${query.queryId}:${this.processId}`;
    if (this.seenQueries.has(seenKey)) {
      const reply: EchoReply = { queryId: query.queryId, from: this.processId, to: query.from, isBlocked: true };
      this.onSendReply?.(reply, query.from);
      return;
    }
    this.seenQueries.add(seenKey);

    const blockers = this.localWFG.blockedBy(this.processId);

    if (blockers.length === 0) {
      // Not blocked — reply NOT blocked
      const reply: EchoReply = { queryId: query.queryId, from: this.processId, to: query.from, isBlocked: false };
      logger.debug({ processId: this.processId, to: query.from }, 'ECHO: not blocked, replying false');
      this.onSendReply?.(reply, query.from);
      return;
    }

    // Forward to blockers and wait
    this.activeQueries.set(query.queryId, {
      parent: query.from,
      totalChildren: blockers.length,
      engagedCount: blockers.length,
      blockedCount: 0,
    });

    for (const blocker of blockers) {
      const q: EchoQuery = { queryId: query.queryId, initiator: query.initiator, from: this.processId, to: blocker };
      this.onSendQuery?.(q, blocker);
    }
  }

  /** Receive a reply from a child in the ECHO tree. */
  receiveReply(reply: EchoReply): void {
    const state = this.activeQueries.get(reply.queryId);
    if (!state) return;

    if (reply.isBlocked) state.blockedCount++;
    state.engagedCount--;

    if (state.engagedCount <= 0) {
      // AND request model: this process is deadlocked only if EVERY branch of
      // its ECHO tree reported "blocked".  (Compare against the original child
      // count — engagedCount has already been decremented to 0 by now.)
      const allBlocked = state.blockedCount === state.totalChildren;

      if (state.parent === null) {
        // We are the initiator
        if (allBlocked) {
          logger.warn({ processId: this.processId, queryId: reply.queryId }, 'ECHO: *** DEADLOCK DETECTED ***');
          this.onDeadlockDetected?.(reply.queryId, this.processId);
        } else {
          logger.info(
            { processId: this.processId, queryId: reply.queryId, blocked: state.blockedCount, of: state.totalChildren },
            'ECHO: not deadlocked — at least one branch is running'
          );
        }
      } else {
        // Propagate reply upward: we are blocked iff all our children are
        const upReply: EchoReply = {
          queryId: reply.queryId,
          from: this.processId,
          to: state.parent,
          isBlocked: allBlocked,
        };
        this.onSendReply?.(upReply, state.parent);
      }
      this.activeQueries.delete(reply.queryId);
    }
  }
}

// ────────────────────────────────────────────────────────────
// Category 4: GLOBAL STATE DETECTION (Snapshot-based WFG)
// ────────────────────────────────────────────────────────────

/**
 * After a Chandy–Lamport snapshot is taken, each site's local WFG edges are
 * collected.  This detector assembles the global WFG and runs DFS cycle detection.
 */
export class GlobalStateDeadlockDetector {
  private globalEdges: WFGEdge[] = [];

  /** Ingest one site's WFG edges from a snapshot. */
  addSiteEdges(siteEdges: WFGEdge[]): void {
    this.globalEdges.push(...siteEdges);
  }

  /** Run DFS on the global WFG. Returns all detected cycles. */
  detectCycles(): string[][] {
    // Build adjacency list
    const adj = new Map<string, string[]>();
    for (const { from, to } of this.globalEdges) {
      if (!adj.has(from)) adj.set(from, []);
      adj.get(from)!.push(to);
    }

    const visited = new Set<string>();
    const stack = new Set<string>();
    const path: string[] = [];
    const cycles: string[][] = [];

    const dfs = (node: string): void => {
      if (stack.has(node)) {
        // Found a cycle — extract it
        const cycleStart = path.indexOf(node);
        cycles.push([...path.slice(cycleStart), node] as string[]);
        return;
      }
      if (visited.has(node)) return;

      visited.add(node);
      stack.add(node);
      path.push(node);

      for (const neighbor of (adj.get(node) ?? [])) {
        dfs(neighbor);
      }

      path.pop();
      stack.delete(node);
    };

    for (const node of adj.keys()) {
      if (!visited.has(node)) dfs(node);
    }

    if (cycles.length > 0) {
      logger.warn({ cycles }, 'GLOBAL STATE: *** DEADLOCK(S) DETECTED in global WFG ***');
    } else {
      logger.info({}, 'GLOBAL STATE: No deadlock detected in global WFG');
    }

    return cycles;
  }
}

// ────────────────────────────────────────────────────────────
// KnappClassifier — utility that summarises all 4 algorithms
// ────────────────────────────────────────────────────────────

export const KnappCategories = {
  PATH_PUSHING: {
    name: 'Path Pushing (Obermarck)',
    mechanism: 'Export cross-site TWF paths; detect loop-back',
    messageComplexity: 'O(E) per site boundary crossed',
    falsePositives: 'Possible (phantom deadlocks) in some variants',
  },
  EDGE_CHASING: {
    name: 'Edge Chasing (Chandy–Misra–Haas)',
    mechanism: 'Probe [i,s,r] chases WFG edges; detected when probe returns to initiator',
    messageComplexity: 'O(E) probes, each tiny (3 fields)',
    falsePositives: 'None (exact detection)',
  },
  DIFFUSING_COMPUTATION: {
    name: 'Diffusing Computation (ECHO)',
    mechanism: 'Query diffuses; ECHO tree collapses with blocked/unblocked reply',
    messageComplexity: 'O(N) messages (2 per tree edge)',
    falsePositives: 'None (exact detection)',
  },
  GLOBAL_STATE: {
    name: 'Global State Detection (Chandy–Lamport Snapshot + WFG)',
    mechanism: 'Consistent global snapshot; offline WFG cycle detection',
    messageComplexity: 'O(N²) for snapshot; O(V+E) DFS afterwards',
    falsePositives: 'Possible if snapshot is stale; resolved by re-checking',
  },
} as const;
