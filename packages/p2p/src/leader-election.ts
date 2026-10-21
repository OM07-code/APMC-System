/**
 * Unit III – §4: Election Algorithms
 *
 * ┌─ Bug fixes from bug-log.md §D (P-H1…P-H4, P-M5) ──────────────────────┐
 * │ P-H1: Election never started at boot → now auto-started via startElection()
 * │ P-H2: All nodes priority=1, no ID tiebreak → distinct priorities +
 * │       nodeId tiebreak added to startElection / handleElection
 * │ P-H3: `responses` counted sends not OKs; electionTimeout never assigned →
 * │       now tracks actual OK replies and uses a real electionTimeout promise
 * │ P-H4: Followers never check leader liveness; recovery was dead code →
 * │       follower watchdog loop added; recovery properly re-starts election
 * │ P-M5: handleCoordinator accepted any COORDINATOR without priority check →
 * │       now validates sender priority before accepting as new leader
 * └────────────────────────────────────────────────────────────────────────┘
 *
 * Two algorithms:
 *
 * 1. Bully Algorithm (fixed)
 *    ─ Node with highest (priority, nodeId) becomes leader.
 *    ─ ELECTION → OK → COORDINATOR message flow.
 *    ─ If no OK arrives within timeout → self-elect.
 *
 * 2. Ring Election Algorithm
 *    ─ Nodes form a logical ring (sorted by nodeId).
 *    ─ Any node can initiate by sending ELECTION(candidateId, priority)
 *      to its successor.
 *    ─ Each node forwards the message with the best candidate seen so far.
 *    ─ When the original initiator receives its own message back, the best
 *      candidate is declared leader via a COORDINATOR message round.
 */

import { EventEmitter } from 'events';
import { logger } from '@apmc/middleware';
import { now, sleep } from '@apmc/shared';

// ────────────────────────────────────────────────────────────
// Shared Types
// ────────────────────────────────────────────────────────────

export enum NodeState {
  FOLLOWER  = 'FOLLOWER',
  CANDIDATE = 'CANDIDATE',
  LEADER    = 'LEADER',
}

export interface ClusterNode {
  id: string;
  host: string;
  port: number;
  priority: number;
  lastHeartbeat: number;
  state: NodeState;
}

export interface ElectionMessage {
  type: 'ELECTION' | 'OK' | 'COORDINATOR' | 'HEARTBEAT';
  from: string;
  fromPriority: number;  // sender's priority (for P-M5 fix)
  timestamp: number;
}

// ────────────────────────────────────────────────────────────
// Bully Leader Election (FIXED)
// ────────────────────────────────────────────────────────────

export class BullyLeaderElection extends EventEmitter {
  private nodeId: string;
  private host: string;
  private port: number;
  private priority: number;
  private nodes = new Map<string, ClusterNode>();
  private state: NodeState = NodeState.FOLLOWER;
  private currentLeader: string | null = null;
  private electionInProgress = false;

  // P-H3 fix: real timeout tracking and OK counter
  private electionTimeout: NodeJS.Timeout | null = null;
  private okReceived: Set<string> = new Set();

  // P-H4 fix: follower watchdog
  private heartbeatInterval: NodeJS.Timeout | null = null;
  private followerWatchdogTimer: NodeJS.Timeout | null = null;
  private static readonly FOLLOWER_TIMEOUT_MS = 15_000;
  private static readonly HEARTBEAT_INTERVAL_MS = 5_000;
  private static readonly ELECTION_TIMEOUT_MS = 3_000;

  // Callback: inject network send (replaced per-node in demo / integration)
  messageHandler: ((msg: ElectionMessage, targetId: string) => void) | null = null;

  constructor(nodeId: string, host: string, port: number, priority: number = 1) {
    super();
    this.nodeId = nodeId;
    this.host = host;
    this.port = port;
    this.priority = priority;

    this.nodes.set(nodeId, {
      id: nodeId, host, port, priority,
      lastHeartbeat: now(),
      state: NodeState.FOLLOWER,
    });
  }

  // ─── Public API ────────────────────────────────────────────

  getNodeId(): string { return this.nodeId; }
  getPriority(): number { return this.priority; }
  getState(): NodeState { return this.state; }
  getCurrentLeader(): string | null { return this.currentLeader; }
  isLeader(): boolean { return this.state === NodeState.LEADER; }

  setMessageHandler(handler: (msg: ElectionMessage, targetId: string) => void): void {
    this.messageHandler = handler;
  }

  addNode(node: ClusterNode): void {
    this.nodes.set(node.id, { ...node, lastHeartbeat: now() });
  }

  removeNode(nodeId: string): void { this.nodes.delete(nodeId); }

  updateHeartbeat(nodeId: string): void {
    const n = this.nodes.get(nodeId);
    if (n) n.lastHeartbeat = now();
  }

  // ─── Election ──────────────────────────────────────────────

  /** P-H1 fix: startElection can be called at boot or by follower watchdog. */
  async startElection(): Promise<void> {
    if (this.electionInProgress) return;

    this.electionInProgress = true;
    this.state = NodeState.CANDIDATE;
    this.okReceived.clear();

    logger.info({ nodeId: this.nodeId, priority: this.priority }, 'Bully: starting election');

    // Find nodes with strictly higher priority OR same priority but higher ID
    const higherNodes = Array.from(this.nodes.values()).filter(n => {
      if (n.id === this.nodeId) return false;
      if (now() - n.lastHeartbeat > 10_000) return false; // consider dead
      // P-H2 fix: proper tiebreak
      return n.priority > this.priority ||
             (n.priority === this.priority && n.id > this.nodeId);
    });

    if (higherNodes.length === 0) {
      // No higher-priority alive node → self-elect
      await this._becomeLeader();
      return;
    }

    // Send ELECTION to all higher-priority nodes
    for (const n of higherNodes) {
      this._send({ type: 'ELECTION', from: this.nodeId, fromPriority: this.priority, timestamp: now() }, n.id);
    }

    // P-H3 fix: wait for actual OKs, not just sends
    await new Promise<void>((resolve) => {
      this.electionTimeout = setTimeout(async () => {
        this.electionTimeout = null;
        if (this.okReceived.size === 0) {
          // No OK → self-elect
          await this._becomeLeader();
        }
        // If OKs received, wait for COORDINATOR (handled in handleCoordinator)
        this.electionInProgress = false;
        resolve();
      }, BullyLeaderElection.ELECTION_TIMEOUT_MS);
    });
  }

  // ─── Message Handling ──────────────────────────────────────

  async handleElectionMessage(msg: ElectionMessage): Promise<void> {
    this.updateHeartbeat(msg.from);

    switch (msg.type) {
      case 'ELECTION':    await this._handleElection(msg);    break;
      case 'OK':          this._handleOK(msg);               break;
      case 'COORDINATOR': await this._handleCoordinator(msg); break;
      case 'HEARTBEAT':   this._handleHeartbeat(msg);         break;
    }
  }

  private async _handleElection(msg: ElectionMessage): Promise<void> {
    // P-H2 fix: proper priority + ID tiebreak comparison
    const senderBeatsUs =
      msg.fromPriority > this.priority ||
      (msg.fromPriority === this.priority && msg.from > this.nodeId);

    if (!senderBeatsUs) {
      // We beat the sender → acknowledge with OK.
      this._send({ type: 'OK', from: this.nodeId, fromPriority: this.priority, timestamp: now() }, msg.from);

      if (this.isLeader()) {
        // Already the established leader: just re-assert authority. Starting a
        // fresh election here is what made round 1 re-elect the same node once
        // per incoming ELECTION (N-1 redundant rounds and COORDINATOR fan-outs),
        // because _becomeLeader() clears electionInProgress and so releases the
        // re-entry guard below.
        this._send(
          { type: 'COORDINATOR', from: this.nodeId, fromPriority: this.priority, timestamp: now() },
          msg.from
        );
        logger.debug(
          { nodeId: this.nodeId, from: msg.from },
          'Bully: already leader — answering ELECTION with COORDINATOR instead of re-electing'
        );
        return;
      }

      // Not leader. If we already know a LIVE leader that outranks us, there is
      // nothing to elect — just acknowledge. Without this, an ELECTION from any
      // lower-priority peer restarts our election, because _handleCoordinator
      // clears electionInProgress and so releases the guard below. With 6 nodes
      // that cascaded into ~8 redundant elections per node (an ELECTION storm).
      const leader = this.currentLeader ? this.nodes.get(this.currentLeader) : undefined;
      if (leader) {
        const leaderOutranksUs =
          leader.priority > this.priority ||
          (leader.priority === this.priority && leader.id > this.nodeId);
        const leaderAlive = now() - leader.lastHeartbeat < 10_000;
        if (leaderOutranksUs && leaderAlive) {
          logger.debug(
            { nodeId: this.nodeId, leader: leader.id, from: msg.from },
            'Bully: live higher-priority leader known — acknowledging without re-electing'
          );
          return;
        }
      }

      // No usable leader → contest the election ourselves.
      if (!this.electionInProgress) {
        setTimeout(() => this.startElection(), 100);
      }
    }
    // If sender beats us, we do nothing (let them proceed)
  }

  private _handleOK(msg: ElectionMessage): void {
    // P-H3 fix: count actual OKs
    this.okReceived.add(msg.from);
    logger.debug({ nodeId: this.nodeId, from: msg.from, okCount: this.okReceived.size }, 'Bully: OK received');
    // Actual election resolution happens when electionTimeout fires
  }

  private async _handleCoordinator(msg: ElectionMessage): Promise<void> {
    // P-M5 fix: validate sender's priority before accepting
    const newLeaderId = msg.from;
    const leaderNode = this.nodes.get(newLeaderId);
    const claimedPriority = msg.fromPriority;

    // Reject if a node with higher priority is still alive
    const higherAlive = Array.from(this.nodes.values()).some(n =>
      n.id !== newLeaderId &&
      n.id !== this.nodeId &&
      now() - n.lastHeartbeat < 10_000 &&
      (n.priority > claimedPriority ||
        (n.priority === claimedPriority && n.id > newLeaderId))
    );

    if (higherAlive) {
      logger.warn(
        { nodeId: this.nodeId, rejectedLeader: newLeaderId },
        'Bully: rejecting COORDINATOR — higher-priority node still alive'
      );
      return;
    }

    const wasLeader = this.isLeader();
    this.currentLeader = newLeaderId;
    this.state = newLeaderId === this.nodeId ? NodeState.LEADER : NodeState.FOLLOWER;
    this.electionInProgress = false;

    if (leaderNode) leaderNode.state = NodeState.LEADER;

    if (wasLeader && newLeaderId !== this.nodeId) {
      logger.info({ newLeader: newLeaderId }, 'Bully: leadership lost');
      this.emit('leadership-lost', newLeaderId);
    } else if (!wasLeader && newLeaderId === this.nodeId) {
      logger.info({ nodeId: this.nodeId }, 'Bully: became leader');
      this.emit('leadership-gained');
      this._startHeartbeats();
    } else {
      logger.info({ leader: newLeaderId }, 'Bully: new leader elected');
      this.emit('leader-changed', newLeaderId);
      // P-H4 fix: follower starts watchdog for leader liveness
      this._startFollowerWatchdog();
    }
  }

  private _handleHeartbeat(msg: ElectionMessage): void {
    const n = this.nodes.get(msg.from);
    if (n) { n.lastHeartbeat = now(); }

    // If we are leader, reply COORDINATOR to confirm
    if (this.isLeader()) {
      this._send({ type: 'COORDINATOR', from: this.nodeId, fromPriority: this.priority, timestamp: now() }, msg.from);
    }

    // P-H4 fix: reset follower watchdog on each heartbeat
    if (!this.isLeader() && msg.from === this.currentLeader) {
      this._startFollowerWatchdog();
    }
  }

  // ─── Self-election ─────────────────────────────────────────

  private async _becomeLeader(): Promise<void> {
    this.state = NodeState.LEADER;
    this.currentLeader = this.nodeId;
    this.electionInProgress = false;

    const selfNode = this.nodes.get(this.nodeId);
    if (selfNode) selfNode.state = NodeState.LEADER;

    // Broadcast COORDINATOR to all peers
    const coordinatorMsg: ElectionMessage = {
      type: 'COORDINATOR', from: this.nodeId, fromPriority: this.priority, timestamp: now(),
    };
    for (const n of this.nodes.values()) {
      if (n.id !== this.nodeId) {
        try { this._send(coordinatorMsg, n.id); } catch { /* ignore */ }
      }
    }

    this._startHeartbeats();
    this.emit('leadership-gained');
    logger.info({ nodeId: this.nodeId, priority: this.priority }, 'Bully: elected as leader');
  }

  // ─── Heartbeat (Leader → Followers) ───────────────────────

  private _startHeartbeats(): void {
    if (this.heartbeatInterval) clearInterval(this.heartbeatInterval);
    this.heartbeatInterval = setInterval(() => {
      if (!this.isLeader()) return;
      const hb: ElectionMessage = { type: 'HEARTBEAT', from: this.nodeId, fromPriority: this.priority, timestamp: now() };
      for (const n of this.nodes.values()) {
        if (n.id !== this.nodeId) {
          try { this._send(hb, n.id); } catch { /* ignore */ }
        }
      }
      this._checkNodeHealth();
    }, BullyLeaderElection.HEARTBEAT_INTERVAL_MS);
    this.heartbeatInterval.unref();
  }

  private _checkNodeHealth(): void {
    const nowTs = now();
    for (const [id, n] of this.nodes) {
      if (id === this.nodeId) continue;
      if (nowTs - n.lastHeartbeat > BullyLeaderElection.FOLLOWER_TIMEOUT_MS) {
        logger.warn({ failedNode: id }, 'Bully: node health check failed');
        this.nodes.delete(id);
        if (id === this.currentLeader && !this.isLeader()) {
          this.currentLeader = null;
          setTimeout(() => this.startElection(), 1000);
        }
      }
    }
  }

  // ─── P-H4 fix: Follower Watchdog ──────────────────────────

  private _startFollowerWatchdog(): void {
    if (this.followerWatchdogTimer) clearTimeout(this.followerWatchdogTimer);
    this.followerWatchdogTimer = setTimeout(() => {
      if (this.isLeader()) return;
      logger.warn(
        { nodeId: this.nodeId, leader: this.currentLeader },
        'Bully: leader timed out — starting new election'
      );
      this.currentLeader = null;
      this.startElection();
    }, BullyLeaderElection.FOLLOWER_TIMEOUT_MS);
    if (this.followerWatchdogTimer) this.followerWatchdogTimer.unref();
  }

  // ─── Helpers ───────────────────────────────────────────────

  private _send(msg: ElectionMessage, targetId: string): void {
    if (!this.messageHandler) return; // No-op when not networked
    this.messageHandler(msg, targetId);
  }

  simulateMessage(msg: ElectionMessage): void { this.handleElectionMessage(msg); }

  getClusterStatus() {
    return {
      leader: this.currentLeader,
      nodes: Array.from(this.nodes.values()),
      self: this.nodes.get(this.nodeId)!,
    };
  }

  shutdown(): void {
    if (this.heartbeatInterval) clearInterval(this.heartbeatInterval);
    if (this.electionTimeout) clearTimeout(this.electionTimeout);
    if (this.followerWatchdogTimer) clearTimeout(this.followerWatchdogTimer);
  }
}

// ────────────────────────────────────────────────────────────
// Ring Election Algorithm
// ────────────────────────────────────────────────────────────

export interface RingElectionMessage {
  type: 'RING_ELECTION' | 'RING_COORDINATOR';
  candidateId: string;
  candidatePriority: number;
  initiatorId: string;
  hops: number;
}

/**
 * Ring Election: nodes form a logical ring sorted by nodeId.
 * Any node can initiate; the node with the highest (priority, id) wins.
 *
 * Protocol:
 *   1. Initiator sends RING_ELECTION(self) to successor.
 *   2. Each node compares the candidate to itself; forwards the better one.
 *   3. When the initiator receives its own message back, it sends
 *      RING_COORDINATOR(winner) around the ring.
 *   4. Each node receiving COORDINATOR records the new leader.
 */
export class RingElection extends EventEmitter {
  readonly nodeId: string;
  private priority: number;
  private ring: string[]; // ordered ring of node IDs (must include self)
  private currentLeader: string | null = null;
  private participating = false;

  onSend: ((msg: RingElectionMessage, toSuccessor: string) => void) | null = null;

  constructor(nodeId: string, priority: number, ring: string[]) {
    super();
    this.nodeId = nodeId;
    this.priority = priority;
    // Ring must be sorted consistently across all nodes (e.g., lexicographic)
    this.ring = [...ring].sort();
  }

  private _successor(): string {
    const idx = this.ring.indexOf(this.nodeId);
    return this.ring[(idx + 1) % this.ring.length]!;
  }

  private _beats(id: string, priority: number, otherId: string, otherPriority: number): boolean {
    if (priority !== otherPriority) return priority > otherPriority;
    return id > otherId;
  }

  /** Initiate a new election on this node. */
  initiate(): void {
    this.participating = true;
    logger.info({ nodeId: this.nodeId }, 'Ring: initiating election');
    const msg: RingElectionMessage = {
      type: 'RING_ELECTION',
      candidateId: this.nodeId,
      candidatePriority: this.priority,
      initiatorId: this.nodeId,
      hops: 0,
    };
    this.onSend?.(msg, this._successor());
  }

  /** Receive a message from the predecessor. */
  receive(msg: RingElectionMessage): void {
    if (msg.type === 'RING_ELECTION') {
      this._handleElection(msg);
    } else {
      this._handleCoordinator(msg);
    }
  }

  private _handleElection(msg: RingElectionMessage): void {
    this.participating = true;

    // Is the incoming candidate better than what we know?
    const incomingBetter = this._beats(msg.candidateId, msg.candidatePriority, this.nodeId, this.priority);

    // Determine best candidate to forward
    const forwardId       = incomingBetter ? msg.candidateId : this.nodeId;
    const forwardPriority = incomingBetter ? msg.candidatePriority : this.priority;

    if (msg.initiatorId === this.nodeId) {
      // Message has gone all the way around — we are the initiator
      // The winner is forwardId (best seen across the ring)
      logger.info({ nodeId: this.nodeId, winner: forwardId }, 'Ring: election complete, sending COORDINATOR');
      const coord: RingElectionMessage = {
        type: 'RING_COORDINATOR',
        candidateId: forwardId,
        candidatePriority: forwardPriority,
        initiatorId: this.nodeId,
        hops: 0,
      };
      this.onSend?.(coord, this._successor());
    } else {
      const forwarded: RingElectionMessage = {
        type: 'RING_ELECTION',
        candidateId: forwardId,
        candidatePriority: forwardPriority,
        initiatorId: msg.initiatorId,
        hops: msg.hops + 1,
      };
      logger.debug({ nodeId: this.nodeId, forwarding: forwardId, hops: forwarded.hops }, 'Ring: forwarding ELECTION');
      this.onSend?.(forwarded, this._successor());
    }
  }

  private _handleCoordinator(msg: RingElectionMessage): void {
    this.currentLeader = msg.candidateId;
    this.participating = false;
    logger.info({ nodeId: this.nodeId, leader: msg.candidateId }, 'Ring: COORDINATOR received — leader elected');
    this.emit('leader-elected', msg.candidateId);

    // Forward around the ring until it reaches the initiator
    if (msg.initiatorId !== this.nodeId) {
      const forwarded: RingElectionMessage = { ...msg, hops: msg.hops + 1 };
      this.onSend?.(forwarded, this._successor());
    }
  }

  getCurrentLeader(): string | null { return this.currentLeader; }
  isLeader(): boolean { return this.currentLeader === this.nodeId; }
}