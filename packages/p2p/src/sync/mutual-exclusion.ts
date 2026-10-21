/**
 * Unit III – §5: Mutual Exclusion
 *
 * Two algorithms demonstrated with APMC mandi-settlement locking:
 *
 * 1. Ricart–Agrawala Algorithm (timestamp-based, 2(N-1) messages)
 *    ─ A mandi wishing to lock the settlement ledger for farmer F broadcasts
 *      REQUEST(ts, nodeId) to all N-1 peers.
 *    ─ A peer sends REPLY immediately unless it has a lower timestamp
 *      (or equal ts with lower nodeId) and is also requesting; otherwise it
 *      defers until it exits the CS.
 *    ─ The requester enters the CS once it has N-1 REPLYs.
 *
 * 2. Lodha–Kshemkalyani Fair Mutual Exclusion (N-1 ≤ messages ≤ 2(N-1))
 *    ─ Extends Ricart–Agrawala with a Local Request Queue (LRQ) at each node.
 *    ─ Three message types: REQUEST, REPLY, FLUSH.
 *    ─ A node that must DEFER (it is in the CS, or holds a higher-priority
 *      request) sends NOTHING — it simply enqueues the requester in its LRQ.
 *      This is where the message saving comes from: Ricart–Agrawala must
 *      eventually send one deferred REPLY to every waiter.
 *    ─ FLUSH is a RELEASE-time permission handoff: on exiting the CS a node
 *      sends a single FLUSH to the highest-priority entry in its LRQ.  Because
 *      the sender has already ordered its LRQ by Lamport timestamp, the FLUSH
 *      recipient is provably next and may enter the CS immediately WITHOUT
 *      collecting its remaining REPLYs.  One FLUSH thus replaces a fan-out of
 *      N-1 deferred REPLYs.
 *    ─ Entry rule:  (all N-1 REPLYs received)  OR  (a FLUSH was handed to us).
 *    ─ Fairness: the FLUSH chain follows strict Lamport-timestamp order, so no
 *      request can be overtaken and no node starves.
 *
 * Both algorithms are purely computational — no network I/O needed for the demo.
 */

import { logger } from '@apmc/middleware';
import { now } from '@apmc/shared';
import { LamportClock } from './logical-clocks';

// ────────────────────────────────────────────────────────────
// Shared Types
// ────────────────────────────────────────────────────────────

export type MutexMessageType = 'REQUEST' | 'REPLY' | 'FLUSH' | 'RELEASE';

export interface MutexMessage {
  type: MutexMessageType;
  from: string;
  timestamp: number;   // Lamport time at sender
  resource: string;    // e.g. 'settlement:farmer:F001'
}

export interface MutexStats {
  algorithm: string;
  nodeId: string;
  enterCount: number;
  messagesSent: number;
  messagesReceived: number;
  avgMessagesPerCS: number;
  /** LK only: CS entries that came from a FLUSH handoff rather than N-1 REPLYs. */
  flushEntries?: number;
  /** LK only: FLUSH handoffs this node sent on release. */
  flushHandoffs?: number;
}

// ────────────────────────────────────────────────────────────
// 1. Ricart–Agrawala Algorithm
// ────────────────────────────────────────────────────────────

type RAState = 'RELEASED' | 'WANTED' | 'HELD';

export class RicartAgrawalaMutex {
  readonly nodeId: string;
  private peers: string[];
  private clock: LamportClock;
  private state: RAState = 'RELEASED';
  private requestTimestamp: number = 0;
  private repliesReceived: Set<string> = new Set();
  private deferredQueue: string[] = [];      // peers we owe a REPLY
  private resource: string;

  // Metrics
  private enterCount = 0;
  private msgSent = 0;
  private msgReceived = 0;

  // Callback to "send" a message over the (simulated) network
  onSend: ((msg: MutexMessage, to: string) => void) | null = null;
  // Callback fired when this node actually acquires the CS
  onEnterCS: (() => void) | null = null;

  constructor(nodeId: string, peers: string[], resource: string) {
    this.nodeId = nodeId;
    this.peers = peers;
    this.resource = resource;
    this.clock = new LamportClock(nodeId);
  }

  /** Request entry to the critical section. */
  requestCS(): void {
    this.state = 'WANTED';
    this.requestTimestamp = this.clock.tick();
    this.repliesReceived.clear();

    logger.info(
      { nodeId: this.nodeId, ts: this.requestTimestamp, resource: this.resource },
      'RA: broadcasting REQUEST'
    );

    for (const peer of this.peers) {
      this._send({ type: 'REQUEST', from: this.nodeId, timestamp: this.requestTimestamp, resource: this.resource }, peer);
    }
  }

  /** Process an incoming message from another node. */
  receive(msg: MutexMessage): void {
    this.clock.receive(msg.timestamp);
    this.msgReceived++;

    switch (msg.type) {
      case 'REQUEST':
        this._handleRequest(msg);
        break;
      case 'REPLY':
        this._handleReply(msg);
        break;
      case 'RELEASE':
        // Not used in RA (RELEASE is implicit via deferred REPLYs), but
        // included for completeness when integrating with other algorithms.
        break;
      default:
        break;
    }
  }

  private _handleRequest(msg: MutexMessage): void {
    // Defer REPLY if we are requesting with higher priority
    const weHavePriority =
      this.state === 'WANTED' &&
      (this.requestTimestamp < msg.timestamp ||
        (this.requestTimestamp === msg.timestamp && this.nodeId < msg.from));

    if (this.state === 'HELD' || weHavePriority) {
      // Defer
      this.deferredQueue.push(msg.from);
      logger.debug({ nodeId: this.nodeId, deferring: msg.from }, 'RA: REQUEST deferred');
    } else {
      // Reply immediately
      this._send({ type: 'REPLY', from: this.nodeId, timestamp: this.clock.tick(), resource: this.resource }, msg.from);
    }
  }

  private _handleReply(msg: MutexMessage): void {
    if (this.state !== 'WANTED') return;

    this.repliesReceived.add(msg.from);
    logger.debug(
      { nodeId: this.nodeId, from: msg.from, count: this.repliesReceived.size, need: this.peers.length },
      'RA: REPLY received'
    );

    if (this.repliesReceived.size >= this.peers.length) {
      this._enterCS();
    }
  }

  private _enterCS(): void {
    this.state = 'HELD';
    this.enterCount++;
    logger.info({ nodeId: this.nodeId, resource: this.resource }, 'RA: *** ENTERED critical section ***');
    this.onEnterCS?.();
  }

  /** Release the critical section. */
  releaseCS(): void {
    if (this.state !== 'HELD') return;
    this.state = 'RELEASED';

    logger.info({ nodeId: this.nodeId, deferred: this.deferredQueue.length }, 'RA: releasing CS — sending deferred REPLYs');

    for (const peer of this.deferredQueue) {
      this._send({ type: 'REPLY', from: this.nodeId, timestamp: this.clock.tick(), resource: this.resource }, peer);
    }
    this.deferredQueue = [];
  }

  private _send(msg: MutexMessage, to: string): void {
    this.msgSent++;
    this.onSend?.(msg, to);
  }

  getStats(): MutexStats {
    return {
      algorithm: 'Ricart-Agrawala',
      nodeId: this.nodeId,
      enterCount: this.enterCount,
      messagesSent: this.msgSent,
      messagesReceived: this.msgReceived,
      avgMessagesPerCS: this.enterCount > 0 ? this.msgSent / this.enterCount : 0,
    };
  }
}

// ────────────────────────────────────────────────────────────
// 2. Lodha–Kshemkalyani Fair Mutual Exclusion
// ────────────────────────────────────────────────────────────

/**
 * Key ideas:
 *   • Each node maintains a Local Request Queue (LRQ) of pending requests.
 *   • Deferring a request costs ZERO messages — the requester is just parked
 *     in the LRQ.  (Ricart–Agrawala instead owes that node a REPLY later.)
 *   • On exiting the CS, a node sorts its LRQ by Lamport timestamp and sends a
 *     single FLUSH to the head.  FLUSH = "the CS is yours, you are provably
 *     next" — so the recipient enters without its remaining REPLYs.
 *   • Net effect: one FLUSH replaces N-1 deferred REPLYs, while the handoff
 *     chain enforces strict Lamport order (fairness, no starvation).
 *
 * Liveness argument: for any requester, every peer either REPLYs or parks it
 * in an LRQ, so replies + parks = N-1.  If it collected all N-1 REPLYs it
 * enters directly; otherwise at least one peer holds it in an LRQ and will
 * hand the CS over by FLUSH when that peer exits — transitively, in Lamport
 * order.  Hence no request is lost and none is overtaken.
 */

export interface LKPendingRequest {
  from: string;
  timestamp: number;
}

type LKState = 'RELEASED' | 'WANTED' | 'HELD';

export class LodhaKshemkalyaniMutex {
  readonly nodeId: string;
  private peers: string[];
  private clock: LamportClock;
  private state: LKState = 'RELEASED';
  private requestTimestamp: number = 0;
  private repliesReceived: Set<string> = new Set();

  // LRQ — pending requests this node may hand the CS to when it exits
  private lrq: LKPendingRequest[] = [];
  // Set when a previous CS holder handed the CS to us directly via FLUSH
  private grantedByFlush = false;

  private resource: string;

  // Metrics
  private enterCount = 0;
  private msgSent = 0;
  private msgReceived = 0;
  private flushEntries = 0;    // CS entries granted by FLUSH handoff
  private flushHandoffs = 0;   // FLUSH handoffs we sent on release

  onSend: ((msg: MutexMessage, to: string) => void) | null = null;
  onEnterCS: (() => void) | null = null;

  constructor(nodeId: string, peers: string[], resource: string) {
    this.nodeId = nodeId;
    this.peers = peers;
    this.resource = resource;
    this.clock = new LamportClock(nodeId);
  }

  requestCS(): void {
    this.state = 'WANTED';
    this.requestTimestamp = this.clock.tick();
    this.repliesReceived.clear();
    this.grantedByFlush = false;

    logger.info({ nodeId: this.nodeId, ts: this.requestTimestamp }, 'LK: broadcasting REQUEST');

    for (const peer of this.peers) {
      this._send({ type: 'REQUEST', from: this.nodeId, timestamp: this.requestTimestamp, resource: this.resource }, peer);
    }
  }

  receive(msg: MutexMessage): void {
    this.clock.receive(msg.timestamp);
    this.msgReceived++;

    switch (msg.type) {
      case 'REQUEST': this._handleRequest(msg); break;
      case 'REPLY':   this._handleReply(msg);   break;
      case 'FLUSH':   this._handleFlush(msg);   break;
      default: break;
    }
  }

  private _handleRequest(msg: MutexMessage): void {
    const weHavePriority =
      this.state === 'WANTED' &&
      (this.requestTimestamp < msg.timestamp ||
        (this.requestTimestamp === msg.timestamp && this.nodeId < msg.from));

    if (this.state === 'HELD' || weHavePriority) {
      // DEFER — costs zero messages.  Park the requester in our LRQ; we will
      // hand the CS to the highest-priority entry by FLUSH when we exit.
      // (Ricart–Agrawala would instead owe this node a deferred REPLY.)
      this._enqueueLRQ({ from: msg.from, timestamp: msg.timestamp });
      logger.debug(
        { nodeId: this.nodeId, deferred: msg.from, lrqSize: this.lrq.length },
        'LK: REQUEST deferred into LRQ (0 messages sent)'
      );
    } else {
      // No contention — grant permission immediately.
      this._send({ type: 'REPLY', from: this.nodeId, timestamp: this.clock.tick(), resource: this.resource }, msg.from);
    }
  }

  /**
   * FLUSH = release-time permission handoff from the previous CS holder.
   * The sender had already sorted its LRQ by Lamport timestamp, so we are
   * provably next and may enter the CS without our remaining REPLYs.
   */
  private _handleFlush(msg: MutexMessage): void {
    if (this.state === 'WANTED') {
      logger.debug(
        { nodeId: this.nodeId, from: msg.from },
        'LK: FLUSH received — CS handed over, entering without remaining REPLYs'
      );
      this.grantedByFlush = true;
      this._enterCS();
      return;
    }

    // We are not waiting (we already entered via REPLYs, or we are in the CS).
    // Do not drop the grant — pass the handoff down our own LRQ so the next
    // requester in Lamport order still gets it (preserves liveness).
    logger.debug(
      { nodeId: this.nodeId, from: msg.from, state: this.state },
      'LK: FLUSH received while not WANTED — forwarding handoff'
    );
    this._handoffToNext();
  }

  private _handleReply(msg: MutexMessage): void {
    if (this.state !== 'WANTED') return;
    this.repliesReceived.add(msg.from);
    logger.debug({ nodeId: this.nodeId, from: msg.from }, 'LK: REPLY received');
    this._checkCanEnterCS();
  }

  /**
   * Entry rule: all N-1 REPLYs received.  (The FLUSH path enters directly in
   * _handleFlush and never goes through here — a FLUSH is a grant, whereas a
   * deferral is silent and must NOT be counted as permission.)
   */
  private _checkCanEnterCS(): void {
    if (this.state !== 'WANTED') return;
    if (this.repliesReceived.size >= this.peers.length) {
      this._enterCS();
    }
  }

  private _enterCS(): void {
    this.state = 'HELD';
    this.enterCount++;
    if (this.grantedByFlush) this.flushEntries++;
    logger.info(
      { nodeId: this.nodeId, resource: this.resource, viaFlush: this.grantedByFlush },
      'LK: *** ENTERED critical section ***'
    );
    this.onEnterCS?.();
  }

  releaseCS(): void {
    if (this.state !== 'HELD') return;
    this.state = 'RELEASED';
    this.grantedByFlush = false;

    logger.info(
      { nodeId: this.nodeId, lrqSize: this.lrq.length },
      'LK: releasing CS — handing over to highest-priority LRQ entry'
    );

    this._handoffToNext();
  }

  /**
   * Hand the CS to the single highest-priority (lowest Lamport timestamp,
   * nodeId tiebreak) entry in the LRQ via one FLUSH message.
   *
   * The remaining LRQ entries are dropped deliberately: every one of them was
   * also parked at the node we just handed over to (or at another contender
   * that outranks them), so they are served transitively down the FLUSH chain.
   * Keeping them here would risk granting the CS twice.
   */
  private _handoffToNext(): void {
    if (this.lrq.length === 0) {
      logger.debug({ nodeId: this.nodeId }, 'LK: LRQ empty — no handoff needed (0 messages)');
      return;
    }

    // Sort by Lamport timestamp, nodeId as tiebreak → strict fairness order
    this.lrq.sort((a, b) =>
      a.timestamp !== b.timestamp ? a.timestamp - b.timestamp : a.from < b.from ? -1 : 1
    );

    const next = this.lrq[0]!;
    this.lrq = [];
    this.flushHandoffs++;

    logger.info(
      { nodeId: this.nodeId, handoffTo: next.from, ts: next.timestamp },
      'LK: FLUSH handoff — 1 message replaces N-1 deferred REPLYs'
    );

    this._send(
      { type: 'FLUSH', from: this.nodeId, timestamp: this.clock.tick(), resource: this.resource },
      next.from
    );
  }

  private _enqueueLRQ(req: LKPendingRequest): void {
    // Avoid duplicates
    if (!this.lrq.find(r => r.from === req.from)) {
      this.lrq.push(req);
    }
  }

  private _send(msg: MutexMessage, to: string): void {
    this.msgSent++;
    this.onSend?.(msg, to);
  }

  getState(): LKState { return this.state; }

  /** Pending requests parked in this node's Local Request Queue. */
  getLRQ(): ReadonlyArray<LKPendingRequest> { return [...this.lrq]; }

  getStats(): MutexStats {
    return {
      algorithm: 'Lodha-Kshemkalyani',
      nodeId: this.nodeId,
      enterCount: this.enterCount,
      messagesSent: this.msgSent,
      messagesReceived: this.msgReceived,
      avgMessagesPerCS: this.enterCount > 0 ? this.msgSent / this.enterCount : 0,
      flushEntries: this.flushEntries,
      flushHandoffs: this.flushHandoffs,
    };
  }
}
