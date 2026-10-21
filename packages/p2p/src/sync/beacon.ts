/**
 * Unit III – §4 (supplement): Beacon Protocol
 *
 * A Beacon Protocol is a lightweight liveness & time-sync broadcast mechanism:
 *   • The current cluster leader (elected by Bully or Ring) periodically
 *     broadcasts a BEACON frame to all peers.
 *   • Each BEACON carries: senderID, Lamport timestamp, corrected physical
 *     time (from Cristian/Berkeley sync), epoch counter, and a load indicator.
 *   • Followers update their failure-suspicion timers on each BEACON receipt;
 *     if no beacon arrives within `suspicionTimeoutMs` they raise a SUSPECT
 *     event which triggers a new Bully election.
 *   • Secondary use: the BEACON doubles as a lightweight time-sync anchor —
 *     followers compute `offset = beaconTime − localTime` and apply a
 *     low-pass filtered correction (α = 0.1, standard NTP-style).
 *
 * This module is intentionally decoupled from the election module so both can
 * evolve independently and be composed flexibly.
 */

import { EventEmitter } from 'events';
import { logger } from '@apmc/middleware';
import { now } from '@apmc/shared';

// ────────────────────────────────────────────────────────────
// Types
// ────────────────────────────────────────────────────────────

export interface BeaconFrame {
  type: 'BEACON';
  senderId: string;
  lamportTime: number;
  physicalTime: number;   // ms since epoch (after Cristian/Berkeley correction)
  epoch: number;          // increments each time a new leader is elected
  load: number;           // 0-100 normalized load indicator
}

export interface BeaconStats {
  nodeId: string;
  role: 'leader' | 'follower';
  epoch: number;
  beaconsSent: number;
  beaconsReceived: number;
  lastBeaconFrom: string | null;
  lastBeaconAt: number | null;
  clockOffset: number;    // estimated offset vs beacon sender (ms)
  suspectedNodes: string[];
}

// ────────────────────────────────────────────────────────────
// BeaconManager
// ────────────────────────────────────────────────────────────

export class BeaconManager extends EventEmitter {
  readonly nodeId: string;
  private peers: string[];
  private isLeader: boolean = false;
  private epoch: number = 0;
  private lamportTime: number = 0;

  // Beacon scheduling
  private beaconIntervalMs: number;
  private suspicionTimeoutMs: number;
  private beaconTimer: NodeJS.Timeout | null = null;

  // Peer liveness tracking
  private lastBeaconFromPeer: Map<string, number> = new Map(); // peerId → timestamp
  private suspectedNodes: Set<string> = new Set();

  // NTP-style clock offset (low-pass filtered)
  private clockOffsetMs: number = 0;
  private readonly ALPHA = 0.1; // filter coefficient

  // Metrics
  private beaconsSent = 0;
  private beaconsReceived = 0;
  private lastBeaconFrom: string | null = null;
  private lastBeaconAt: number | null = null;

  // Inject: how to broadcast a beacon to all peers
  onBroadcastBeacon: ((frame: BeaconFrame) => void) | null = null;

  constructor(
    nodeId: string,
    peers: string[],
    beaconIntervalMs: number = 3000,
    suspicionTimeoutMs: number = 9000 // 3× beacon interval (default)
  ) {
    super();
    this.nodeId = nodeId;
    this.peers = [...peers];
    this.beaconIntervalMs = beaconIntervalMs;
    this.suspicionTimeoutMs = suspicionTimeoutMs;
  }

  // ─── Leadership ────────────────────────────────────────────

  /** Called when this node wins an election. */
  becomeLeader(epoch: number): void {
    this.isLeader = true;
    this.epoch = epoch;
    this.startBeaconing();
    logger.info({ nodeId: this.nodeId, epoch }, 'Beacon: became leader — starting beacon broadcasts');
    this.emit('became-leader', epoch);
  }

  /** Called when this node learns of a new leader (becomes follower). */
  becomeFollower(leaderId: string, epoch: number): void {
    this.isLeader = false;
    this.epoch = epoch;
    this.stopBeaconing();
    logger.info({ nodeId: this.nodeId, leaderId, epoch }, 'Beacon: became follower');
    this.emit('became-follower', leaderId, epoch);
  }

  // ─── Beacon Sending (Leader only) ──────────────────────────

  private startBeaconing(): void {
    this.stopBeaconing();
    this.beaconTimer = setInterval(() => {
      this._broadcastBeacon();
      this._checkPeerLiveness();
    }, this.beaconIntervalMs);
    this.beaconTimer.unref(); // don't block process exit
  }

  private stopBeaconing(): void {
    if (this.beaconTimer) {
      clearInterval(this.beaconTimer);
      this.beaconTimer = null;
    }
  }

  private _broadcastBeacon(): void {
    if (!this.isLeader) return;

    this.lamportTime += 1;
    const frame: BeaconFrame = {
      type: 'BEACON',
      senderId: this.nodeId,
      lamportTime: this.lamportTime,
      physicalTime: now(),
      epoch: this.epoch,
      load: this._sampleLoad(),
    };

    this.beaconsSent++;
    logger.debug({ nodeId: this.nodeId, epoch: this.epoch, lt: this.lamportTime }, 'Beacon: broadcasting');
    this.onBroadcastBeacon?.(frame);
  }

  private _sampleLoad(): number {
    // Simulate load; in production this would be a real metric
    return Math.round(Math.random() * 30 + 10); // 10–40%
  }

  // ─── Peer Liveness (Leader checks followers) ───────────────

  private _checkPeerLiveness(): void {
    const nowTs = now();
    for (const peer of this.peers) {
      const last = this.lastBeaconFromPeer.get(peer); // followers heartbeat back? (simplified)
      // In a fuller implementation followers send ACK/HEARTBEAT back.
      // Here we track beacons received from peers (gossip beacons).
      if (last && nowTs - last > this.suspicionTimeoutMs) {
        if (!this.suspectedNodes.has(peer)) {
          this.suspectedNodes.add(peer);
          logger.warn({ nodeId: this.nodeId, suspected: peer }, 'Beacon: peer SUSPECTED (no heartbeat)');
          this.emit('peer-suspected', peer);
        }
      } else if (last && this.suspectedNodes.has(peer)) {
        this.suspectedNodes.delete(peer);
        this.emit('peer-recovered', peer);
      }
    }
  }

  // ─── Beacon Receiving (Followers) ──────────────────────────

  /** Called when a beacon from the cluster leader (or any peer) is received. */
  receiveBeacon(frame: BeaconFrame): void {
    // Update Lamport clock
    this.lamportTime = Math.max(this.lamportTime, frame.lamportTime) + 1;

    // Update liveness tracking
    const nowTs = now();
    this.lastBeaconFromPeer.set(frame.senderId, nowTs);
    this.lastBeaconFrom = frame.senderId;
    this.lastBeaconAt = nowTs;
    this.beaconsReceived++;

    // Clear suspicion if previously suspected
    if (this.suspectedNodes.has(frame.senderId)) {
      this.suspectedNodes.delete(frame.senderId);
      logger.info({ nodeId: this.nodeId, from: frame.senderId }, 'Beacon: peer recovered');
      this.emit('peer-recovered', frame.senderId);
    }

    // NTP-style clock offset update (low-pass filter)
    const rawOffset = frame.physicalTime - nowTs;
    this.clockOffsetMs = this.clockOffsetMs * (1 - this.ALPHA) + rawOffset * this.ALPHA;

    // Epoch change → new leader elected, become follower
    if (frame.epoch > this.epoch) {
      this.epoch = frame.epoch;
      this.becomeFollower(frame.senderId, frame.epoch);
    }

    logger.debug(
      { nodeId: this.nodeId, from: frame.senderId, epoch: frame.epoch, offset: this.clockOffsetMs.toFixed(1) },
      'Beacon: received'
    );

    this.emit('beacon-received', frame);

    // Follower liveness: if NO beacon arrives within suspicionTimeout,
    // raise SUSPECT for the leader, triggering a new election.
    this._resetLeaderSuspicionTimer(frame.senderId);
  }

  // ─── Follower-side leader failure detector ─────────────────

  private leaderSuspicionTimer: NodeJS.Timeout | null = null;
  private currentLeaderNode: string | null = null;

  private _resetLeaderSuspicionTimer(leaderId: string): void {
    this.currentLeaderNode = leaderId;
    if (this.leaderSuspicionTimer) clearTimeout(this.leaderSuspicionTimer);

    this.leaderSuspicionTimer = setTimeout(() => {
      logger.warn(
        { nodeId: this.nodeId, suspectedLeader: leaderId },
        'Beacon: *** LEADER SUSPECTED — no beacon received within timeout ***'
      );
      this.emit('leader-suspected', leaderId);
      // In integration: the P2P service would call leaderElection.startElection() here
    }, this.suspicionTimeoutMs);

    if (this.leaderSuspicionTimer) this.leaderSuspicionTimer.unref();
  }

  // ─── Peer Management ───────────────────────────────────────

  addPeer(peerId: string): void {
    if (!this.peers.includes(peerId)) {
      this.peers.push(peerId);
    }
  }

  removePeer(peerId: string): void {
    this.peers = this.peers.filter(p => p !== peerId);
    this.lastBeaconFromPeer.delete(peerId);
    this.suspectedNodes.delete(peerId);
  }

  // ─── Shutdown ──────────────────────────────────────────────

  shutdown(): void {
    this.stopBeaconing();
    if (this.leaderSuspicionTimer) clearTimeout(this.leaderSuspicionTimer);
    logger.info({ nodeId: this.nodeId }, 'BeaconManager shutdown');
  }

  // ─── Stats ─────────────────────────────────────────────────

  getStats(): BeaconStats {
    return {
      nodeId: this.nodeId,
      role: this.isLeader ? 'leader' : 'follower',
      epoch: this.epoch,
      beaconsSent: this.beaconsSent,
      beaconsReceived: this.beaconsReceived,
      lastBeaconFrom: this.lastBeaconFrom,
      lastBeaconAt: this.lastBeaconAt,
      clockOffset: parseFloat(this.clockOffsetMs.toFixed(2)),
      suspectedNodes: Array.from(this.suspectedNodes),
    };
  }
}
