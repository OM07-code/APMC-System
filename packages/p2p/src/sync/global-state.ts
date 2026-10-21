/**
 * Unit III – §3: Global State — Chandy–Lamport Distributed Snapshot Algorithm
 *
 * APMC use-case — capturing a consistent global cut:
 *   An APMC regulator wants to verify that the total INR value in transit
 *   (placed bids whose settlement is in flight between mandis) equals the
 *   expected escrow balance.  A simple approach (pause everything and read all
 *   state) would halt the live auction.  The Chandy–Lamport algorithm records
 *   a consistent "cut" without stopping any mandi node.
 *
 * Mechanism:
 *   1. An initiating mandi node (the "initiator") records its own local state
 *      and immediately broadcasts a MARKER message on every outgoing channel.
 *   2. A process receiving a MARKER for the first time records its own local
 *      state and begins recording messages on all OTHER incoming channels.
 *   3. A process that has received a MARKER on every incoming channel assembles
 *      the full snapshot: its local state + the in-transit messages captured
 *      on each channel before the marker arrived.
 *
 * Assumptions (satisfied by our gRPC overlay):
 *   • FIFO channels (gRPC streams maintain order)
 *   • No failures during the snapshot
 *   • Messages do not vanish
 *
 * This module is self-contained (pure logic, no network I/O) so it can run
 * inside the demo script without any Docker infrastructure.
 */

import { logger } from '@apmc/middleware';
import { now } from '@apmc/shared';

// ────────────────────────────────────────────────────────────
// Types
// ────────────────────────────────────────────────────────────

/** The "local state" that each APMC mandi node checkpoints. */
export interface MandiLocalState {
  nodeId: string;
  activeBids: Array<{ auctionId: string; amount: number; buyerId: string }>;
  escrowBalance: number;    // INR held in escrow for this mandi's live auctions
  pendingSettlements: Array<{ auctionId: string; amount: number; to: string }>;
  recordedAt: number;
}

/** An application-level message in transit between two mandi nodes. */
export interface ChannelMessage {
  id: string;
  from: string;
  to: string;
  type: 'BID' | 'SETTLEMENT' | 'PRICE_UPDATE' | 'MARKER';
  payload: Record<string, unknown>;
  sentAt: number;
}

/** Per-channel recording state at a node during a snapshot. */
interface ChannelRecord {
  fromNode: string;
  markerReceived: boolean;
  inTransitMessages: ChannelMessage[]; // messages captured before MARKER arrived
}

/** The completed snapshot contribution from one process. */
export interface ProcessSnapshot {
  nodeId: string;
  localState: MandiLocalState;
  channelStates: Record<string, ChannelMessage[]>; // fromNodeId → in-transit messages
  completedAt: number;
}

/** A full, globally consistent cut assembled from all process snapshots. */
export interface GlobalSnapshot {
  snapshotId: string;
  initiatedBy: string;
  initiatedAt: number;
  processSnapshots: ProcessSnapshot[];
  totalEscrow: number;            // sum of all mandi escrow balances
  totalInTransit: number;         // sum of in-transit settlement amounts
  conservationHolds: boolean;     // totalEscrow + totalInTransit = expected?
  assembledAt: number;
}

// ────────────────────────────────────────────────────────────
// MandiNode — participates in the snapshot protocol
// ────────────────────────────────────────────────────────────

export class MandiSnapshotNode {
  readonly nodeId: string;
  private localState: MandiLocalState;
  private peers: string[] = [];

  // Snapshot-protocol runtime state (null = not in a snapshot)
  private snapshotInProgress: string | null = null;  // snapshotId
  private recordedLocalState: MandiLocalState | null = null;
  private channels: Map<string, ChannelRecord> = new Map();

  // Inject a callback so the node can "send" a MARKER to a peer
  onSendMarker: ((marker: ChannelMessage, toPeer: string) => void) | null = null;
  // Inject a callback so peers notify us when their snapshot is done
  onSnapshotComplete: ((snapshot: ProcessSnapshot) => void) | null = null;

  constructor(nodeId: string, initialState: MandiLocalState) {
    this.nodeId = nodeId;
    this.localState = { ...initialState };
  }

  /** Register known peers (unidirectional channels from each peer to this node). */
  addPeers(peers: string[]): void {
    this.peers = peers;
    for (const p of peers) {
      this.channels.set(p, {
        fromNode: p,
        markerReceived: false,
        inTransitMessages: [],
      });
    }
  }

  /** Update the node's live state (simulates ongoing auction activity). */
  updateState(patch: Partial<MandiLocalState>): void {
    this.localState = { ...this.localState, ...patch };
  }

  getLocalState(): MandiLocalState {
    return { ...this.localState };
  }

  // ─── Chandy–Lamport Protocol ───────────────────────────────

  /**
   * Step 1 (initiator only): Record own state and send MARKERs on all
   * outgoing channels.
   */
  initiateSnapshot(snapshotId: string): void {
    if (this.snapshotInProgress) {
      logger.warn({ nodeId: this.nodeId }, 'Snapshot already in progress, ignoring');
      return;
    }

    this.snapshotInProgress = snapshotId;
    this.recordedLocalState = { ...this.localState, recordedAt: now() };

    // Mark all incoming channels as "recording" (no messages captured yet)
    for (const [, record] of this.channels) {
      record.markerReceived = false;
      record.inTransitMessages = [];
    }

    logger.info({ nodeId: this.nodeId, snapshotId }, 'Snapshot initiated — local state recorded');

    // Broadcast MARKER on every outgoing channel
    for (const peer of this.peers) {
      const marker: ChannelMessage = {
        id: `marker-${snapshotId}-${this.nodeId}-${peer}`,
        from: this.nodeId,
        to: peer,
        type: 'MARKER',
        payload: { snapshotId },
        sentAt: now(),
      };
      this.onSendMarker?.(marker, peer);
    }
  }

  /**
   * Step 2: Called when this node receives an application message while a
   * snapshot is in progress.  If the sending channel hasn't yet delivered its
   * MARKER, record this message as "in transit."
   */
  receiveMessage(msg: ChannelMessage): void {
    if (!this.snapshotInProgress) {
      // No snapshot in progress — just deliver normally
      this.deliverMessage(msg);
      return;
    }

    const record = this.channels.get(msg.from);
    if (record && !record.markerReceived) {
      // Channel is in recording mode — capture this message
      record.inTransitMessages.push(msg);
      logger.debug(
        { nodeId: this.nodeId, from: msg.from, msgId: msg.id },
        'Recording in-transit message on channel'
      );
    }
    // Always deliver to the application
    this.deliverMessage(msg);
  }

  /**
   * Step 3: Called when this node receives a MARKER from a peer.
   * If this is the FIRST marker for this node, record own state and send
   * MARKERs on all outgoing channels.
   * When all channels have received their MARKER, assemble and report the
   * process snapshot.
   */
  receiveMarker(marker: ChannelMessage): void {
    const snapshotId = marker.payload['snapshotId'] as string;

    // First marker ever received for this snapshot
    if (!this.snapshotInProgress) {
      this.snapshotInProgress = snapshotId;
      this.recordedLocalState = { ...this.localState, recordedAt: now() };

      // Initialize recording on all channels
      for (const [, record] of this.channels) {
        record.markerReceived = false;
        record.inTransitMessages = [];
      }

      logger.info({ nodeId: this.nodeId, snapshotId, from: marker.from }, 'Snapshot joined — local state recorded');

      // Propagate MARKER on outgoing channels (excluding back to sender to avoid double-send)
      for (const peer of this.peers) {
        const m: ChannelMessage = {
          id: `marker-${snapshotId}-${this.nodeId}-${peer}`,
          from: this.nodeId,
          to: peer,
          type: 'MARKER',
          payload: { snapshotId },
          sentAt: now(),
        };
        this.onSendMarker?.(m, peer);
      }
    }

    // Mark this channel as done
    const record = this.channels.get(marker.from);
    if (record) {
      record.markerReceived = true;
      logger.debug({ nodeId: this.nodeId, from: marker.from }, 'MARKER received, channel recording complete');
    }

    // Check if all channels are done
    if (this.isSnapshotComplete()) {
      this.finalizeSnapshot(snapshotId);
    }
  }

  private isSnapshotComplete(): boolean {
    if (!this.snapshotInProgress) return false;
    for (const [, record] of this.channels) {
      if (!record.markerReceived) return false;
    }
    return true;
  }

  private finalizeSnapshot(snapshotId: string): void {
    const channelStates: Record<string, ChannelMessage[]> = {};
    for (const [peerId, record] of this.channels) {
      channelStates[peerId] = [...record.inTransitMessages];
    }

    const snapshot: ProcessSnapshot = {
      nodeId: this.nodeId,
      localState: this.recordedLocalState!,
      channelStates,
      completedAt: now(),
    };

    logger.info({ nodeId: this.nodeId, snapshotId }, 'Process snapshot complete');

    this.snapshotInProgress = null;
    this.recordedLocalState = null;

    this.onSnapshotComplete?.(snapshot);
  }

  /** Stub: deliver message to the application layer. */
  private deliverMessage(msg: ChannelMessage): void {
    logger.debug({ to: this.nodeId, from: msg.from, type: msg.type }, 'Message delivered');
  }
}

// ────────────────────────────────────────────────────────────
// GlobalSnapshotCoordinator — assembles individual snapshots
// ────────────────────────────────────────────────────────────

/**
 * Collects ProcessSnapshot reports from all mandi nodes and assembles the
 * final GlobalSnapshot, verifying that mandi escrow balances + in-transit
 * settlement values sum to the expected total (conservation check).
 */
export class GlobalSnapshotCoordinator {
  private snapshotId: string;
  private expectedNodes: string[];
  private collected: ProcessSnapshot[] = [];
  private expectedEscrowTotal: number;

  constructor(snapshotId: string, expectedNodes: string[], expectedEscrowTotal: number) {
    this.snapshotId = snapshotId;
    this.expectedNodes = expectedNodes;
    this.expectedEscrowTotal = expectedEscrowTotal;
  }

  addProcessSnapshot(ps: ProcessSnapshot): void {
    this.collected.push(ps);
    logger.info(
      { snapshotId: this.snapshotId, from: ps.nodeId, count: this.collected.length, total: this.expectedNodes.length },
      'Process snapshot received'
    );
  }

  isComplete(): boolean {
    return this.collected.length >= this.expectedNodes.length;
  }

  assemble(): GlobalSnapshot {
    // Sum escrow balances across all nodes
    const totalEscrow = this.collected.reduce(
      (s, ps) => s + ps.localState.escrowBalance, 0
    );

    // Sum in-transit SETTLEMENT messages (money that left one node but hasn't
    // arrived at the other yet — these are captured in channel states)
    let totalInTransit = 0;
    for (const ps of this.collected) {
      for (const msgs of Object.values(ps.channelStates)) {
        for (const m of msgs) {
          if (m.type === 'SETTLEMENT') {
            totalInTransit += (m.payload['amount'] as number) ?? 0;
          }
        }
      }
    }

    const conservationHolds =
      Math.abs((totalEscrow + totalInTransit) - this.expectedEscrowTotal) < 0.01;

    const gs: GlobalSnapshot = {
      snapshotId: this.snapshotId,
      initiatedBy: this.expectedNodes[0]!,
      initiatedAt: Math.min(...this.collected.map(ps => ps.localState.recordedAt)),
      processSnapshots: this.collected,
      totalEscrow,
      totalInTransit,
      conservationHolds,
      assembledAt: now(),
    };

    logger.info(
      { snapshotId: this.snapshotId, totalEscrow, totalInTransit, conservationHolds },
      'Global snapshot assembled'
    );

    return gs;
  }
}
