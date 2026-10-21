import { EventEmitter } from 'events';
import { createHash } from 'crypto';
import { config } from '@apmc/config';
import { logger } from '@apmc/middleware';
import { now, sleep } from '@apmc/shared';

export interface GossipMessage {
  type: 'SYNC' | 'NOTIFY' | 'ACK';
  from: string;
  timestamp: number;
  payload: Record<string, any>;
  version: number;
}

export interface GossipState {
  [key: string]: {
    value: any;
    version: number;
    timestamp: number;
    origin: string;
  };
}

export class GossipProtocol extends EventEmitter {
  private nodeId: string;
  private state: GossipState = {};
  private peers = new Map<string, { 
    id: string; 
    host: string; 
    port: number; 
    lastSeen: number;
    stateVersion: number;
  }>();
  private messageHandler: ((msg: GossipMessage, from: string) => void) | null = null;
  private gossipInterval: NodeJS.Timeout | null = null;
  private fanout = 3; // Number of peers to gossip with each round
  private interval = 5000; // Gossip interval in ms

  constructor(nodeId: string) {
    super();
    this.nodeId = nodeId;
  }

  setMessageHandler(handler: (msg: GossipMessage, from: string) => void): void {
    this.messageHandler = handler;
  }

  addPeer(peerId: string, host: string, port: number): void {
    this.peers.set(peerId, {
      id: peerId,
      host,
      port,
      lastSeen: now(),
      stateVersion: 0,
    });
  }

  removePeer(peerId: string): void {
    this.peers.delete(peerId);
  }

  updatePeerHeartbeat(peerId: string): void {
    const peer = this.peers.get(peerId);
    if (peer) {
      peer.lastSeen = now();
    }
  }

  // Set a value in local state
  setState(key: string, value: any): void {
    const current = this.state[key];
    const version = (current?.version ?? 0) + 1;
    
    this.state[key] = {
      value,
      version,
      timestamp: now(),
      origin: this.nodeId,
    };
    
    this.emit('state-changed', key, value, version);
  }

  // Get a value from local state
  getState(key: string): any {
    const entry = this.state[key];
    return entry?.value;
  }

  // Get full state
  getFullState(): GossipState {
    return { ...this.state };
  }

  // Start gossip protocol
  start(): void {
    if (this.gossipInterval) return;
    
    this.gossipInterval = setInterval(() => {
      this.gossipRound();
    }, this.interval);
    
    logger.info({ nodeId: this.nodeId }, 'Gossip protocol started');
  }

  stop(): void {
    if (this.gossipInterval) {
      clearInterval(this.gossipInterval);
      this.gossipInterval = null;
    }
    logger.info({ nodeId: this.nodeId }, 'Gossip protocol stopped');
  }

  // Perform one gossip round
  private async gossipRound(): Promise<void> {
    const alivePeers = Array.from(this.peers.values())
      .filter(p => now() - p.lastSeen < 15000)
      .sort(() => Math.random() - 0.5)
      .slice(0, this.fanout);

    if (alivePeers.length === 0) return;

    // Create SYNC message with our state
    const message: GossipMessage = {
      type: 'SYNC',
      from: this.nodeId,
      timestamp: now(),
      payload: this.state,
      version: this.getStateVersion(),
    };

    for (const peer of alivePeers) {
      try {
        await this.sendMessage(peer.id, message);
      } catch (error) {
        logger.debug({ err: error, peer: peer.id }, 'Failed to send gossip');
      }
    }
  }

  // Handle incoming gossip message
  handleMessage(msg: GossipMessage, fromPeerId: string): void {
    this.updatePeerHeartbeat(fromPeerId);
    
    switch (msg.type) {
      case 'SYNC':
        this.handleSync(msg, fromPeerId);
        break;
      case 'NOTIFY':
        this.handleNotify(msg, fromPeerId);
        break;
      case 'ACK':
        // Handle acknowledgment
        break;
    }
  }

  private handleSync(msg: GossipMessage, fromPeerId: string): void {
    let merged = false;
    
    for (const [key, remoteEntry] of Object.entries(msg.payload)) {
      const localEntry = this.state[key];
      
      // Use version vector for conflict resolution
      if (!localEntry || remoteEntry.version > localEntry.version) {
        this.state[key] = { ...remoteEntry };
        merged = true;
        this.emit('state-changed', key, remoteEntry.value, remoteEntry.version);
      } else if (remoteEntry.version === localEntry.version && 
                 remoteEntry.timestamp > localEntry.timestamp) {
        // Same version, newer timestamp wins
        this.state[key] = { ...remoteEntry };
        merged = true;
      }
    }
    
    // Send ACK back
    if (this.messageHandler) {
      this.messageHandler({
        type: 'ACK',
        from: this.nodeId,
        timestamp: now(),
        payload: { synced: merged },
        version: this.getStateVersion(),
      }, fromPeerId);
    }
  }

  private handleNotify(msg: GossipMessage, fromPeerId: string): void {
    // NOTIFY is a push-based update for a specific key
    const { key, value, version } = msg.payload;
    
    const localEntry = this.state[key];
    if (!localEntry || version > localEntry.version) {
      this.state[key] = {
        value,
        version,
        timestamp: msg.timestamp,
        origin: msg.from,
      };
      this.emit('state-changed', key, value, version);
    }
  }

  // Push a specific update to peers (for important updates)
  async notify(key: string, value: any): Promise<void> {
    const entry = this.state[key];
    if (!entry) return;
    
    const message: GossipMessage = {
      type: 'NOTIFY',
      from: this.nodeId,
      timestamp: now(),
      payload: { key, value, version: entry.version },
      version: entry.version,
    };
    
    const alivePeers = Array.from(this.peers.values())
      .filter(p => now() - p.lastSeen < 15000);
    
    for (const peer of alivePeers) {
      try {
        await this.sendMessage(peer.id, message);
      } catch {
        // Ignore individual failures
      }
    }
  }

  private async sendMessage(targetId: string, msg: GossipMessage): Promise<void> {
    if (!this.messageHandler) {
      throw new Error('No message handler set');
    }
    this.messageHandler(msg, targetId);
  }

  private getStateVersion(): number {
    return Object.values(this.state).reduce((max, entry) => Math.max(max, entry.version), 0);
  }

  // Simulate receiving a message (for testing)
  simulateMessage(msg: GossipMessage, from: string): void {
    this.handleMessage(msg, from);
  }

  getStats(): { nodeId: string; stateSize: number; peerCount: number; stateVersion: number } {
    return {
      nodeId: this.nodeId,
      stateSize: Object.keys(this.state).length,
      peerCount: this.peers.size,
      stateVersion: this.getStateVersion(),
    };
  }
}