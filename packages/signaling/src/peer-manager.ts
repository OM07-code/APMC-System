import { Redis } from 'ioredis';
import { config } from '@apmc/config';
import { logger } from '@apmc/middleware';
import { generatePeerId, now } from '@apmc/shared';
import { AppError, NotFoundError } from '@apmc/shared';

// WebRTC types (normally from @types/web or dom lib)
interface RTCIceServer {
  urls: string | string[];
  username?: string;
  credential?: string;
}

export interface Peer {
  peer_id: string;
  peer_type: 'auctioneer' | 'buyer' | 'farmer' | 'observer';
  apmc_location: string;
  user_id?: string;
  metadata: Record<string, string>;
  is_online: boolean;
  last_seen: number;
  connected_at: number;
  socket_id?: string;
}

export interface SignalMessage {
  from_peer_id: string;
  to_peer_id: string;
  type: 'offer' | 'answer' | 'ice_candidate';
  payload: {
    sdp?: string;
    type?: string;
    candidate?: string;
    sdp_mid?: string;
    sdp_mline_index?: number;
  };
  timestamp: number;
}

const PEER_KEY_PREFIX = 'apmc:peer:';
const PEER_ONLINE_SET = 'apmc:peers:online';
const PEER_LOCATION_PREFIX = 'apmc:peers:location:';
const SIGNAL_QUEUE_PREFIX = 'apmc:signals:queue:';
const PEER_TTL = 300; // 5 minutes

export class PeerManager {
  private redis: Redis;

  constructor(redis?: Redis) {
    this.redis = redis || new Redis(config.REDIS_URL, {
      maxRetriesPerRequest: 3,
      retryStrategy: (times) => Math.min(times * 100, 3000),
      enableReadyCheck: true,
      lazyConnect: true,
    });
  }

  async connect(): Promise<void> {
    if (this.redis.status === 'wait') {
      await this.redis.connect();
    }
  }

  async disconnect(): Promise<void> {
    await this.redis.quit();
  }

  // Peer registration
  async registerPeer(peer: Omit<Peer, 'peer_id' | 'is_online' | 'last_seen' | 'connected_at'>): Promise<Peer> {
    await this.connect();
    
    const peerId = generatePeerId();
    const nowTimestamp = now();
    
    const newPeer: Peer = {
      ...peer,
      peer_id: peerId,
      is_online: true,
      last_seen: nowTimestamp,
      connected_at: nowTimestamp,
    };

    // Store peer data
    await this.redis.hset(`${PEER_KEY_PREFIX}${peerId}`, {
      peer_id: peerId,
      peer_type: peer.peer_type,
      apmc_location: peer.apmc_location,
      user_id: peer.user_id || '',
      metadata: JSON.stringify(peer.metadata),
      is_online: '1',
      last_seen: nowTimestamp.toString(),
      connected_at: nowTimestamp.toString(),
    });

    // Add to online set
    await this.redis.sadd(PEER_ONLINE_SET, peerId);
    
    // Add to location index
    await this.redis.sadd(`${PEER_LOCATION_PREFIX}${peer.apmc_location}`, peerId);
    
    // Set TTL for auto-cleanup
    await this.redis.expire(`${PEER_KEY_PREFIX}${peerId}`, PEER_TTL);
    
    logger.info({ peer_id: peerId, type: peer.peer_type, location: peer.apmc_location }, 'Peer registered');
    
    return newPeer;
  }

  async unregisterPeer(peerId: string): Promise<boolean> {
    await this.connect();
    
    const peerData = await this.redis.hgetall(`${PEER_KEY_PREFIX}${peerId}`);
    if (!peerData || Object.keys(peerData).length === 0) {
      return false;
    }

    // Remove from online set
    await this.redis.srem(PEER_ONLINE_SET, peerId);
    
    // Remove from location index
    const location = peerData['apmc_location'];
    if (location) {
      await this.redis.srem(`${PEER_LOCATION_PREFIX}${location}`, peerId);
    }
    
    // Delete peer data
    await this.redis.del(`${PEER_KEY_PREFIX}${peerId}`);
    
    // Clear signal queue
    await this.redis.del(`${SIGNAL_QUEUE_PREFIX}${peerId}`);
    
    logger.info({ peer_id: peerId }, 'Peer unregistered');
    return true;
  }

  async updatePeerHeartbeat(peerId: string, socketId?: string): Promise<Peer | null> {
    await this.connect();
    
    const nowTimestamp = now();
    const updates: Record<string, string> = {
      is_online: '1',
      last_seen: nowTimestamp.toString(),
    };
    
    if (socketId) {
      (updates as any).socket_id = socketId;
    }
    
    // Bug fix: redis.hset returns the count of NEWLY created fields, so updating an
    // ALREADY-registered peer returns 0. The old `if (result === 0) return null`
    // therefore fired on every heartbeat for existing peers and skipped the TTL reset
    // below — so peers expired after PEER_TTL despite active heartbeats. Gate on key
    // existence instead, then update and refresh the TTL.
    const exists = await this.redis.exists(`${PEER_KEY_PREFIX}${peerId}`);
    if (!exists) {
      return null;
    }

    await this.redis.hset(`${PEER_KEY_PREFIX}${peerId}`, updates);

    // Reset TTL so active peers stay alive
    await this.redis.expire(`${PEER_KEY_PREFIX}${peerId}`, PEER_TTL);
    
    return this.getPeer(peerId);
  }

  async getPeer(peerId: string): Promise<Peer | null> {
    await this.connect();
    
    const data = await this.redis.hgetall(`${PEER_KEY_PREFIX}${peerId}`);
    if (!data || Object.keys(data).length === 0) {
      return null;
    }
    
    return {
      peer_id: data['peer_id'] || '',
      peer_type: data['peer_type'] as Peer['peer_type'],
      apmc_location: data['apmc_location'] || '',
      user_id: data['user_id'] || undefined,
      metadata: JSON.parse(data['metadata'] || '{}'),
      is_online: data['is_online'] === '1',
      last_seen: parseInt(data['last_seen'] || '0', 10),
      connected_at: parseInt(data['connected_at'] || '0', 10),
      socket_id: data['socket_id'],
    };
  }

  async getPeersByLocation(location: string, peerType?: string): Promise<Peer[]> {
    await this.connect();
    
    const peerIds = await this.redis.smembers(`${PEER_LOCATION_PREFIX}${location}`);
    const peers: Peer[] = [];
    
    for (const peerId of peerIds) {
      const peer = await this.getPeer(peerId);
      if (peer && (!peerType || peer.peer_type === peerType)) {
        peers.push(peer);
      }
    }
    
    return peers;
  }

  async getOnlinePeers(location?: string, peerType?: string): Promise<Peer[]> {
    await this.connect();
    
    let peerIds: string[];
    
    if (location) {
      peerIds = await this.redis.sinter(PEER_ONLINE_SET, `${PEER_LOCATION_PREFIX}${location}`);
    } else {
      peerIds = await this.redis.smembers(PEER_ONLINE_SET);
    }
    
    const peers: Peer[] = [];
    for (const peerId of peerIds) {
      const peer = await this.getPeer(peerId);
      if (peer && (!peerType || peer.peer_type === peerType)) {
        peers.push(peer);
      }
    }
    
    return peers;
  }

  // Signal queuing for offline peers
  async queueSignal(signal: SignalMessage): Promise<void> {
    await this.connect();
    
    const queueKey = `${SIGNAL_QUEUE_PREFIX}${signal.to_peer_id}`;
    await this.redis.lpush(queueKey, JSON.stringify(signal));
    await this.redis.ltrim(queueKey, 0, 99); // Keep last 100 signals
    await this.redis.expire(queueKey, 3600); // 1 hour TTL
  }

  async getQueuedSignals(peerId: string): Promise<SignalMessage[]> {
    await this.connect();
    
    const queueKey = `${SIGNAL_QUEUE_PREFIX}${peerId}`;
    const signals = await this.redis.lrange(queueKey, 0, -1);
    
    // Clear queue after retrieval
    await this.redis.del(queueKey);
    
    return signals.map(s => JSON.parse(s));
  }

  // Direct signal delivery (for online peers)
  async deliverSignal(signal: SignalMessage): Promise<boolean> {
    await this.connect();
    
    const targetPeer = await this.getPeer(signal.to_peer_id);
    if (!targetPeer || !targetPeer.is_online || !targetPeer.socket_id) {
      // Queue for later delivery
      await this.queueSignal(signal);
      return false;
    }
    
    // In a real implementation, this would use WebSocket to deliver
    // For now, we'll use Redis pub/sub
    const channel = `apmc:signals:${targetPeer.socket_id}`;
    await this.redis.publish(channel, JSON.stringify(signal));
    
    return true;
  }

  // ICE servers configuration
  getICEServers(): RTCIceServer[] {
    const servers: RTCIceServer[] = [
      { urls: 'stun:stun.l.google.com:19302' },
      { urls: 'stun:stun1.l.google.com:19302' },
    ];
    
    if (config.TURN_SERVER) {
      servers.push({
        urls: config.TURN_SERVER,
        username: config.TURN_USERNAME,
        credential: config.TURN_CREDENTIAL,
      });
    }
    
    return servers;
  }

  // Cleanup stale peers
  async cleanupStalePeers(): Promise<number> {
    await this.connect();
    
    const onlinePeers = await this.redis.smembers(PEER_ONLINE_SET);
    let cleaned = 0;
    
    for (const peerId of onlinePeers) {
      const peer = await this.getPeer(peerId);
      if (peer && now() - peer.last_seen > PEER_TTL * 1000) {
        await this.unregisterPeer(peerId);
        cleaned++;
      }
    }
    
    return cleaned;
  }
}

export const peerManager = new PeerManager();