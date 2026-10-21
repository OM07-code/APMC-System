import { createHash } from 'crypto';
import { config } from '@apmc/config';
import { logger } from '@apmc/middleware';
import { now, sleep } from '@apmc/shared';

const K = 20; // Kademlia bucket size
const ALPHA = 3; // Concurrency factor
const REPLICATION_FACTOR = 3;

export interface DHTNode {
  id: string;
  host: string;
  port: number;
  lastSeen: number;
  isSelf: boolean;
}

export interface DHTValue {
  key: string;
  value: Uint8Array;
  version: number;
  timestamp: number;
  ttl: number;
}

export class KademliaDHT {
  private nodeId: string;
  private buckets: Map<number, DHTNode[]> = new Map();
  private storage = new Map<string, DHTValue>();
  private localHost: string;
  private localPort: number;

  constructor(nodeId?: string, host: string = 'localhost', port: number = 0) {
    this.nodeId = nodeId || this.generateNodeId();
    this.localHost = host;
    this.localPort = port;
    
    // Initialize buckets (160 buckets for 160-bit IDs)
    for (let i = 0; i < 160; i++) {
      this.buckets.set(i, []);
    }
  }

  private generateNodeId(): string {
    return createHash('sha256').update(`${Date.now()}-${Math.random()}`).digest('hex').slice(0, 40);
  }

  getNodeId(): string {
    return this.nodeId;
  }

  // XOR distance between two node IDs
  static distance(id1: string, id2: string): bigint {
    const bytes1 = Buffer.from(id1, 'hex');
    const bytes2 = Buffer.from(id2, 'hex');
    let distance = 0n;
    for (let i = 0; i < bytes1.length; i++) {
      const b1 = bytes1[i] ?? 0;
      const b2 = bytes2[i] ?? 0;
      distance = (distance << 8n) | BigInt(b1 ^ b2);
    }
    return distance;
  }

  // Get bucket index for a given distance
  private getBucketIndex(distance: bigint): number {
    if (distance === 0n) return 0;
    return distance.toString(2).length - 1;
  }

  // Add or update a node in the routing table
  addNode(node: DHTNode): void {
    if (node.id === this.nodeId) return;
    
    const distance = KademliaDHT.distance(this.nodeId, node.id);
    const bucketIndex = this.getBucketIndex(distance);
    const bucket = this.buckets.get(bucketIndex) || [];
    
    // Check if node already exists
    const existingIndex = bucket.findIndex(n => n.id === node.id);
    if (existingIndex >= 0) {
      // Move to front (most recently seen)
      bucket.splice(existingIndex, 1);
      bucket.unshift(node);
    } else if (bucket.length < K) {
      // Add to front
      bucket.unshift(node);
    } else {
      // Bucket full - ping least recently seen
      // In a real implementation, we'd ping the last node
      // For now, we'll just replace if the new node is closer
      const lastNode = bucket[bucket.length - 1];
      if (lastNode) {
        const lastDistance = KademliaDHT.distance(this.nodeId, lastNode.id);
        if (distance < lastDistance) {
          bucket.pop();
          bucket.unshift(node);
        }
      }
    }
    
    this.buckets.set(bucketIndex, bucket);
  }

  // Find closest nodes to a target ID
  findClosestNodes(targetId: string, count: number = K): DHTNode[] {
    const distance = KademliaDHT.distance(this.nodeId, targetId);
    const bucketIndex = this.getBucketIndex(distance);
    
    let candidates: DHTNode[] = [];
    
    // Search buckets from closest to furthest
    for (let i = bucketIndex; i >= 0 && candidates.length < count; i--) {
      candidates.push(...(this.buckets.get(i) || []));
    }
    for (let i = bucketIndex + 1; i < 160 && candidates.length < count; i++) {
      candidates.push(...(this.buckets.get(i) || []));
    }
    
    // Sort by distance
    return candidates
      .sort((a, b) => Number(KademliaDHT.distance(a.id, targetId) - KademliaDHT.distance(b.id, targetId)))
      .slice(0, count);
  }

  // Store a value
  async put(key: string, value: Uint8Array, ttl: number = 3600000): Promise<void> {
    const entry: DHTValue = {
      key,
      value,
      version: (this.storage.get(key)?.version ?? 0) + 1,
      timestamp: now(),
      ttl,
    };
    
    this.storage.set(key, entry);
    logger.debug({ key, version: entry.version }, 'DHT put');
  }

  // Get a value
  async get(key: string): Promise<Uint8Array | null> {
    const entry = this.storage.get(key);
    if (!entry) return null;
    
    // Check TTL
    if (now() - entry.timestamp > entry.ttl) {
      this.storage.delete(key);
      return null;
    }
    
    return entry.value;
  }

  // Delete a value
  async delete(key: string): Promise<boolean> {
    return this.storage.delete(key);
  }

  // Get all stored keys (for debugging)
  getStoredKeys(): string[] {
    return Array.from(this.storage.keys());
  }

  // Get routing table stats
  getRoutingTableStats(): { totalNodes: number; bucketsUsed: number } {
    let totalNodes = 0;
    let bucketsUsed = 0;
    
    for (const bucket of this.buckets.values()) {
      if (bucket.length > 0) {
        bucketsUsed++;
        totalNodes += bucket.length;
      }
    }
    
    return { totalNodes, bucketsUsed };
  }

  // Bootstrap from known nodes
  async bootstrap(bootstrapNodes: DHTNode[]): Promise<void> {
    for (const node of bootstrapNodes) {
      this.addNode(node);
    }
    
    // Perform iterative lookup for our own ID to populate routing table
    await this.lookup(this.nodeId);
  }

  // Iterative lookup for a key
  async lookup(key: string): Promise<DHTNode[]> {
    const closest = this.findClosestNodes(key, K);
    
    // In a real implementation, this would contact nodes via RPC
    // For now, we just return local knowledge
    return closest;
  }

  // Periodic bucket refresh
  async refreshBuckets(): Promise<void> {
    // Generate random IDs in each bucket range and lookup
    for (let i = 0; i < 160; i++) {
      const randomId = this.generateRandomIdInBucket(i);
      await this.lookup(randomId);
    }
  }

  private generateRandomIdInBucket(bucketIndex: number): string {
    // Generate an ID that falls in the specified bucket
    const prefix = this.nodeId.slice(0, Math.floor(bucketIndex / 4));
    const remaining = 40 - prefix.length;
    return prefix + createHash('sha256').update(`${Math.random()}`).digest('hex').slice(0, remaining);
  }

  // Cleanup expired entries
  cleanup(): number {
    let removed = 0;
    for (const [key, entry] of this.storage) {
      if (now() - entry.timestamp > entry.ttl) {
        this.storage.delete(key);
        removed++;
      }
    }
    return removed;
  }
}

export const dht = new KademliaDHT();