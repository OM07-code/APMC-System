import { createHash } from 'crypto';

export interface FlatNameRecord {
  key: string;
  value: Uint8Array;
  version: number;
  ttl: number;
  createdAt: number;
}

export interface DHTNode {
  id: string;
  host: string;
  port: number;
  lastSeen: number;
}

export class FlatNamingService {
  private records = new Map<string, FlatNameRecord>();
  private nodes = new Map<string, DHTNode>();
  private readonly k = 20; // Kademlia k-bucket size
  private readonly alpha = 3; // Concurrency factor

  constructor(private readonly localNodeId: string) {}

  // Generate a flat name (hash-based identifier)
  static generateFlatName(data: string | Uint8Array): string {
    const hash = createHash('sha256');
    hash.update(typeof data === 'string' ? data : Buffer.from(data));
    return hash.digest('hex').slice(0, 32);
  }

  // Calculate XOR distance between two IDs
  static xorDistance(id1: string, id2: string): bigint {
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

  // Store a value with a flat name
  async put(key: string, value: Uint8Array, ttl: number = 3600000): Promise<void> {
    const record: FlatNameRecord = {
      key,
      value,
      version: (this.records.get(key)?.version ?? 0) + 1,
      ttl,
      createdAt: Date.now(),
    };
    this.records.set(key, record);
  }

  // Retrieve a value by flat name
  async get(key: string): Promise<Uint8Array | null> {
    const record = this.records.get(key);
    if (!record) return null;
    
    // Check TTL
    if (Date.now() - record.createdAt > record.ttl) {
      this.records.delete(key);
      return null;
    }
    
    return record.value;
  }

  // Delete a flat name record
  async delete(key: string): Promise<boolean> {
    return this.records.delete(key);
  }

  // Find closest nodes to a key (for routing)
  findClosestNodes(key: string, count: number = this.k): DHTNode[] {
    const nodes = Array.from(this.nodes.values())
      .filter(n => n.id !== this.localNodeId)
      .sort((a, b) => 
        Number(FlatNamingService.xorDistance(a.id, key) - FlatNamingService.xorDistance(b.id, key))
      );
    return nodes.slice(0, count);
  }

  // Add/update a node in the routing table
  updateNode(node: DHTNode): void {
    this.nodes.set(node.id, { ...node, lastSeen: Date.now() });
  }

  // Remove a node
  removeNode(nodeId: string): void {
    this.nodes.delete(nodeId);
  }

  // Get local node ID
  getLocalNodeId(): string {
    return this.localNodeId;
  }

  // Get all records (for debugging)
  getAllRecords(): FlatNameRecord[] {
    return Array.from(this.records.values());
  }

  // Cleanup expired records
  cleanup(): number {
    const now = Date.now();
    let removed = 0;
    for (const [key, record] of this.records) {
      if (now - record.createdAt > record.ttl) {
        this.records.delete(key);
        removed++;
      }
    }
    return removed;
  }
}

// Structured naming (DNS-like hierarchical names)
export interface StructuredNameRecord {
  name: string;
  type: 'A' | 'AAAA' | 'CNAME' | 'TXT' | 'SRV';
  value: string;
  ttl: number;
  priority?: number;
  weight?: number;
  port?: number;
}

export class StructuredNamingService {
  private zones = new Map<string, Map<string, StructuredNameRecord[]>>();

  // Parse a structured name (e.g., "auction.jalgaon.apmc.network")
  static parse(name: string): { labels: string[]; zone: string } {
    const labels = name.split('.').filter(Boolean);
    const zone = labels.slice(-2).join('.'); // e.g., "apmc.network"
    return { labels, zone };
  }

  // Add a record to a zone
  addRecord(zone: string, record: StructuredNameRecord): void {
    if (!this.zones.has(zone)) {
      this.zones.set(zone, new Map());
    }
    const zoneRecords = this.zones.get(zone)!;
    const existing = zoneRecords.get(record.name) || [];
    existing.push(record);
    zoneRecords.set(record.name, existing);
  }

  // Remove a record
  removeRecord(zone: string, name: string, type: StructuredNameRecord['type']): boolean {
    const zoneRecords = this.zones.get(zone);
    if (!zoneRecords) return false;
    
    const records = zoneRecords.get(name);
    if (!records) return false;
    
    const filtered = records.filter(r => r.type !== type);
    if (filtered.length === 0) {
      zoneRecords.delete(name);
    } else {
      zoneRecords.set(name, filtered);
    }
    return true;
  }

  // Resolve a name
  async resolve(name: string, type: StructuredNameRecord['type'] = 'A'): Promise<string[]> {
    const { labels, zone } = StructuredNamingService.parse(name);
    const zoneRecords = this.zones.get(zone);
    if (!zoneRecords) return [];

    const records = zoneRecords.get(labels.join('.')) || [];
    return records
      .filter(r => r.type === type)
      .map(r => r.value);
  }

  // Resolve SRV records (for service discovery)
  async resolveSRV(service: string, proto: string, name: string): Promise<Array<{
    priority: number;
    weight: number;
    port: number;
    target: string;
  }>> {
    const srvName = `_${service}._${proto}.${name}`;
    const { zone } = StructuredNamingService.parse(srvName);
    const zoneRecords = this.zones.get(zone);
    if (!zoneRecords) return [];

    const records = zoneRecords.get(srvName) || [];
    return records
      .filter(r => r.type === 'SRV')
      .map(r => ({
        priority: r.priority ?? 0,
        weight: r.weight ?? 0,
        port: r.port ?? 0,
        target: r.value,
      }))
      .sort((a, b) => a.priority - b.priority || b.weight - a.weight);
  }

  // Get all records in a zone
  getZone(zone: string): Map<string, StructuredNameRecord[]> {
    return this.zones.get(zone) || new Map();
  }

  // List all zones
  listZones(): string[] {
    return Array.from(this.zones.keys());
  }
}

// Attribute-based naming (directory service)
export interface AttributeRecord {
  id: string;
  attributes: Record<string, string>;
  metadata: Record<string, unknown>;
  registeredAt: number;
}

export class AttributeNamingService {
  private records = new Map<string, AttributeRecord>();
  private indexes = new Map<string, Map<string, Set<string>>>(); // attribute -> value -> set of ids

  // Register an entity with attributes
  register(attributes: Record<string, string>, metadata: Record<string, unknown> = {}): string {
    const id = `attr-${Date.now()}-${Math.random().toString(36).slice(2, 9)}`;
    const record: AttributeRecord = {
      id,
      attributes,
      metadata,
      registeredAt: Date.now(),
    };
    
    this.records.set(id, record);
    
    // Update indexes
    for (const [key, value] of Object.entries(attributes)) {
      if (!this.indexes.has(key)) {
        this.indexes.set(key, new Map());
      }
      const valueIndex = this.indexes.get(key)!;
      if (!valueIndex.has(value)) {
        valueIndex.set(value, new Set());
      }
      valueIndex.get(value)!.add(id);
    }
    
    return id;
  }

  // Unregister an entity
  unregister(id: string): boolean {
    const record = this.records.get(id);
    if (!record) return false;
    
    // Remove from indexes
    for (const [key, value] of Object.entries(record.attributes)) {
      const valueIndex = this.indexes.get(key);
      if (valueIndex) {
        valueIndex.get(value)?.delete(id);
        if (valueIndex.get(value)?.size === 0) {
          valueIndex.delete(value);
        }
      }
    }
    
    return this.records.delete(id);
  }

  // Query by attributes
  query(attributes: Record<string, string>): AttributeRecord[] {
    let candidateIds: Set<string> | null = null;
    
    for (const [key, value] of Object.entries(attributes)) {
      const valueIndex = this.indexes.get(key)?.get(value);
      if (!valueIndex || valueIndex.size === 0) {
        return [];
      }
      
      if (candidateIds === null) {
        candidateIds = new Set(valueIndex);
      } else {
        // Intersection
        for (const id of candidateIds) {
          if (!valueIndex.has(id)) {
            candidateIds.delete(id);
          }
        }
      }
      
      if (candidateIds.size === 0) {
        return [];
      }
    }
    
    if (!candidateIds) return [];
    
    return Array.from(candidateIds)
      .map(id => this.records.get(id))
      .filter((r): r is AttributeRecord => r !== undefined);
  }

  // Get by ID
  get(id: string): AttributeRecord | undefined {
    return this.records.get(id);
  }

  // Update attributes
  update(id: string, attributes: Record<string, string>): boolean {
    const record = this.records.get(id);
    if (!record) return false;
    
    // Remove old indexes
    for (const [key, value] of Object.entries(record.attributes)) {
      const valueIndex = this.indexes.get(key);
      if (valueIndex) {
        valueIndex.get(value)?.delete(id);
        if (valueIndex.get(value)?.size === 0) {
          valueIndex.delete(value);
        }
      }
    }
    
    // Add new indexes
    record.attributes = { ...record.attributes, ...attributes };
    for (const [key, value] of Object.entries(record.attributes)) {
      if (!this.indexes.has(key)) {
        this.indexes.set(key, new Map());
      }
      const valueIndex = this.indexes.get(key)!;
      if (!valueIndex.has(value)) {
        valueIndex.set(value, new Set());
      }
      valueIndex.get(value)!.add(id);
    }
    
    return true;
  }

  // Get all attributes keys (for schema discovery)
  getAttributeKeys(): string[] {
    return Array.from(this.indexes.keys());
  }

  // Get values for an attribute key
  getAttributeValues(key: string): string[] {
    return Array.from(this.indexes.get(key)?.keys() || []);
  }
}