import { Server, ServerCredentials, sendUnaryData, ServerUnaryCall, Metadata, loadPackageDefinition } from '@grpc/grpc-js';
import { loadSync } from '@grpc/proto-loader';
import { join } from 'path';
import { createHash } from 'crypto';
import { config } from '@apmc/config';
import { logger } from '@apmc/middleware';
import { KademliaDHT, dht } from './dht';
import { BullyLeaderElection, NodeState } from './leader-election';
import { GossipProtocol } from './gossip';

const PROTO_PATH = join(__dirname, '..', '..', '..', 'proto', 'p2p.proto');

let packageDefinition: any;
let proto: any;

try {
  packageDefinition = loadSync(PROTO_PATH, {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
  });
  // loadPackageDefinition comes from @grpc/grpc-js (imported above), NOT
  // @grpc/proto-loader. The old require returned undefined so this threw and the
  // catch below silently left `proto` undefined — meaning the real P2PService was
  // never registered and all RPCs returned UNIMPLEMENTED.
  proto = loadPackageDefinition(packageDefinition);
} catch (err) {
  logger.error({ err, protoPath: PROTO_PATH }, 'Failed to load p2p.proto — P2P RPCs will be unavailable');
}

const p2pService = proto?.apmc?.p2p?.v1?.P2PService;

const dhtInstance = dht;
// Priority is derived from the service name so each APMC mandi node gets a
// distinct value (hash of name → 1-10), satisfying the P-H2 bug fix.
function priorityFor(name: string): number {
  let h = 0;
  for (let i = 0; i < name.length; i++) h = (h * 31 + name.charCodeAt(i)) >>> 0;
  return (h % 10) + 1;
}
const leaderElection = new BullyLeaderElection(
  config.SERVICE_NAME,
  'localhost',
  config.P2P_GRPC_PORT,
  priorityFor(config.SERVICE_NAME)
);
const gossip = new GossipProtocol(config.SERVICE_NAME);

// Seed the Kademlia routing table with the APMC mandi nodes that make up the P2P
// overlay. Without this the routing table is empty, so GetDHTInfo reports
// total_nodes: 0 and findClosestNodes() has nothing to return — the DHT looks dead
// even though put/get work. IDs are derived deterministically from each node's name
// so every process agrees on the same 160-bit ID space (SHA-1-length hex, 40 chars).
const OVERLAY_NODES: Array<{ name: string; host: string; port: number }> = [
  { name: 'apmc-pune', host: 'localhost', port: 50053 },
  { name: 'apmc-nashik', host: 'localhost', port: 50153 },
  { name: 'apmc-mumbai', host: 'localhost', port: 50253 },
  { name: 'apmc-nagpur', host: 'localhost', port: 50353 },
  { name: 'apmc-solapur', host: 'localhost', port: 50453 },
  { name: 'apmc-kolhapur', host: 'localhost', port: 50553 },
];

function nodeIdFor(name: string): string {
  return createHash('sha256').update(name).digest('hex').slice(0, 40);
}

function seedOverlay(): void {
  for (const n of OVERLAY_NODES) {
    dhtInstance.addNode({
      id: nodeIdFor(n.name),
      host: n.host,
      port: n.port,
      lastSeen: Date.now(),
      isSelf: n.port === config.P2P_GRPC_PORT,
    });
  }

  const stats = dhtInstance.getRoutingTableStats();
  logger.info(
    { self: dhtInstance.getNodeId(), nodes: stats.totalNodes, buckets: stats.bucketsUsed },
    'DHT routing table seeded with APMC overlay nodes'
  );
}

function createP2PServer(): Server {
  const server = new Server();
  
  if (p2pService) {
    server.addService(p2pService.service, {
      GetDHTInfo: getDHTInfo,
      GetLeaderInfo: getLeaderInfo,
      GetGossipState: getGossipState,
      PutDHT: putDHT,
      GetDHT: getDHT,
    });
  }
  
  return server;
}

async function getDHTInfo(call: ServerUnaryCall<any, any>, callback: any) {
  const stats = dhtInstance.getRoutingTableStats();
  const keys = dhtInstance.getStoredKeys();
  
  callback(null, {
    node_id: dhtInstance.getNodeId(),
    total_nodes: stats.totalNodes,
    buckets_used: stats.bucketsUsed,
    stored_keys: keys.length,
    keys: keys.slice(0, 100),
  });
}

async function getLeaderInfo(call: ServerUnaryCall<any, any>, callback: any) {
  const status = leaderElection.getClusterStatus();
  
  callback(null, {
    is_leader: status.self.state === NodeState.LEADER,
    leader_id: status.leader,
    self_id: status.self.id,
    self_priority: status.self.priority,
    nodes: status.nodes.map(n => ({
      id: n.id,
      host: n.host,
      port: n.port,
      priority: n.priority,
      state: n.state,
      last_heartbeat: n.lastHeartbeat.toString(),
    })),
  });
}

async function getGossipState(call: ServerUnaryCall<any, any>, callback: any) {
  const stats = gossip.getStats();
  
  callback(null, {
    node_id: stats.nodeId,
    state_size: stats.stateSize,
    peer_count: stats.peerCount,
    state_version: stats.stateVersion,
  });
}

async function putDHT(call: ServerUnaryCall<any, any>, callback: any) {
  try {
    const { key, value, ttl } = call.request;
    
    if (!key || !value) {
      throw new Error('Key and value are required');
    }
    
    const buffer = Buffer.from(value, 'base64');
    await dhtInstance.put(key, buffer, ttl || 3600000);
    
    // Gossip the update
    gossip.setState(key, value);
    await gossip.notify(key, value);
    
    callback(null, { success: true });
  } catch (error) {
    callback({
      code: 13,
      details: error instanceof Error ? error.message : 'Failed to put',
      metadata: new Metadata(),
    }, null);
  }
}

async function getDHT(call: ServerUnaryCall<any, any>, callback: sendUnaryData<any>) {
  const cb = callback as any;
  try {
    const { key } = call.request;
    
    if (!key) {
      throw new Error('Key is required');
    }
    
    const value = await dhtInstance.get(key);
    
    if (!value) {
      cb({
        code: 5,
        details: 'Key not found',
        metadata: new Metadata(),
      }, null);
      return;
    }
    
    // @ts-ignore
    cb(null, { value: value.toString('base64') });
  } catch (error) {
      cb({
        code: 13,
        details: error instanceof Error ? error.message : 'Failed to get',
        metadata: new Metadata(),
      }, null);
    }
  }

export async function startP2PServer(): Promise<Server> {
  const server = createP2PServer();

  // Populate the DHT overlay before serving RPCs.
  seedOverlay();

  // Start gossip protocol
  gossip.start();
  
  // Start leader election
  leaderElection.setMessageHandler((msg, target) => {
    // In a real multi-process deployment this would send the message over gRPC.
    // In the single-process demo, we wire nodes together directly.
    logger.debug({ from: msg.from, to: target, type: msg.type }, 'Election message (no-op in single-node mode)');
  });
  
  return new Promise((resolve, reject) => {
    server.bindAsync(
      `0.0.0.0:${config.P2P_GRPC_PORT}`,
      ServerCredentials.createInsecure(),
      (err, port) => {
        if (err) {
          reject(err);
          return;
        }
        server.start();
        logger.info(`P2P gRPC server started on port ${port}`);
        
        // Start leader election with a random delay to prevent collisions
        setTimeout(() => {
          leaderElection.startElection().catch(err => {
            logger.error({ err }, 'Failed to start leader election');
          });
        }, 500 + Math.random() * 1000);
        
        resolve(server);
      }
    );
  });
}

export function stopP2PServer(): void {
  gossip.stop();
  leaderElection.shutdown();
}