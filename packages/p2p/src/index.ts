import { startP2PServer, stopP2PServer } from './grpc-server';
import { initTracing } from '@apmc/middleware';
import { logger } from '@apmc/middleware';
import { config } from '@apmc/config';

// Re-export the Kademlia DHT so demos/tooling can drive the P2P overlay directly
// (see scripts/demo/dht-overlay.cjs) without booting the gRPC server.
export { KademliaDHT, dht } from './dht';
export type { DHTNode, DHTValue } from './dht';

// Unit III – Synchronization algorithms (all pure-logic, no gRPC server needed)
export {
  // §1: Clock Synchronization
  APMCReferenceClockServer, ClockManager, cristianSync, berkeleySync,
  // §2: Logical Clocks
  LamportClock, VectorClock, LogicalClockManager,
  // §3: Global State
  MandiSnapshotNode, GlobalSnapshotCoordinator,
  // §4: Beacon Protocol
  BeaconManager,
  // §5: Mutual Exclusion
  RicartAgrawalaMutex, LodhaKshemkalyaniMutex,
  // §6: Deadlock Detection
  LocalWFG, PathPushingDetector, EdgeChasingDetector,
  DiffusingComputationDetector, GlobalStateDeadlockDetector, KnappCategories,
} from './sync';

// §4: Election Algorithms (BullyLeaderElection is also used by grpc-server)
export { BullyLeaderElection, RingElection, NodeState } from './leader-election';
export type { ClusterNode, ElectionMessage, RingElectionMessage } from './leader-election';

async function main(): Promise<void> {
  try {
    // Initialize tracing
    await initTracing();
    
    // Start gRPC server
    const server = await startP2PServer();
    
    // Graceful shutdown
    const shutdown = async (signal: string) => {
      logger.info({ signal }, 'Shutting down P2P service...');
      stopP2PServer();
      server.tryShutdown(() => {
        logger.info('P2P server stopped');
        process.exit(0);
      });
      
      setTimeout(() => {
        logger.error('Force shutdown');
        process.exit(1);
      }, 30000);
    };
    
    process.on('SIGTERM', () => shutdown('SIGTERM'));
    process.on('SIGINT', () => shutdown('SIGINT'));
    
    logger.info(`P2P service started on port ${config.P2P_GRPC_PORT}`);
  } catch (error) {
    logger.error({ err: error }, 'Failed to start P2P service');
    process.exit(1);
  }
}

// Only start when run directly, so importing @apmc/p2p for its exports never
// boots a second gRPC server inside the importing process.
if (require.main === module) {
  main();
}