import { startSignalingServer } from './grpc-server';
import { signalingWsServer } from './websocket-server';
import { initTracing } from '@apmc/middleware';
import { logger } from '@apmc/middleware';
import { config } from '@apmc/config';
import { peerManager } from './peer-manager';

// Public exports so other packages (e.g. the gateway's /health check) can reach
// the signaling singletons. Previously this file had NO exports, so
// `require('@apmc/signaling')` returned {} and `.peerManager` was undefined.
export { peerManager } from './peer-manager';
export { signalingWsServer } from './websocket-server';
export { startSignalingServer } from './grpc-server';

async function main(): Promise<void> {
  try {
    // Initialize tracing
    await initTracing();
    
    // Start gRPC server
    const grpcServer = await startSignalingServer();
    
    // Start WebSocket server
    await signalingWsServer.start(config.SIGNALING_GRPC_PORT + 1000);
    
    // Graceful shutdown
    const shutdown = async (signal: string) => {
      logger.info({ signal }, 'Shutting down signaling service...');
      
      grpcServer.tryShutdown(() => {
        logger.info('Signaling gRPC server stopped');
      });
      
      await signalingWsServer.stop();
      await peerManager.disconnect();
      
      process.exit(0);
    };
    
    process.on('SIGTERM', () => shutdown('SIGTERM'));
    process.on('SIGINT', () => shutdown('SIGINT'));
    
    logger.info(`Signaling service started`);
  } catch (error) {
    logger.error({ err: error }, 'Failed to start signaling service');
    process.exit(1);
  }
}

// Only start when run directly. Guard prevents main() from firing when the gateway
// imports @apmc/signaling (peerManager) for its exports, which would otherwise try
// to bind the signaling gRPC/WS ports a second time inside the gateway process.
if (require.main === module) {
  main();
}