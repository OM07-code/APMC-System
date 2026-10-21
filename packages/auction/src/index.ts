import { startAuctionServer } from './server';
export { startAuctionServer } from './server';
export { query, getPool, getClient, transaction, closePool, healthCheck } from './db/pool';
export { auctionRepository, AuctionRepository } from './db/repository';
export type { Auction, Bid, CreateAuctionInput, PlaceBidInput } from './db/repository';

import { initTracing } from '@apmc/middleware';
import { logger } from '@apmc/middleware';
import { config } from '@apmc/config';

async function main(): Promise<void> {
  try {
    // Initialize tracing
    await initTracing();
    
    // Start gRPC server
    const server = await startAuctionServer();
    
    // Graceful shutdown
    const shutdown = async (signal: string) => {
      logger.info({ signal }, 'Shutting down...');
      server.tryShutdown(() => {
        logger.info('Auction server stopped');
        process.exit(0);
      });
      
      // Force shutdown after 30 seconds
      setTimeout(() => {
        logger.error('Force shutdown');
        process.exit(1);
      }, 30000);
    };
    
    process.on('SIGTERM', () => shutdown('SIGTERM'));
    process.on('SIGINT', () => shutdown('SIGINT'));
    
    logger.info(`Auction service started on port ${config.AUCTION_GRPC_PORT}`);
  } catch (error) {
    logger.error({ err: error }, 'Failed to start auction service');
    process.exit(1);
  }
}

// Only start the server when this module is run directly (e.g. `node dist/index.js`).
// Guard prevents main() from firing when another package imports @apmc/auction
// for its exports (query, auctionRepository, ...), which would otherwise try to
// bind the gRPC port a second time inside the importing process.
if (require.main === module) {
  main();
}