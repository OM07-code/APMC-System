export * from './service-discovery';
export * from './naming';
export { startNamingServer } from './grpc-server';

import { startNamingServer } from './grpc-server';
import { initTracing } from '@apmc/middleware';
import { logger } from '@apmc/middleware';
import { config } from '@apmc/config';

async function main(): Promise<void> {
  try {
    // Initialize tracing
    await initTracing();

    // Start gRPC server
    const server = await startNamingServer();

    // Graceful shutdown
    const shutdown = async (signal: string) => {
      logger.info({ signal }, 'Shutting down naming service...');
      server.tryShutdown(() => {
        logger.info('Naming server stopped');
        process.exit(0);
      });

      setTimeout(() => {
        logger.error('Force shutdown');
        process.exit(1);
      }, 30000);
    };

    process.on('SIGTERM', () => shutdown('SIGTERM'));
    process.on('SIGINT', () => shutdown('SIGINT'));

    logger.info(`Naming service started on port ${config.NAMING_GRPC_PORT}`);
  } catch (error) {
    logger.error({ err: error }, 'Failed to start naming service');
    process.exit(1);
  }
}

// Only start when run directly (`node dist/index.js`), not when imported for exports.
if (require.main === module) {
  main();
}
