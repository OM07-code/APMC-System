import Fastify, { FastifyInstance } from 'fastify';
import { config } from '@apmc/config';
import { logger, initTracing } from '@apmc/middleware';
import { auctionRoutes } from './routes/auction';
import { marketRoutes } from './routes/market';
import { healthRoutes } from './routes/health';
import { websocketRoutes } from './routes/websocket';
import { authRoutes, authenticate } from './routes/auth';
import { AppError } from '@apmc/shared';
import { initializeBroker, shutdownBroker } from '@apmc/broker';

async function buildApp(): Promise<FastifyInstance> {
  const app = Fastify({
    logger: false, // We use our own logger
    ajv: { customOptions: { removeAdditional: 'all' } },
  });

  // Register plugins
  await app.register(require('@fastify/cors'), {
    origin: true,
    credentials: true,
  });

  await app.register(require('@fastify/rate-limit'), {
    max: 100,
    timeWindow: '1 minute',
    keyGenerator: (req: any) => req.ip,
  });

  await app.register(require('@fastify/websocket'));
  
  await app.register(require('@fastify/jwt'), {
    secret: config.JWT_SECRET,
  });

  // Add authentication hook
  authenticate(app);

  // Error handler
  app.setErrorHandler((error, request, reply) => {
    logger.error({ err: error, url: request.url }, 'Request error');
    
    if (error instanceof AppError) {
      return reply.code(error.statusCode).send({ error: error.message, code: error.code });
    }
    
    if (error.validation) {
      return reply.code(400).send({ 
        error: 'Validation failed', 
        code: 'VALIDATION_ERROR',
        details: error.validation 
      });
    }
    
    return reply.code(500).send({ error: 'Internal server error', code: 'INTERNAL_ERROR' });
  });

  // Register routes
  await app.register(healthRoutes, { prefix: '' });
  await app.register(authRoutes, { prefix: '' });
  await app.register(auctionRoutes, { prefix: '' });
  await app.register(marketRoutes, { prefix: '' });
  await app.register(websocketRoutes, { prefix: '' });

  // 404 handler
  app.setNotFoundHandler((request, reply) => {
    return reply.code(404).send({ error: 'Not found', code: 'NOT_FOUND' });
  });

  return app;
}

async function main(): Promise<void> {
  try {
    // Initialize tracing
    await initTracing();

    // Connect the Kafka producer so auction/price events actually publish (MOM).
    // Non-fatal: if the broker is unreachable the gateway still serves HTTP; the
    // per-request publish wrappers degrade to a logged warning.
    try {
      await initializeBroker();
      logger.info('Kafka broker initialized');
    } catch (err) {
      logger.warn({ err }, 'Kafka broker init failed — event publishing disabled until it recovers');
    }

    const app = await buildApp();

    // Graceful shutdown
    const shutdown = async (signal: string) => {
      logger.info({ signal }, 'Shutting down gateway...');
      await app.close();
      await shutdownBroker().catch((err) => logger.warn({ err }, 'broker shutdown failed'));
      process.exit(0);
    };
    
    process.on('SIGTERM', () => shutdown('SIGTERM'));
    process.on('SIGINT', () => shutdown('SIGINT'));
    
    // Start HTTP server
    // Bind 0.0.0.0 so the gateway is reachable from other hosts/containers, not just
    // loopback. (Previously '127.0.0.1', which blocked access from Docker/LAN.)
    const address = await app.listen({ port: config.GATEWAY_HTTP_PORT, host: '0.0.0.0' });
    logger.info(`API Gateway started on ${address}`);
    
    // Start gRPC gateway (if needed)
    // In a real implementation, this would start a gRPC-gateway proxy
    
  } catch (error) {
    logger.error({ err: error }, 'Failed to start gateway');
    process.exit(1);
  }
}

// Only start when run directly (`node dist/index.js`).
if (require.main === module) {
  main();
}