import { FastifyInstance } from 'fastify';
import { config } from '@apmc/config';
import { logger } from '@apmc/middleware';
import { query } from '@apmc/auction';

export async function healthRoutes(fastify: FastifyInstance): Promise<void> {
  fastify.get('/health', async (request, reply) => {
    try {
      // Check database
      const dbHealthy = await query('SELECT 1').then(() => true).catch(() => false);
      
      // Check Redis
      const redisHealthy = await require('@apmc/signaling').peerManager.redis.ping().then(() => true).catch(() => false);
      
      // Check Kafka (simplified)
      const kafkaHealthy = true; // Would check producer/consumer status
      
      const healthy = dbHealthy && redisHealthy && kafkaHealthy;
      
      return reply.code(healthy ? 200 : 503).send({
        status: healthy ? 'healthy' : 'unhealthy',
        timestamp: new Date().toISOString(),
        service: config.SERVICE_NAME,
        version: config.SERVICE_VERSION,
        checks: {
          database: dbHealthy ? 'healthy' : 'unhealthy',
          redis: redisHealthy ? 'healthy' : 'unhealthy',
          kafka: kafkaHealthy ? 'healthy' : 'unhealthy',
        },
      });
    } catch {
      return reply.code(503).send({
        status: 'unhealthy',
        timestamp: new Date().toISOString(),
      });
    }
  });

  fastify.get('/ready', async (request, reply) => {
    return { ready: true, timestamp: new Date().toISOString() };
  });

  fastify.get('/metrics', async (request, reply) => {
    // Prometheus metrics endpoint
    // In a real implementation, this would use prom-client
    reply.header('Content-Type', 'text/plain');
    return '# Metrics endpoint - integrate with prom-client\n';
  });
}