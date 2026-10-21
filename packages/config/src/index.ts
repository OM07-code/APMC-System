import { z } from 'zod';
import * as dotenv from 'dotenv';

dotenv.config();

const envSchema = z.object({
  NODE_ENV: z.enum(['development', 'test', 'production']).default('development'),
  LOG_LEVEL: z.enum(['fatal', 'error', 'warn', 'info', 'debug', 'trace']).default('info'),
  
  // Database
  DATABASE_URL: z.string().url().default('postgresql://apmc:apmc@localhost:5432/apmc'),
  DATABASE_POOL_SIZE: z.coerce.number().default(10),
  
  // Redis
  REDIS_URL: z.string().url().default('redis://localhost:6379'),
  
  // Kafka/Redpanda
  KAFKA_BROKERS: z.string().default('localhost:9092'),
  KAFKA_CLIENT_ID: z.string().default('apmc-system'),
  KAFKA_CONSUMER_GROUP: z.string().default('apmc-consumers'),
  
  // gRPC Services
  AUCTION_GRPC_PORT: z.coerce.number().default(50051),
  SIGNALING_GRPC_PORT: z.coerce.number().default(50052),
  P2P_GRPC_PORT: z.coerce.number().default(50053),
  NAMING_GRPC_PORT: z.coerce.number().default(50054),
  
  // Gateway
  GATEWAY_HTTP_PORT: z.coerce.number().default(8000),
  GATEWAY_GRPC_PORT: z.coerce.number().default(8001),
  
  // Auth
  JWT_SECRET: z.string().min(32).default('development-secret-change-in-production-min-32-chars'),
  JWT_ACCESS_TTL: z.string().default('15m'),
  JWT_REFRESH_TTL: z.string().default('7d'),
  
  // WebRTC
  TURN_SERVER: z.string().optional(),
  TURN_USERNAME: z.string().optional(),
  TURN_CREDENTIAL: z.string().optional(),
  
  // Observability
  OTEL_EXPORTER_OTLP_ENDPOINT: z.string().url().optional(),
  JAEGER_ENDPOINT: z.string().url().optional(),
  PROMETHEUS_PORT: z.coerce.number().default(9090),
  
  // Service Discovery
  SERVICE_NAME: z.string().default('apmc-service'),
  SERVICE_VERSION: z.string().default('1.0.0'),
});

export type Config = z.infer<typeof envSchema>;

let configInstance: Config | null = null;

export function getConfig(): Config {
  if (!configInstance) {
    configInstance = envSchema.parse(process.env);
  }
  return configInstance;
}

export function resetConfig(): void {
  configInstance = null;
}

export const config = getConfig();