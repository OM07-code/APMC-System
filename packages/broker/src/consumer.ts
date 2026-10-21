import { Kafka, Consumer, EachMessagePayload, KafkaConfig } from 'kafkajs';
import { config } from '@apmc/config';
import { logger } from '@apmc/middleware';
import { bulkheadRegistry } from '@apmc/fault';
import { now } from '@apmc/shared';

const KAFKA_TOPICS = {
  PRICE_UPDATES: 'apmc.price.updates',
  MARKET_ALERTS: 'apmc.market.alerts',
  AUCTION_EVENTS: 'apmc.auction.events',
  COMMODITY_INDEX: 'apmc.commodity.index',
} as const;

export interface PriceUpdateMessage {
  auction_id: string;
  commodity: string;
  grade: string;
  price: number;
  timestamp: number;
  apmc_location: string;
  currency: string;
  source: number;
}

export interface MarketAlertMessage {
  alert_id: string;
  type: number;
  commodity: string;
  message: string;
  timestamp: number;
  metadata: Record<string, string>;
}

export interface AuctionEventMessage {
  event_id: string;
  auction_id: string;
  type: number;
  timestamp: number;
  data: Record<string, string>;
}

export interface CommodityIndexMessage {
  commodity: string;
  grade: string;
  average_price: number;
  min_price: number;
  max_price: number;
  volume: number;
  timestamp: number;
  regional_prices: Array<{
    apmc_location: string;
    price: number;
    volume: number;
  }>;
}

export type MessageHandler<T> = (message: T, payload: EachMessagePayload) => Promise<void>;

export class BrokerConsumer {
  private consumer: Consumer | null = null;
  private handlers = new Map<string, MessageHandler<any>[]>();
  private running = false;
  private bulkhead = bulkheadRegistry.getOrCreate('kafka-consumer', {
    maxConcurrent: 100,
    queueSize: 1000,
  });

  constructor(private readonly groupId: string = config.KAFKA_CONSUMER_GROUP) {}

  async initialize(kafkaConfig?: Partial<KafkaConfig>): Promise<void> {
    if (this.consumer) return;

    const kafka = new Kafka({
      clientId: `${config.KAFKA_CLIENT_ID}-consumer`,
      brokers: config.KAFKA_BROKERS.split(','),
      retry: {
        initialRetryTime: 100,
        retries: 3,
      },
      connectionTimeout: 5000,
      requestTimeout: 30000,
      ...kafkaConfig,
    });

    this.consumer = kafka.consumer({
      groupId: this.groupId,
      maxWaitTimeInMs: 100,
      maxBytes: 10485760, // 10MB
      minBytes: 1,
      sessionTimeout: 30000,
      heartbeatInterval: 3000,
    });

    await this.consumer.connect();
    
    // Subscribe to all topics
    for (const topic of Object.values(KAFKA_TOPICS)) {
      await this.consumer.subscribe({ topic, fromBeginning: false });
    }

    // Register default handlers
    this.registerDefaultHandlers();

    this.running = true;
    logger.info({ groupId: this.groupId }, 'Kafka consumer initialized');
  }

  private registerDefaultHandlers(): void {
    // Price updates handler - forward to WebSocket clients via Redis pub/sub
    this.on(KAFKA_TOPICS.PRICE_UPDATES, async (message: PriceUpdateMessage) => {
      // In a real implementation, this would publish to Redis for WebSocket servers
      logger.debug({ auction_id: message.auction_id }, 'Processing price update');
    });

    // Market alerts handler
    this.on(KAFKA_TOPICS.MARKET_ALERTS, async (message: MarketAlertMessage) => {
      logger.info({ alert_id: message.alert_id }, 'Market alert received');
    });

    // Auction events handler
    this.on(KAFKA_TOPICS.AUCTION_EVENTS, async (message: AuctionEventMessage) => {
      logger.debug({ event_id: message.event_id }, 'Auction event received');
    });

    // Commodity index handler
    this.on(KAFKA_TOPICS.COMMODITY_INDEX, async (message: CommodityIndexMessage) => {
      logger.debug({ commodity: message.commodity }, 'Commodity index received');
    });
  }

  on<T>(topic: string, handler: MessageHandler<T>): void {
    const handlers = this.handlers.get(topic) || [];
    handlers.push(handler);
    this.handlers.set(topic, handlers);
  }

  off(topic: string, handler: MessageHandler<any>): void {
    const handlers = this.handlers.get(topic);
    if (handlers) {
      const index = handlers.indexOf(handler);
      if (index >= 0) handlers.splice(index, 1);
    }
  }

  async start(): Promise<void> {
    if (!this.consumer) {
      throw new Error('Consumer not initialized');
    }

    await this.consumer.run({
      eachMessage: async (payload: EachMessagePayload) => {
        const { topic, partition, message } = payload;
        
        if (!message.value) return;
        
        const handlers = this.handlers.get(topic);
        if (!handlers || handlers.length === 0) return;

        // Process with bulkhead for backpressure.
        //
        // At-least-once delivery: kafkajs commits the offset only when this callback
        // resolves. We split two failure modes deliberately:
        //   • Unparseable JSON = a "poison" message that can NEVER succeed. Log and
        //     return so the offset commits and it doesn't wedge the partition forever.
        //     A production system would route it to a dead-letter topic here.
        //   • Handler failure = likely transient. Rethrow (don't swallow) so the offset
        //     is NOT committed and Kafka redelivers on the next poll — no silent loss.
        await this.bulkhead.execute(async () => {
          let parsed: any;
          try {
            parsed = JSON.parse((message.value as Buffer).toString());
          } catch (error) {
            logger.error({ err: error, topic, partition, offset: message.offset }, 'Skipping unparseable (poison) message');
            return;
          }

          for (const handler of handlers) {
            await handler(parsed, payload);
          }
        });
      },
    });

    logger.info('Kafka consumer started');
  }

  async stop(): Promise<void> {
    this.running = false;
    if (this.consumer) {
      await this.consumer.disconnect();
      this.consumer = null;
      logger.info('Kafka consumer stopped');
    }
  }

  isRunning(): boolean {
    return this.running;
  }

  getLag(): Promise<Record<string, number>> {
    // In a real implementation, this would query consumer group lag
    return Promise.resolve({});
  }
}

export const brokerConsumer = new BrokerConsumer();

export async function initializeConsumer(groupId?: string): Promise<BrokerConsumer> {
  const consumer = new BrokerConsumer(groupId);
  await consumer.initialize();
  await consumer.start();
  return consumer;
}

export async function shutdownConsumer(consumer: BrokerConsumer): Promise<void> {
  await consumer.stop();
}