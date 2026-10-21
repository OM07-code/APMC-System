import { Kafka, Producer, ProducerRecord, CompressionTypes } from 'kafkajs';
import { config } from '@apmc/config';
import { logger } from '@apmc/middleware';
import { circuitBreakerRegistry, withRetry, retryableErrors } from '@apmc/fault';
import { now } from '@apmc/shared';

const KAFKA_TOPICS = {
  PRICE_UPDATES: 'apmc.price.updates',
  MARKET_ALERTS: 'apmc.market.alerts',
  AUCTION_EVENTS: 'apmc.auction.events',
  COMMODITY_INDEX: 'apmc.commodity.index',
} as const;

interface PriceUpdateMessage {
  auction_id: string;
  commodity: string;
  grade: string;
  price: number;
  timestamp: number;
  apmc_location: string;
  currency: string;
  source: number;
}

interface MarketAlertMessage {
  alert_id: string;
  type: number;
  commodity: string;
  message: string;
  timestamp: number;
  metadata: Record<string, string>;
}

interface AuctionEventMessage {
  event_id: string;
  auction_id: string;
  type: number;
  timestamp: number;
  data: Record<string, string>;
}

interface CommodityIndexMessage {
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

class BrokerProducer {
  private producer: Producer | null = null;
  private initialized = false;
  private circuitBreaker = circuitBreakerRegistry.getOrCreate('kafka-producer', {
    failureThreshold: 5,
    resetTimeout: 30000,
  });

  async initialize(): Promise<void> {
    if (this.initialized) return;

    const kafka = new Kafka({
      clientId: config.KAFKA_CLIENT_ID,
      brokers: config.KAFKA_BROKERS.split(','),
      retry: {
        initialRetryTime: 100,
        retries: 3,
      },
      connectionTimeout: 5000,
      requestTimeout: 30000,
    });

    this.producer = kafka.producer({
      transactionTimeout: 30000,
      maxInFlightRequests: 1,
    });

    await this.producer.connect();
    this.initialized = true;
    logger.info('Kafka producer connected');
  }

  async disconnect(): Promise<void> {
    if (this.producer) {
      await this.producer.disconnect();
      this.initialized = false;
      logger.info('Kafka producer disconnected');
    }
  }

  private async send<T>(topic: string, messages: Array<{ key?: string; value: T }>): Promise<void> {
    if (!this.producer) {
      throw new Error('Producer not initialized');
    }

    await this.circuitBreaker.execute(async () => {
      const result = await withRetry(
        async () => {
          await this.producer!.send({
            topic,
            compression: CompressionTypes.GZIP,
            messages: messages.map(m => ({
              key: m.key,
              value: JSON.stringify(m.value),
              timestamp: now().toString(),
            })),
          });
        },
        {
          maxAttempts: 3,
          baseDelay: 500,
          maxDelay: 5000,
          retryable: retryableErrors,
        }
      );
      
      if (!result.success) {
        throw result.error || new Error('Failed to send message');
      }
    });
  }

  async publishPriceUpdate(message: PriceUpdateMessage): Promise<void> {
    await this.send(KAFKA_TOPICS.PRICE_UPDATES, [
      { key: message.auction_id, value: message },
    ]);
    logger.debug({ auction_id: message.auction_id, price: message.price }, 'Price update published');
  }

  async publishMarketAlert(message: MarketAlertMessage): Promise<void> {
    await this.send(KAFKA_TOPICS.MARKET_ALERTS, [
      { key: message.alert_id, value: message },
    ]);
    logger.debug({ alert_id: message.alert_id, type: message.type }, 'Market alert published');
  }

  async publishAuctionEvent(message: AuctionEventMessage): Promise<void> {
    await this.send(KAFKA_TOPICS.AUCTION_EVENTS, [
      { key: message.auction_id, value: message },
    ]);
    logger.debug({ event_id: message.event_id, auction_id: message.auction_id }, 'Auction event published');
  }

  async publishCommodityIndex(message: CommodityIndexMessage): Promise<void> {
    await this.send(KAFKA_TOPICS.COMMODITY_INDEX, [
      { key: message.commodity, value: message },
    ]);
    logger.debug({ commodity: message.commodity }, 'Commodity index published');
  }

  async publishBatchPriceUpdates(messages: PriceUpdateMessage[]): Promise<void> {
    await this.send(KAFKA_TOPICS.PRICE_UPDATES, 
      messages.map(m => ({ key: m.auction_id, value: m }))
    );
    logger.info({ count: messages.length }, 'Batch price updates published');
  }
}

export const brokerProducer = new BrokerProducer();

// Convenience functions
export async function publishPriceUpdate(message: PriceUpdateMessage): Promise<void> {
  await brokerProducer.publishPriceUpdate(message);
}

export async function publishMarketAlert(message: MarketAlertMessage): Promise<void> {
  await brokerProducer.publishMarketAlert(message);
}

export async function publishAuctionEvent(message: AuctionEventMessage): Promise<void> {
  await brokerProducer.publishAuctionEvent(message);
}

export async function publishCommodityIndex(message: CommodityIndexMessage): Promise<void> {
  await brokerProducer.publishCommodityIndex(message);
}

export async function initializeBroker(): Promise<void> {
  await brokerProducer.initialize();
}

export async function shutdownBroker(): Promise<void> {
  await brokerProducer.disconnect();
}