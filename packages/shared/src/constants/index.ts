export const AUCTION_STATUS = {
  PENDING: 'PENDING',
  ACTIVE: 'ACTIVE',
  ENDED: 'ENDED',
  CANCELLED: 'CANCELLED',
} as const;

export type AuctionStatus = (typeof AUCTION_STATUS)[keyof typeof AUCTION_STATUS];

export const PEER_TYPE = {
  AUCTIONEER: 'AUCTIONEER',
  BUYER: 'BUYER',
  FARMER: 'FARMER',
  OBSERVER: 'OBSERVER',
} as const;

export type PeerType = (typeof PEER_TYPE)[keyof typeof PEER_TYPE];

export const SIGNAL_TYPE = {
  OFFER: 'OFFER',
  ANSWER: 'ANSWER',
  ICE_CANDIDATE: 'ICE_CANDIDATE',
  PEER_JOINED: 'PEER_JOINED',
  PEER_LEFT: 'PEER_LEFT',
} as const;

export type SignalType = (typeof SIGNAL_TYPE)[keyof typeof SIGNAL_TYPE];

export const PRICE_SOURCE = {
  AUCTION: 'AUCTION',
  MANUAL: 'MANUAL',
  EXTERNAL_API: 'EXTERNAL_API',
} as const;

export type PriceSource = (typeof PRICE_SOURCE)[keyof typeof PRICE_SOURCE];

export const ALERT_TYPE = {
  PRICE_SPIKE: 'PRICE_SPIKE',
  PRICE_DROP: 'PRICE_DROP',
  NEW_AUCTION: 'NEW_AUCTION',
  AUCTION_ENDED: 'AUCTION_ENDED',
  HIGH_DEMAND: 'HIGH_DEMAND',
  LOW_SUPPLY: 'LOW_SUPPLY',
} as const;

export type AlertType = (typeof ALERT_TYPE)[keyof typeof ALERT_TYPE];

export const EVENT_TYPE = {
  AUCTION_CREATED: 'AUCTION_CREATED',
  AUCTION_STARTED: 'AUCTION_STARTED',
  BID_PLACED: 'BID_PLACED',
  AUCTION_ENDED: 'AUCTION_ENDED',
  AUCTION_CANCELLED: 'AUCTION_CANCELLED',
} as const;

export type EventType = (typeof EVENT_TYPE)[keyof typeof EVENT_TYPE];

export const KAFKA_TOPICS = {
  PRICE_UPDATES: 'apmc.price.updates',
  MARKET_ALERTS: 'apmc.market.alerts',
  AUCTION_EVENTS: 'apmc.auction.events',
  COMMODITY_INDEX: 'apmc.commodity.index',
} as const;

export const GRPC_SERVICES = {
  AUCTION: 'apmc.auction.v1.AuctionService',
  SIGNALING: 'apmc.signaling.v1.SignalingService',
  NAMING: 'apmc.naming.v1.NamingService',
} as const;

export const DEFAULT_PORTS = {
  AUCTION_GRPC: 50051,
  SIGNALING_GRPC: 50052,
  P2P_GRPC: 50053,
  NAMING_GRPC: 50054,
  GATEWAY_HTTP: 8000,
  GATEWAY_GRPC: 8001,
} as const;

export const CIRCUIT_BREAKER = {
  FAILURE_THRESHOLD: 5,
  SUCCESS_THRESHOLD: 2,
  TIMEOUT: 30000,
  RESET_TIMEOUT: 60000,
} as const;

export const RETRY = {
  MAX_ATTEMPTS: 3,
  BASE_DELAY: 1000,
  MAX_DELAY: 10000,
} as const;

export const BULKHEAD = {
  MAX_CONCURRENT: 100,
  QUEUE_SIZE: 50,
} as const;

export const DHT = {
  K: 20,
  ALPHA: 3,
  REPLICATION_FACTOR: 3,
  REFRESH_INTERVAL: 3600000,
} as const;

export const WEbrtc = {
  ICE_SERVERS: [
    { urls: 'stun:stun.l.google.com:19302' },
    { urls: 'stun:stun1.l.google.com:19302' },
  ],
  TURN_SERVER: process.env['TURN_SERVER'] || '',
  TURN_USERNAME: process.env['TURN_USERNAME'] || '',
  TURN_CREDENTIAL: process.env['TURN_CREDENTIAL'] || '',
} as const;