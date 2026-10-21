# APMC Decentralized Auction System

A distributed system implementation for decentralized Agricultural Produce Market Committee (APMC) auction, demonstrating concepts from Distributed Systems.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        API Gateway (Fastify)                    │
│                    Port 8000 (HTTP) / 8001 (gRPC)               │
└─────────────────────────────────────────────────────────────────┘
          │                    │                    │
          ▼                    ▼                    ▼
┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
│  Auction Svc    │  │  Signaling Svc  │  │  P2P Overlay    │
│  Port 50051     │  │  Port 50052     │  │  Port 50053     │
│  (gRPC)         │  │  (gRPC + WS)    │  │  (gRPC)         │
└────────┬────────┘  └────────┬────────┘  └────────┬────────┘
         │                    │                    │
         └────────────────────┼────────────────────┘
                              ▼
              ┌───────────────────────────────┐
              │     Message Broker (Redpanda) │
              │  Port 9092 (Kafka-compatible) │
              └───────────────┬───────────────┘
                              │
              ┌───────────────┴───────────────┐
              ▼                               ▼
      ┌───────────────┐               ┌───────────────┐
      │  PostgreSQL   │               │    Redis      │
      │  Port 5432    │               │  Port 6379    │
      └───────────────┘               └───────────────┘
```

## Technology Stack

| Component | Technology | Purpose |
|-----------|------------|---------|
| Language | TypeScript (Node.js 20) | Unified stack |
| Build | Turborepo | Monorepo management |
| RPC | gRPC + Protocol Buffers | Strongly-typed service communication |
| Message Broker | Redpanda (Kafka-compatible) | Async event streaming |
| Database | PostgreSQL | Persistent storage |
| Cache/PubSub | Redis | Sessions, signaling, caching |
| WebRTC | coturn (STUN/TURN) | NAT traversal for P2P media |
| Service Discovery | Custom (in-memory + Redis) | Naming & discovery |
| Observability | OpenTelemetry + Prometheus + Grafana + Jaeger | Metrics, tracing, logs |
| Containerization | Docker + Docker Compose | Local development |
| Orchestration | Kubernetes manifests | Production deployment |

## Distributed Systems Concepts Covered

### Unit 1: Introduction to Distributed Systems
- **Transparency**: Location, migration, replication, failure transparency via service discovery, container orchestration, read replicas, circuit breakers
- **Scalability**: Size (stateless services), Geographical (multi-region), Administrative (independent services)
- **Architectures**: Layered (middleware), Object-based (gRPC), Event-based (Kafka), P2P (WebRTC/DHT)
- **Middleware**: Shared interceptors for auth, logging, tracing, rate limiting
- **Computation Model**: Async with explicit failure handling (fail-stop, omission, timing, Byzantine)
- **Virtualization**: Docker containers, K8s deployments with HPA

### Unit 2: Communication
- **RPC/gRPC**: Auction service with unary + bidirectional streaming
- **Message-Oriented (MOM)**: Kafka for price updates, market alerts, auction events
- **Stream-Oriented**: WebRTC signaling + P2P media via ICE/STUN/TURN
- **P2P Overlays**: Kademlia DHT for resilient state sync, Bully leader election
- **Naming**: Flat (UUID), Structured (DNS-like), Attribute-based (directory queries)
- **Fault Tolerance**: Circuit breaker, retry, bulkhead, CAP theorem (CP for bids, AP for prices)

## Quick Start

### Prerequisites
- Node.js 20+
- Docker & Docker Compose
- pnpm (recommended) or npm

### Installation

```bash
# Clone and install dependencies
cd apmc-system
npm install

# Build all packages
npm run build

# Generate protobuf types
npm run proto:generate
```

### Development with Docker Compose

```bash
# Start all infrastructure
npm run docker:up

# Run database migrations
npm run db:migrate

# Seed test data
npm run db:seed

# Start services in development mode (in separate terminals)
npm run dev --filter=@apmc/auction
npm run dev --filter=@apmc/signaling
npm run dev --filter=@apmc/p2p
npm run dev --filter=@apmc/naming
npm run dev --filter=@apmc/gateway
```

Or start everything at once:
```bash
npm run dev
```

### Access Points

| Service | URL |
|---------|-----|
| API Gateway (REST) | http://localhost:8000 |
| API Gateway (gRPC) | localhost:8001 |
| Auction gRPC | localhost:50051 |
| Signaling gRPC | localhost:50052 |
| P2P gRPC | localhost:50053 |
| Naming gRPC | localhost:50054 |
| Grafana | http://localhost:3000 (admin/admin) |
| Prometheus | http://localhost:9090 |
| Jaeger | http://localhost:16686 |

### API Examples

#### Create Auction
```bash
curl -X POST http://localhost:8000/api/v1/auctions \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer <token>" \
  -d '{
    "commodity": "COTTON",
    "grade": "A",
    "quantity": 100,
    "base_price": 5000,
    "duration_seconds": 3600,
    "apmc_location": "JLG",
    "farmer_id": "<farmer-uuid>"
  }'
```

#### Place Bid
```bash
curl -X POST http://localhost:8000/api/v1/auctions/<auction-id>/bids \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer <token>" \
  -d '{
    "buyer_id": "<buyer-uuid>",
    "amount": 5500
  }'
```

#### Get Market Prices
```bash
curl http://localhost:8000/api/v1/market/prices?commodity=COTTON&grade=A
```

#### WebSocket for Real-time Updates
```javascript
const ws = new WebSocket('ws://localhost:8000/ws/auctions/<auction-id>');
ws.onmessage = (event) => {
  const data = JSON.parse(event.data);
  console.log('Auction update:', data);
};
```

## Project Structure

```
apmc-system/
├── packages/
│   ├── proto/           # Protocol Buffer definitions
│   ├── shared/          # Common types, errors, utilities
│   ├── config/          # Environment configuration (Zod)
│   ├── middleware/      # gRPC interceptors, auth, logging, tracing
│   ├── fault/           # Circuit breaker, retry, bulkhead, timeout
│   ├── naming/          # Service discovery (flat/structured/attribute)
│   ├── auction/         # Auction domain + gRPC service
│   ├── signaling/       # WebRTC signaling + WebSocket
│   ├── p2p/             # Kademlia DHT, Bully election, Gossip
│   ├── broker/          # Kafka producer/consumer
│   ├── gateway/         # Fastify REST API + WebSocket
│   └── tests/           # Integration & unit tests
├── proto/               # .proto files
├── deployments/
│   ├── docker/          # Dockerfiles, docker-compose
│   └── k8s/             # Kubernetes manifests
├── init-db/             # Database schema & seed data
└── turbo.json           # Turborepo configuration
```

## Key Features

### Auction Service (gRPC)
- Strong consistency for bid placement
- Bidirectional streaming for real-time auction watch
- Optimistic locking for concurrent bids
- Event sourcing for audit trail

### Signaling Service (WebRTC)
- gRPC for structured signaling (offer/answer/ICE)
- WebSocket for browser clients
- STUN/TURN via coturn for NAT traversal
- Peer presence and discovery

### P2P Overlay (Resilience)
- Kademlia DHT for decentralized state storage
- Bully algorithm for leader election at edge
- Gossip protocol for eventual consistency
- Local auction continuation during partition

### Message Broker (Kafka)
- Price updates: `apmc.price.updates`
- Market alerts: `apmc.market.alerts`
- Auction events: `apmc.auction.events`
- Commodity index: `apmc.commodity.index`

### Fault Tolerance
- Circuit breakers per downstream dependency
- Exponential backoff retry with jitter
- Bulkhead isolation for resource protection
- Timeout handling with cancellation

## Testing

```bash
# Run unit tests
npm run test

# Run integration tests (requires running services)
npm run test:integration

# Run with coverage
npm run test -- --coverage
```

## Production Deployment

```bash
# Build production images
docker-compose -f docker-compose.yml -f docker-compose.prod.yml build

# Deploy to Kubernetes
kubectl apply -f deployments/k8s/
```

## Monitoring

- **Metrics**: Prometheus scrapes `/metrics` from all services
- **Traces**: Jaeger collects traces via OpenTelemetry
- **Logs**: Structured JSON logs via Pino
- **Dashboards**: Pre-configured Grafana dashboards for:
  - Request rates, latency, errors
  - Auction throughput
  - WebRTC connection stats
  - Kafka consumer lag
  - Circuit breaker states
  - P2P cluster health

## License

MIT