# Project Context — APMC Decentralized Auction System

## 1. Purpose & academic framing

Academic **Distributed Systems** project. Deliverable **FA-1** must *demonstrate the application of*:

- **Unit 1 — Introduction to DS:** definition/goals/types; architectures; design issues; middleware;
  **distributed multimedia systems**; (self-study) models of distributed computation, virtualization.
- **Unit 2 — Communication:** fundamentals; **RPC**; **message-oriented**; **stream-oriented**;
  **P2P messaging**; **WebRTC**; (self-study) names/identifiers/addresses, **fault tolerance**.

So the code must visibly exercise: RPC (gRPC), MOM (Kafka/Redpanda), stream-oriented media
(WebRTC via signaling + coturn), P2P (Kademlia DHT overlay), naming/discovery, and fault tolerance
(circuit breaker / retry / bulkhead / timeout). The domain: APMC mandi commodity auctions with
real-time price discovery.

**FA-1 vs FA-2 boundary (important — see `decisions.md` D8).** FA-1 is Unit 1 + Unit 2 only.
**Bully leader election, Ricart–Agrawala mutual exclusion, advanced replication/caching, latency
metrics, and Jaeger tracing are FA-2** and are deliberately out of scope. `packages/p2p` contains
election and gossip code that boots, but its correctness was **not** audited — demo `demo:dht`
(the Kademlia overlay) for P2P, and say plainly that election is FA-2 material if asked.

Each FA-1 section has a runnable demo — see **`../scripts/demo/README.md`**.

Source of truth for requirements: **`../_pdf_extract.txt`** (text extract of
"Distributed Systems Project Ideas.pdf").

## 2. Tech stack

- **Monorepo:** Turborepo + npm workspaces (`packages/*`), TypeScript ~5.4, Node ≥20 (runs on v22.14.0).
- **RPC:** `@grpc/grpc-js`, `@grpc/proto-loader` (protos in `proto/*.proto`).
- **MOM:** `kafkajs` → Redpanda (`packages/broker`).
- **WebRTC:** `ws` WebSocket signaling (`packages/signaling`) + coturn STUN/TURN.
- **P2P:** custom Kademlia DHT, Bully leader election, gossip (`packages/p2p`).
- **Gateway:** Fastify (`@fastify/cors|jwt|rate-limit|websocket`).
- **Data:** PostgreSQL (`pg`), Redis (`ioredis`).
- **Fault tolerance:** `packages/fault` (circuit breaker, retry, bulkhead, timeout).
- **Observability:** OpenTelemetry (`sdk-node`, Prometheus + Jaeger exporters), `pino` logs,
  Grafana dashboards, Envoy proxy.
- **Config:** Zod-validated env in `packages/config`.

## 3. Packages

| Package | Role | Notable files |
|---|---|---|
| `auction` | gRPC auction service + Postgres | `src/server.ts`, `src/index.ts`, `src/db/{pool,repository,migrate,seed}.ts` |
| `signaling` | WebRTC signaling (gRPC + WS) | `src/index.ts`, `src/websocket-server.ts`, `src/peer-manager.ts` |
| `p2p` | DHT + Bully + gossip overlay | `src/{dht,leader-election,gossip,grpc-server,index}.ts` |
| `naming` | Service discovery / naming | `src/{index,naming,service-discovery}.ts` |
| `gateway` | Fastify API gateway | `src/index.ts`, `src/routes/{auth,auction,market,health,websocket}.ts` |
| `broker` | Kafka producer/consumer | `src/{producer,consumer,index}.ts` |
| `fault` | Resilience patterns | circuit-breaker / retry / bulkhead / timeout |
| `middleware` | Tracing + logging | `src/tracing.ts`, `src/logger.ts` |
| `config` | Zod env schema | `src/index.ts` |
| `shared` | Types, errors, id/time utils | `AppError`, `generateAuctionId`, `now`, … |
| `proto` | Proto helpers / generated | `proto/*.proto` at repo root |

## 4. Ports (host-run defaults — all verified live)

| Thing | Port | Notes |
|---|---|---|
| Auction gRPC | 50051 | `AUCTION_GRPC_PORT` |
| Signaling gRPC | 50052 | `SIGNALING_GRPC_PORT` |
| Signaling WS | 51052 | `SIGNALING_GRPC_PORT + 1000` |
| P2P gRPC | 50053 | `P2P_GRPC_PORT` |
| Naming gRPC | 50054 | `NAMING_GRPC_PORT` |
| Gateway HTTP | 8000 | `GATEWAY_HTTP_PORT`, now binds `0.0.0.0`. **The gateway's only listener.** |
| ~~Gateway gRPC~~ | ~~8001~~ | `GATEWAY_GRPC_PORT` is declared in the Zod config and published in compose, but **nothing binds it** — the gateway is a gRPC *client* of auction, not a server. Nothing depends on it; see `bug-log.md` G1. |
| Prometheus metrics (per service) | 9090 auction · 9091 signaling · 9093 gateway · 9094 naming · 9095 p2p | `PROMETHEUS_PORT`, set per child by `scripts/dev-services.mjs`. 9092 is skipped — Redpanda publishes it on the host. |
| Postgres / Redis | 5432 / 6379 | infra |
| Redpanda Kafka | **19092 from the host**, 9092 inside Docker | Dual listener. Host processes **must** use `localhost:19092`; `redpanda:9092` only resolves inside the Docker network. |
| coturn STUN/TURN | 3478 tcp+udp, 5349 tcp+udp | credentials `apmc` / `apmcpass`, realm `apmc.local` |
| coturn UDP relay | 49160–49200 (41 ports) | Was `49152-65535` = 16,384 ports, which crashed the PC. |

## 5. Environment facts (verified)

- Node **v22.14.0**, npm **10.9.2**, Docker **29.7.2**, Compose **v5.5.0**.
- **No `.env` file** exists (config falls back to localhost defaults). `.env.example` documents the
  knobs, including the `TURN_*` trio.
- **All 11 packages built** (`dist/` present); `@apmc/*` workspace symlinks intact.
- **`concurrently` is NOT installed** — and is not needed: `scripts/dev-services.mjs` uses plain
  `node:child_process`.
- Present deps: opentelemetry, pino, kafkajs, ioredis, grpc, fastify.
- `JWT_SECRET` default = `development-secret-change-in-production-min-32-chars`.
- Seeded demo users all use password **`password123`** (the seed used to store an invalid bcrypt
  placeholder, so no login worked — fixed, see `bug-log.md` F1).
- In this Git Bash environment **`/tmp` resolves to `E:\tmp`, which does not exist** — don't redirect
  output there.

## 6. Root npm scripts

```
build              turbo run build
dev                turbo run dev
test / lint        turbo run test / lint
proto:generate     turbo run proto:generate
db:migrate         turbo run db:migrate
db:seed            turbo run db:seed

dev:services       node scripts/dev-services.mjs      # ← the launcher: all 5 services, one Ctrl-C
flow:test          node scripts/flow-test.mjs         # end-to-end auction lifecycle over gRPC

docker:up:core     postgres + redis + redpanda        # what most work needs
docker:up:webrtc   ...+ coturn                        # needed for a real TURN relay
docker:up          ⚠️ ALL 13 containers, incl. 5 image builds — NEVER RUN THIS
docker:down        docker-compose down

demo:circuit-breaker · demo:naming · demo:dht · demo:mom · demo:webrtc
demo:offline       the three that need no Docker, back to back
```

## 7. Docker topology

`docker-compose.yml`: postgres:16-alpine, redis:7-alpine, redpanda:v23.3.10
(`--smp=1 --memory=1G`, dual Kafka listener), coturn:4.6.2, jaeger, prometheus, grafana, envoy, + 5
Node services built from `deployments/docker/Dockerfile.all` (multi-stage: base → builder →
prod-deps → 5 runner targets). **Every container now has a `mem_limit`.**

Under the host-launcher-only decision (`decisions.md` D6) the 5 app containers and Envoy are never
started, so `Dockerfile.all` is not built and `envoy.yaml` is not exercised — its duplicate
`clusters:` key was fixed anyway. Only the infra containers are used, via `docker:up:core` /
`docker:up:webrtc` (4 containers ≈ 410 MB measured).
