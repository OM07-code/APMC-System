# APMC System — Claude Code Project Guide

> **Read `.claude/README.md` first.** It is the index for resuming work in a new chat.
> This file is auto-loaded every session; the `.claude/` folder holds the full working knowledge.

## What this project is

A **Decentralized APMC Market Price Discovery and Real-Time Auction System** — an academic
**Distributed Systems** project (Formative Assessment / FA-1). It must *demonstrate* DS concepts
from **Unit 1** (intro, architectures, middleware, distributed multimedia) and **Unit 2**
(communication: RPC, message-oriented, **stream-oriented**, **P2P**, **WebRTC**, naming,
fault tolerance). The concepts being demonstrated matter as much as the app working.

Monorepo: **Turborepo + npm workspaces**, TypeScript, Node ≥20 (runs on v22). Packages in `packages/*`.

## Services (5 app services + infra)

| Service | Package | Port(s) | Transport | DS concept |
|---|---|---|---|---|
| Auction | `packages/auction` | 50051 (gRPC) | gRPC + Protobuf | RPC, CP (strong consistency for bids) |
| Signaling | `packages/signaling` | 50052 gRPC, 51052 WS | gRPC + WebSocket | Stream-oriented / WebRTC signaling |
| P2P | `packages/p2p` | 50053 (gRPC) | gRPC | P2P: Kademlia DHT (FA-1). Bully election + gossip exist but are **FA-2**, unaudited. |
| Naming | `packages/naming` | 50054 (gRPC) | gRPC | Naming / service discovery |
| Gateway | `packages/gateway` | 8000 HTTP | Fastify HTTP + WS | API gateway, JWT, rate-limit |

Infra (Docker): Postgres 16, Redis 7, Redpanda (Kafka/MOM), coturn (STUN/TURN for WebRTC),
Jaeger, Prometheus, Grafana, Envoy. Support libs: `config`, `middleware` (tracing+logging),
`fault` (circuit-breaker/retry/bulkhead/timeout), `broker` (Kafka), `shared`, `proto`.

## Current state: DEMO READY (2026-09-02)

Phase 1 and Phase 2 are implemented and verified live. Build 11/11; `npm run dev:services` brings up
all 5 services (6 listeners: 8000, 50051, 50052, 50053, 50054, 51052; zero errors);
`npm run flow:test` completes the full auction lifecycle; 6 demo scripts cover every FA-1 section
(`scripts/demo/README.md` is the viva script).

## ⚠️ Critical, must-know facts

1. **Never run `npm run docker:up`.** The crash root cause is fixed (coturn's UDP range went from
   16,384 published ports to 41, and every container now has a `mem_limit`), but the full 13-container
   stack — including 5 image builds — has **never been run on this machine**. Use
   `npm run docker:up:core` (postgres+redis+redpanda) or `npm run docker:up:webrtc` (+coturn, needed
   for a real TURN relay). 4 containers measure ≈ 410 MB.
2. **Host processes reach Redpanda at `localhost:19092`**, not `redpanda:9092` (that only resolves
   inside the Docker network). The launcher and demos already default correctly.
3. **If `dev:services` is force-killed, its 5 children survive and hold their ports.** The launcher
   now preflight-checks and prints the exact `taskkill /PID … /F` commands — run those, don't guess.
4. **Machine: 16 GB RAM.** Keep memory caps in mind for every container.
5. **Behavioral rule from the user: EXPLAIN FIRST, then ASK before making ANY code change.**
   Documentation in `.claude/` is pre-approved; source/config edits are NOT — get sign-off. (The
   user's "FIX EVERYTHING" go-ahead covered the agreed Phase 1 + Phase 2 plan only; this rule applies
   again to anything new.)
6. Everything under `packages/*` is built (`dist/` present). `concurrently` is **not installed** and
   is not needed — `scripts/dev-services.mjs` uses plain `child_process`.

## Decisions already made (do not re-litigate)

- **Run mode: host launcher only** (`npm run dev:services`) with infra in Docker. The full-Docker app
  path (`Dockerfile.all` + Envoy) was dropped.
- **coturn / WebRTC and the DHT stay** (FA-1 needs stream-oriented + P2P) — coturn was fixed by
  *shrinking* its port range, never by deleting it.
- **FA-1 only.** Bully leader election, Ricart–Agrawala, advanced replication/caching, latency
  metrics, and Jaeger tracing are **FA-2** and deliberately out of scope. If asked about election,
  say it's FA-2 and demo `npm run demo:dht` instead.
- **Do not touch** `placeBid`'s `SELECT … FOR UPDATE` (correct as written) or restructure the
  money/`parseFloat` handling (would touch the whole bid path for no FA-1 benefit).

## Conventions

- gRPC handlers use proto-loader with `keepCase: true` → **proto fields are snake_case**; response
  objects must use snake_case keys or fields serialize empty.
- Config is validated by Zod in `packages/config/src/index.ts`; `config` is parsed at import time.
- Money is `parseFloat` (precision risk, knowingly left — see the decision above).
- Seeded demo users all use password **`password123`**.
- Demo scripts are `.cjs` so they can `require()` the compiled `packages/*/dist` output directly.
- In this Git Bash environment `/tmp` resolves to `E:\tmp`, which does not exist — don't redirect there.
- `_pdf_extract.txt` at repo root is a scratch extract of the FA PDF (safe to delete later).
