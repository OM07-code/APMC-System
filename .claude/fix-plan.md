# Fix Plan — Phase 1 + Phase 2

Status: **BOTH PHASES IMPLEMENTED AND VERIFIED (2026-09-02).**
This file is now the *record* of what was planned and what actually happened, so a new chat can
tell a plan item from an unfinished one. Per-finding detail lives in `bug-log.md`.

Scope decisions that shaped this plan (see `decisions.md`):

- Run mode: **host launcher only** (`npm run dev:services`). The full-Docker app path was dropped.
- coturn / WebRTC and the P2P/DHT overlay **stay** — FA-1 requires stream-oriented + P2P. coturn was
  fixed by *shrinking* its port range, never by removing it.
- Phase 2 = **Option A ("harden + WebRTC"), covering every FA-1 point.** FA-2 topics are excluded.

---

## Phase 1 — Stop the crash + one command to run everything

### Goal A — Make Docker safe on a 16 GB machine

| # | Item | Outcome |
|---|---|---|
| A1 | **Shrink coturn UDP range** — `49152-65535` (16,384 published UDP ports) was the crash source: Docker Desktop spawns a userland proxy per published port. | ✅ **DONE** — `49160-49200/udp` (41 ports) plus matching `--min-port`/`--max-port`. coturn starts in ~2 s at 26 MB. |
| A2 | **Memory caps on every container** | ✅ **DONE & MEASURED** — `mem_limit` on all 13. Live: coturn 26/128 MB, redpanda 333 MB/1.17 GB, postgres 38/512 MB, redis 9/256 MB ≈ **410 MB** for the 4 needed for demos. |
| A3 | **Reduce the first-run build stampede** | ✅ **DONE differently** — instead of ordering the 5 image builds, `docker:up:core` (postgres+redis+redpanda) and `docker:up:webrtc` (+coturn) bring up *infra only*. Under host-launcher-only, the app images are never built, so the stampede is gone rather than sequenced. `docker:up` itself is left intact but **unused/untested** — see the warning in `README.md`. |
| A4 | **Envoy duplicate `clusters:` key** | ✅ **DONE** — merged to one block. Not exercised (Envoy only runs in the dropped Docker path). |

### Goal B — One command for all 5 services

| # | Item | Outcome |
|---|---|---|
| B1 | **naming had no entrypoint** (index.ts only re-exported; no `start` script) | ✅ **DONE** — `main()` + graceful shutdown; listens on 50054. |
| B2 | **Prometheus exporter on 9090 for every service** → 4 of 5 died `EADDRINUSE` | ✅ **DONE** — launcher assigns a distinct `PROMETHEUS_PORT` per child (naming 9094, auction 9090, signaling 9091, p2p 9095, gateway 9093). 9092 avoided because Redpanda publishes it on the host. |
| B3 | **gateway bound `127.0.0.1`** | ✅ **DONE** — binds `0.0.0.0:8000` (confirmed via `netstat`). |
| B4 | **Launcher** | ✅ **DONE without adding a dependency** — `concurrently` was never installed; `scripts/dev-services.mjs` uses plain `child_process` (staggered start, per-child env, colored prefixes, one Ctrl-C tears down all 5, a child dying non-zero tears down the rest). Later hardened with the orphan-port preflight (bug S7). |
| B5 | **`.env.example`** | ✅ **DONE** — all knobs documented, including the `TURN_*` trio (whose credential was wrong and is now corrected to match coturn). |
| B-TRACE | **Exporters never attached to `NodeSDK`** (`spanProcessor: … ? undefined : undefined`) | ⚠️ **PARTIAL** — `metricReader: prometheusExporter` is wired, so **metrics export works**. No trace `spanProcessor` is attached, so **spans never reach Jaeger**. Left deliberately: Jaeger tracing is FA-2. |

---

## Phase 2 — Option A: FA-1 correctness + full FA coverage

Two halves: make the DS mechanisms *actually work* (they were demoed-by-README, not by behaviour),
then give every FA-1 section a runnable demo.

### B.1 Correctness fixes (12 bugs — full detail in `bug-log.md` §C)

The ones that were outright broken, not merely rough:

- **Seeded logins were impossible** — the seed stored the literal string `'$2b$10$testhash'`, so
  `bcrypt.compare()` could never succeed for any demo user.
- **WebRTC signaling was 100 % non-functional** — `handleMessage`'s return value was discarded, so
  `currentPeer` stayed `null` after `register` and every subsequent offer/answer/ICE was rejected
  with "Not registered". A second bug routed signals to a Redis channel with no subscriber.
- **Any caller could act as any user** — the gateway read `farmer_id`/`buyer_id`/`auctioneer_id` from
  the request **body** instead of the verified JWT, and no role was enforced.
- **Kafka consumer silently lost messages** — handler errors were swallowed, so kafkajs committed
  offsets for work that had failed.
- **The DHT looked dead** — the Kademlia routing table was never seeded, so `total_nodes: 0`.
- **`require('@apmc/naming')` hung forever** — a module-scope reaper `setInterval` held the loop open.
- **TURN was never advertised** — `getICEServers()` returned STUN only, because nothing set
  `TURN_SERVER`; FA-1 §3.3 needs both.
- **coturn was permanently `unhealthy`** — its healthcheck ran `turnadmin -k` without `-p`, which
  exits 255. Signaling depends on `service_healthy`, so Docker-mode signaling could never start.

### B.2 FA-1 demo coverage

Six commands, each driving the real `packages/*` code (no mocks). Mapping and the suggested viva
order live in **`scripts/demo/README.md`** — that is the script to follow on the day.

| FA section | Demo |
|---|---|
| §3.1 RPC | `npm run flow:test` — full auction lifecycle over gRPC through the gateway |
| §3.2 MOM | `npm run demo:mom` — pub/sub, durable log, replay from offset 0, per-group offsets, ordering, real at-least-once redelivery |
| §3.3 Stream-oriented | `npm run demo:webrtc` — SDP offer/answer, ICE host/srflx/relay, STUN **and** TURN, presence, offline queueing |
| §3.4 P2P | `npm run demo:dht` — 6-node Kademlia overlay, XOR routing, k-buckets, put/get, TTL |
| §4.1 Naming | `npm run demo:naming` — flat, structured (A/TXT/SRV + delegation), attribute-based |
| §4.2 Fault tolerance | `npm run demo:circuit-breaker` — CLOSED→OPEN→HALF_OPEN→CLOSED, CAP/BASE |

Unit 1 (architectures, middleware, IDL, failure models) is shown by the artifacts themselves:
the `.proto` files are the IDL, `packages/middleware` is the middleware layer, the 5-service split
is the distributed architecture.

---

## What was deliberately NOT done

- **Full-Docker app mode** (`Dockerfile.all`, Envoy, the 5 app containers) — superseded by the
  host-launcher-only decision. The compose entries remain but are untested on this machine.
- **FA-2 material** — Bully leader election, Ricart–Agrawala mutual exclusion, advanced
  replication/caching, latency metrics, Jaeger span export. Listed as open in `bug-log.md` §D.
- **Money precision** — still `parseFloat`. Changing it touches the whole bid path; out of scope by
  decision, recorded as M1.
- **`placeBid`'s `SELECT … FOR UPDATE`** — correct as written, explicitly left alone.

## If you pick this up fresh

Everything above is built and green. `npm run build` → 11/11. Start infra with
`docker:up:webrtc`, then `dev:services`, then run the demos. The two operational traps are the
untested `docker:up` and orphaned children after a force-kill — both covered in `.claude/README.md`.
