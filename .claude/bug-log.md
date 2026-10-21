# Bug Log — APMC System

Severity: 🔴 blocker/crash · 🟠 high · 🟡 medium · ⚪ low.
Status legend: **✅ FIXED & VERIFIED** · **✅ FIXED** (code changed, not exercised live) ·
**⛔ OUT OF SCOPE** (FA-2 or superseded by the host-launcher-only decision) · **❌ OPEN**.

Last verified: **2026-09-02**, against a live stack (4 containers + 5 host services).

---

## A. Crash & load (the `npm run docker:up` freeze)

| ID | Sev | File | Problem | Status |
|---|---|---|---|---|
| C1 | 🔴 | `docker-compose.yml:81,82,92` | coturn mapped **16,384 UDP ports** `49152-65535` → Docker Desktop spawns a userland proxy per port → handle/RAM exhaustion → PC freeze | **✅ FIXED & VERIFIED** — shrunk to `49160-49200` (41 ports), with matching `--min-port`/`--max-port`. coturn starts in ~2 s at 26 MB. |
| C2 | 🟠 | `docker-compose.yml` | 13 containers with **no memory limits** on 16 GB → swap storm | **✅ FIXED & VERIFIED** — `mem_limit` on every service. Measured: coturn 26/128 MB, redpanda 333 MB/1.17 GB, postgres 38/512 MB, redis 9/256 MB ≈ **410 MB total**. |
| C3 | 🟠 | `Dockerfile.all` | First run builds **5 heavy images at once** | **⛔ OUT OF SCOPE** — run mode is host-launcher-only by decision, so the app images are never built. `docker:up:core` / `docker:up:webrtc` pull infra images only. |
| C4 | 🟡 | `docker-compose.yml:94` | coturn healthcheck `turnadmin -k -u apmc -r apmc.local` **omits `-p`** → exits 255 "the password must be set" → container permanently `unhealthy`. Signaling declares `coturn: condition: service_healthy`, so **in Docker mode signaling could never start**. | **✅ FIXED & VERIFIED** — added `-p apmcpass`. Command now exits 0 and prints the HMAC key; container reports **healthy**. |

## B. Startup blockers (why all 5 couldn't co-run on host)

| ID | Sev | File | Problem | Status |
|---|---|---|---|---|
| S1 | 🔴 | `packages/naming/src/index.ts` | Only re-exports; **no `main()`** → naming never boots | **✅ FIXED & VERIFIED** — naming listens on `50054`. |
| S2 | 🔴 | `packages/middleware/src/tracing.ts` | Prometheus exporter on **9090 for every service** → 4/5 die `EADDRINUSE` | **✅ FIXED & VERIFIED** — `dev-services.mjs` assigns a distinct `PROMETHEUS_PORT` per service (9090/9091/9093/9094/9095). Gateway logs "metrics server started on port 9093". |
| S3 | 🔴 | `packages/gateway/src/index.ts` | Binds `127.0.0.1` | **✅ FIXED & VERIFIED** — `netstat` shows `0.0.0.0:8000`. |
| S4 | 🟡 | `packages/middleware/src/tracing.ts:46` | Exporters never wired into `NodeSDK` | **PARTIAL** — `metricReader: prometheusExporter` is now wired, so **metrics export**. A trace `spanProcessor` is still not attached, so **spans are not exported to Jaeger**. ⛔ Jaeger/tracing is FA-2, deliberately left. |
| S5 | 🟠 | `deployments/docker/envoy.yaml` | **Duplicate top-level `clusters:`** → YAML keeps the last, main clusters dropped | **✅ FIXED** — one `clusters:` block remains. Not exercised (Envoy only runs in Docker mode). |
| S6 | 🟠 | root `package.json` | No one-command launcher; `concurrently` not installed | **✅ FIXED & VERIFIED** — `npm run dev:services` (`scripts/dev-services.mjs`, zero deps, pure `child_process`). |
| S7 | 🟡 | `scripts/dev-services.mjs` | If the launcher is **force-killed**, its SIGTERM handler never runs and the 5 children survive holding their ports. The next run died with a raw gRPC `EADDRINUSE` stack trace that said nothing actionable. | **✅ FIXED & VERIFIED** — added a preflight port scan that lists each busy port + holding PID and prints the exact `taskkill` / `kill -9` commands, then exits 1. |

---

## C. FA-1 correctness fixes (Phase 2) — all verified live

| ID | Sev | File | Problem | Status |
|---|---|---|---|---|
| F1 | 🔴 | `packages/auction/src/db/seed.ts` | Seeded users had `'$2b$10$testhash'` — **not a valid bcrypt digest**, so `bcrypt.compare()` failed and **every seeded login was impossible** | **✅ FIXED & VERIFIED** — real cost-10 hash of `password123` embedded (no new dependency); all INSERTs use `ON CONFLICT (email) DO UPDATE SET password_hash` so re-seeding repairs old rows. Confirmed against the live DB: `compareSync('password123', hash) === true`. |
| F2 | 🔴 | `packages/signaling/src/websocket-server.ts` | `handleMessage`'s return value was **discarded**, so `currentPeer` stayed `null` after `register` → **every offer/answer/ice returned "Not registered"**. WebRTC signaling was completely non-functional. | **✅ FIXED & VERIFIED** — `currentPeer = await this.handleMessage(...)`. Full SDP offer/answer + 3 ICE candidates now relay in both directions. |
| F3 | 🟠 | `packages/signaling/src/websocket-server.ts` | Signals were routed via `peerManager.deliverSignal`, which publishes to a Redis channel **nobody subscribes to** on the WS path → silent drop | **✅ FIXED & VERIFIED** — new `deliverToPeer()` pushes to the target's live socket on this instance; on miss the signal is queued in Redis and drained on (re)register. Demo shows "Target peer offline, signal queued". |
| F4 | 🟠 | `packages/signaling/src/peer-manager.ts` | `updatePeerHeartbeat` gated on `redis.hset`'s return, which is the count of **newly created** fields → `0` for an existing peer → the TTL refresh was skipped → **peers expired despite active heartbeats** | **✅ FIXED & VERIFIED** — gate on `redis.exists`, then update and `expire`. |
| F5 | 🟠 | `packages/gateway/src/routes/auction.ts` | Identity was read from the **request body** (`farmer_id`/`buyer_id`/`auctioneer_id`) → any caller could act as any user, and any role could perform any action | **✅ FIXED & VERIFIED** — `requireRole()` sources identity from the verified JWT claims only; body fields ignored. **12/12 assertions pass**: 403 on every wrong-role action, spoofed `farmer_id` ignored (auction owned by the real farmer), 401 unauthenticated. |
| F6 | 🟠 | `packages/broker/src/consumer.ts` | Handler errors were **swallowed** inside the bulkhead → kafkajs committed the offset → **silent message loss** | **✅ FIXED & VERIFIED** — unparseable JSON is logged and skipped (poison message, can never succeed); handler failures **rethrow** so the offset is not committed. `demo:mom` shows a real redelivery of the same offset after a throw. |
| F7 | 🟠 | `packages/p2p/src/grpc-server.ts` | Kademlia routing table was **never seeded** → `GetDHTInfo` reported `total_nodes: 0` and `findClosestNodes()` had nothing to return — the DHT looked dead | **✅ FIXED & VERIFIED** — `seedOverlay()` adds the 6 APMC mandi nodes with deterministic IDs. Live gRPC now returns `total_nodes: 6, buckets_used: 4`. |
| F8 | 🟠 | `packages/naming/src/service-discovery.ts` | The reaper `setInterval` in a **module-scope** `InMemoryNamingService` kept the event loop alive → merely `require`-ing `@apmc/naming` hung the process forever | **✅ FIXED & VERIFIED** — `heartbeatInterval.unref()`. Servers still stay alive via their listening socket. |
| F9 | 🟡 | `packages/gateway/src/routes/auction.ts` | HTTP bid path did **no** numeric coercion/validation (the gRPC path did) → `"2500"` or `NaN` could enter the bid comparison | **✅ FIXED & VERIFIED** — `Number(amount)` + `Number.isFinite` + `> 0`. `"2500"` → 201, `"not-a-number"` → 400, `-5` → 400. |
| F10 | 🟡 | `gateway/src/routes/{auction,market,auth}.ts` | Non-`AppError` exceptions were mapped to a generic 500 with **nothing logged** → unexplainable 500s | **✅ FIXED** — every catch logs `{ err, url }` before the fallback (15 sites). |
| F11 | 🟡 | `packages/p2p/src/index.ts` | `KademliaDHT`/`dht` not exported → no way to drive the overlay without booting gRPC | **✅ FIXED & VERIFIED** — re-exported; `demo:dht` uses it. |
| F12 | 🟡 | `scripts/dev-services.mjs`, `.env.example` | Nothing set `TURN_*`, so `getICEServers()` returned **STUN only** — FA-1 §3.3 requires STUN **and** TURN. `.env.example` also documented the wrong credential (`apmc-turn-secret` vs coturn's actual `apmcpass`). | **✅ FIXED & VERIFIED** — launcher defaults `TURN_SERVER=turn:localhost:3478`, `apmc`/`apmcpass` (matching the coturn container); `.env.example` corrected. Demo now lists `turn:localhost:3478` as TURN. |

**Already correct on inspection (no change needed):** `dht.ts` `getBucketIndex` returns
`distance.toString(2).length - 1` (the P-H5 off-by-one is not present);
`getGossipState`/`getDHTInfo` already return snake_case (P-M1); `putDHT` registers in gossip state
(`state_size: 1` over the wire, so P-M2 holds); `placeBid` uses a proper
`SELECT … FOR UPDATE` transaction — **do not touch it**.

---

## D. Known-open, deliberately not fixed

Mostly **FA-2 scope**: the FA-1 syllabus does not cover Bully election, Ricart–Agrawala, advanced
replication/caching, or latency metrics. The last two rows are simply low-value, not FA-2.

| ID | Sev | Area | Problem |
|---|---|---|---|
| P-H1 | 🟠 | `p2p/leader-election.ts` | Election never started at boot → cluster permanently leaderless |
| P-H2 | 🟠 | `p2p/leader-election.ts` | All nodes priority = 1, no ID tiebreak → every node self-elects (split brain) |
| P-H3 | 🟠 | `p2p/leader-election.ts` | `responses` counts sends not OKs; `electionTimeout` never assigned → election can deadlock |
| P-H4 | 🟠 | `p2p/leader-election.ts` | Followers never check leader liveness; the recovery branch is unsatisfiable dead code |
| P-M3 | 🟡 | `p2p/index.ts`, gossip | Gossip/heartbeat intervals not cleared on shutdown |
| P-M4 | 🟡 | `p2p/dht.ts` | k-bucket eviction uses XOR distance, not Kademlia ping-LRS liveness |
| P-M5 | 🟡 | `p2p/leader-election.ts` | `handleCoordinator` accepts any COORDINATOR without a priority check |
| P-L2 | ⚪ | `p2p/dht.ts` | `lookup()` is local-only; `refreshBuckets`/`cleanup` never scheduled |
| P-L3 | ⚪ | `p2p` | Election/gossip not networked — message handlers are no-ops, peers never added |
| S4b | 🟡 | `middleware/tracing.ts` | No trace `spanProcessor` → spans never reach Jaeger |
| G1 | ⚪ | `config`, `gateway`, `docker-compose.yml` | `GATEWAY_GRPC_PORT` (8001) is validated by Zod and published in compose, but **no code binds it** — the gateway is a gRPC *client* of auction, never a server. So `dev:services` shows 6 listeners, not 7. Harmless (nothing calls 8001); it is a dead config knob. `REQUIRED_PORTS` in the launcher still watches 8001, which costs nothing and would matter if the server is ever added. |
| M1 | 🟡 | `auction` | Money is `parseFloat` (precision risk). Left alone by decision — changing it touches the whole bid path. |

**If the examiner asks about leader election:** it is FA-2 material and the code is present but
unaudited. Say so plainly rather than demoing it — `demo:dht` is the P2P demo that actually works.

---

## How to use this file

Fix order was **A (crash) → B (startup) → C (FA-1 correctness)**. Section D is the FA-2 backlog.
When adding a finding, record the file:line, the observable symptom, and how you *verified* the fix —
not just that you changed code.
