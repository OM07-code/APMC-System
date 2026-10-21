# FA-1 demo scripts

Each script demonstrates one **Distributed Systems** concept from the FA-1 syllabus by
driving the project's *real* implementation (`packages/*`) — not a mock. Run them one at a
time during the viva: each prints a narrated trace plus a closing summary naming the
concept it just proved.

## Which demo shows which FA concept

| # | Command | Script | FA section | DS concept demonstrated |
|---|---|---|---|---|
| 1 | `npm run demo:circuit-breaker` | `circuit-breaker.cjs` | **§4.2** Fault tolerance | Failure detection, fail-fast load shedding, CLOSED→OPEN→HALF_OPEN→CLOSED self-healing, CAP→AP / BASE |
| 2 | `npm run demo:naming` | `naming-types.cjs` | **§4.1** Naming | Flat (hash, XOR-routed), structured (DNS-like A/TXT/SRV, zone delegation), attribute-based (directory query) |
| 3 | `npm run demo:dht` | `dht-overlay.cjs` | **§3.4** P2P | Kademlia overlay: 160-bit ID space, XOR distance, k-buckets, iterative lookup, replicated put/get with versioning + TTL |
| 4 | `npm run demo:mom` | `mom-pubsub.cjs` | **§3.2** Message-oriented middleware | Pub/sub decoupling, durable append-only log, **replay** by a new consumer group, per-group offsets, partition ordering, at-least-once redelivery |
| 5 | `npm run demo:webrtc` | `webrtc-signaling.cjs` | **§3.3** Stream-oriented communication | WebRTC signaling: SDP offer/answer, ICE trickling (host/srflx/relay), STUN vs TURN, presence, queued signals for offline peers |
| 6 | `npm run flow:test` | `../flow-test.mjs` | **§3.1** RPC + Unit 1 architecture | End-to-end auction lifecycle over gRPC through the API gateway: JWT auth, create → start → bid → end |

Unit 1 topics (architectures, middleware, IDL, failure models) are shown by the system
itself rather than by a script: the `.proto` files are the **IDL**, `packages/middleware`
is the **middleware layer**, and the 5-service split is the **distributed architecture**.
Point at those during the walkthrough.

## Infrastructure each demo needs

```bash
# 1, 2, 3 — nothing. Pure library code, no Docker, no network.
npm run demo:offline        # runs all three back to back

# 4 — needs Redpanda (Kafka)
npm run docker:up:core      # postgres + redis + redpanda, memory-capped
npm run demo:mom

# 5 — needs Redis + coturn + the signaling service.
#     Use docker:up:webrtc (NOT docker:up:core) so coturn is running and the
#     signaling service can hand peers a real TURN relay alongside STUN.
npm run docker:up:webrtc
npm run dev:services        # in a second terminal; signaling -> ws://localhost:51052
npm run demo:webrtc

# 6 — needs the full host stack (postgres migrated + seeded, all 5 services)
npm run docker:up:core
npm run db:migrate && npm run db:seed
npm run dev:services
npm run flow:test
```

> **Never run `npm run docker:up`** while demoing. It starts 13 containers including
> 5 image builds. Use `docker:up:core` (3 containers, ~380 MB) or `docker:up:webrtc`
> (4 containers, ~410 MB). See `.claude/fix-plan.md`.

If `dev:services` refuses to start with a list of busy ports, a previous run was
force-killed and left orphaned services behind. The launcher prints the exact
`taskkill` commands to run — do that, then start it again.

## Suggested viva order (offline-first, ~8 minutes)

1. `demo:naming` — naming is the foundation everything else resolves through.
2. `demo:dht` — how a *decentralized* mandi network finds data with no central index.
3. `demo:circuit-breaker` — what happens when a node fails (and why the system survives).
4. `demo:mom` — asynchronous price feed, and replay of history a subscriber missed.
5. `demo:webrtc` — the live auction floor stream, negotiated then peer-to-peer.
6. `flow:test` — the whole thing working as one system over RPC.

Starting with the three offline demos means you always have something to show even if
Docker misbehaves on the day.

## Notes

- All demos are `.cjs` (CommonJS) so they can `require()` the compiled `packages/*/dist`
  output directly. Run `npm run build` first if you have pulled changes.
- The MOM demo derives a fresh consumer-group id from the clock on every run, so each run
  replays the topic from offset 0 instead of resuming a previous run's cursor.
- Host processes must reach Redpanda on **`localhost:19092`** (the external listener);
  `redpanda:9092` only resolves *inside* the Docker network. Both the MOM demo and
  `scripts/dev-services.mjs` already default to the right one.
- Ports used: gateway 8000, auction 50051, signaling 50052 gRPC / 51052 WS, p2p 50053,
  naming 50054.
