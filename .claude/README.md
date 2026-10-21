# .claude/ — Resume Here

This folder is the **working memory** for the APMC System project so any new chat can continue
without re-discovering everything. Read these in order:

1. **`project-context.md`** — what the system is, architecture, services, ports, environment facts,
   the FA-1 requirements it must satisfy.
2. **`fix-plan.md`** — Phase 1 (crash fix + one-command launcher) and Phase 2 (FA coverage
   hardening). Both are now **implemented**; the file records what was done.
3. **`bug-log.md`** — every confirmed bug, severity-ranked, with fix status.
4. **`decisions.md`** — decisions already made, open questions, and a running progress log.
5. **`scripts/demo/README.md`** (outside this folder) — the 6 demo commands mapped to FA sections.
   This is the script to follow during the viva.

## Current status (2026-09-02) — DEMO READY

Phase 1 **and** Phase 2 are implemented, built, and verified against a live stack.

**Verified working on this machine, end to end:**

| Check | Result |
|---|---|
| `npm run build` | 11/11 packages clean |
| `npm run dev:services` | all 5 services up, 6 listeners (8000, 50051, 50052, 50053, 50054, 51052), zero errors |
| `npm run flow:test` | full lifecycle: register → create → start → bid ×2 → state → history → end |
| Role gates / authz | 12/12 assertions pass (403 on wrong role, spoofed `farmer_id` ignored, 400 on bad money, 401 unauthenticated) |
| `npm run demo:circuit-breaker` | full CLOSED→OPEN→HALF_OPEN→OPEN→HALF_OPEN→CLOSED cycle |
| `npm run demo:naming` | flat + structured (A/TXT/SRV) + attribute naming |
| `npm run demo:dht` | 6-node overlay, XOR routing, put/get, TTL expiry |
| `npm run demo:mom` | publish → replay from offset 0 by a new group → 2 independent groups → ordering → real redelivery after a handler throw |
| `npm run demo:webrtc` | register → presence → discovery → SDP offer/answer → 3 ICE candidates both directions → offline queueing → peer_left |
| Seeded logins | `password123` verifies against the DB hash (was a broken placeholder) |
| coturn | **healthy** (healthcheck was missing `-p`, so it was permanently unhealthy) |
| Container memory | 4 containers ≈ 410 MB total, all capped |

**FA-1 coverage:** all of Unit 2 §3.1–§3.4, §4.1, §4.2 have a runnable demo. Unit 1 topics are
shown by the system itself (proto = IDL, `packages/middleware` = middleware, 5-service split =
architecture).

**Deliberately OUT of scope (FA-2, not FA-1):** Bully leader election, Ricart–Agrawala mutual
exclusion, advanced replication/caching, latency metrics, Jaeger tracing preload. The election and
gossip code still exists and starts; its *correctness* was not audited because FA-1 does not cover it.

## The user's standing rules

- **Explain first, then ASK before changing ANY code.** (Writing to `.claude/` docs is fine.)
  This was **superseded for the agreed Phase 1 + Phase 2 plan** by the user's explicit
  "FIX EVERYTHING" go-ahead — but it applies again to any NEW work beyond that plan.
- Machine is **16 GB RAM, Windows 11, Docker Desktop**. Keep resource use low.
- Run mode decided: **host launcher only** (`npm run dev:services`). Docker mode B was dropped.
- FA-1 must **demonstrate** Unit 1 + Unit 2 DS concepts, **including WebRTC + P2P** — so coturn and
  the P2P/DHT code must stay and work, not be removed.

## Two things to know before you touch anything

1. **`npm run docker:up` still starts 13 containers including 5 image builds.** The coturn UDP port
   range that caused the PC crash is fixed (16,384 → 41 ports) and every container is memory-capped,
   but the full stack has NOT been run on this machine. Use `npm run docker:up:core` (3 containers)
   or `npm run docker:up:webrtc` (adds coturn, needed for TURN in the WebRTC demo).
2. **If `dev:services` is force-killed, the 5 child services survive** and hold their ports. The
   launcher now preflight-checks and prints the exact `taskkill` commands — follow them, don't
   guess.

## Fast path for a new chat

> "Read `.claude/README.md`, `.claude/project-context.md`, `.claude/bug-log.md`, and
> `.claude/decisions.md`, plus `scripts/demo/README.md`. Phase 1 and 2 are done and verified —
> tell me what you'd check next, and ask before changing code."
