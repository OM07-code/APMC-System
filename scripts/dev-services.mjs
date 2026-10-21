#!/usr/bin/env node
// @ts-check
/**
 * dev-services.mjs — one-command host launcher for the 5 APMC app services.
 *
 * Why this exists (Phase 1):
 *   - Runs all 5 already-built Node services from ONE terminal without Docker,
 *     while core infra (Postgres/Redis/Redpanda) runs via `npm run docker:up:core`.
 *   - Gives each child a DISTINCT PROMETHEUS_PORT so the metrics exporters don't
 *     collide on 9090 (only the first service would otherwise serve metrics).
 *   - No external deps (concurrently is not installed): pure child_process.
 *
 * Usage:
 *   npm run docker:up:core       # start Postgres + Redis + Redpanda
 *   npm run build                # ensure dist/ is fresh
 *   npm run db:migrate && npm run db:seed
 *   npm run dev:services         # <- this script
 *
 * Ctrl-C once to stop all services gracefully.
 */
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { existsSync } from 'node:fs';

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, '..');

// Start order matters mildly: naming (registry) first, gateway last (its /health
// probes the others). Each service still boots independently of infra — DB/Redis/
// Kafka connect lazily on first use — so a missing dependency degrades a feature,
// it does not stop startup.
const SERVICES = [
  { name: 'naming',    dist: 'packages/naming/dist/index.js',    prom: 9094, color: '\x1b[35m' }, // magenta
  { name: 'auction',   dist: 'packages/auction/dist/index.js',   prom: 9090, color: '\x1b[32m' }, // green
  { name: 'signaling', dist: 'packages/signaling/dist/index.js', prom: 9091, color: '\x1b[36m' }, // cyan
  { name: 'p2p',       dist: 'packages/p2p/dist/index.js',       prom: 9095, color: '\x1b[33m' }, // yellow (9095: avoid Redpanda's host 9092)
  { name: 'gateway',   dist: 'packages/gateway/dist/index.js',   prom: 9093, color: '\x1b[34m' }, // blue
];

const RESET = '\x1b[0m';
const DIM = '\x1b[2m';
const RED = '\x1b[31m';
const STAGGER_MS = 800; // small gap between spawns so logs interleave readably

/** @type {{name: string, color: string, child: import('node:child_process').ChildProcess}[]} */
const running = [];
let shuttingDown = false;

/** Prefix every line of a chunk with a colored [service] tag. */
function pipe(stream, name, color, isErr) {
  let buf = '';
  stream.on('data', (chunk) => {
    buf += chunk.toString();
    const lines = buf.split('\n');
    buf = lines.pop() ?? '';
    for (const line of lines) {
      const tag = `${color}[${name}]${RESET}`;
      process.stdout.write(isErr ? `${tag} ${line}\n` : `${tag} ${line}\n`);
    }
  });
}

function startService(svc) {
  const abs = join(ROOT, svc.dist);
  if (!existsSync(abs)) {
    console.error(`${RED}[launcher] ${svc.name}: build output missing at ${svc.dist} — run \`npm run build\` first${RESET}`);
    return null;
  }

  const child = spawn(process.execPath, [abs], {
    cwd: ROOT,
    env: {
      ...process.env,
      // Distinct metrics port per service (avoids the 9090 collision).
      PROMETHEUS_PORT: String(svc.prom),
      // Label the service for logs / tracing / service-discovery.
      SERVICE_NAME: `apmc-${svc.name}`,
      // Host services reach Redpanda's EXTERNAL listener (localhost:19092);
      // the INTERNAL listener (redpanda:9092) stays for the full-Docker path.
      KAFKA_BROKERS: process.env.KAFKA_BROKERS ?? 'localhost:19092',
      // WebRTC relay. FA-1 §3.3 needs BOTH STUN (discover your public address) and
      // TURN (relay when NAT/firewall blocks a direct path), so the signaling service
      // must hand a TURN entry to peers in its ICE config. Credentials match the
      // coturn container in docker-compose.yml (`--user=apmc:apmcpass`, realm
      // apmc.local). Start it with `npm run docker:up:webrtc`. If coturn is not
      // running, ICE simply falls back to STUN/host candidates.
      TURN_SERVER: process.env.TURN_SERVER ?? 'turn:localhost:3478',
      TURN_USERNAME: process.env.TURN_USERNAME ?? 'apmc',
      TURN_CREDENTIAL: process.env.TURN_CREDENTIAL ?? 'apmcpass',
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  });

  pipe(child.stdout, svc.name, svc.color, false);
  pipe(child.stderr, svc.name, svc.color, true);

  child.on('exit', (code, signal) => {
    const how = signal ? `signal ${signal}` : `code ${code}`;
    console.log(`${svc.color}[${svc.name}]${RESET} ${DIM}exited (${how})${RESET}`);
    // If a service dies unexpectedly (not during our own shutdown), tear down the
    // rest so the user isn't left with a half-running stack that looks healthy.
    if (!shuttingDown && code !== 0) {
      console.error(`${RED}[launcher] ${svc.name} exited with ${how} — shutting down all services${RESET}`);
      shutdown('child-failure');
    }
  });

  console.log(`${svc.color}[${svc.name}]${RESET} ${DIM}starting (metrics :${svc.prom})${RESET}`);
  running.push({ name: svc.name, color: svc.color, child });
  return child;
}

function shutdown(reason) {
  if (shuttingDown) return;
  shuttingDown = true;
  console.log(`\n${DIM}[launcher] shutting down (${reason})...${RESET}`);
  for (const { child } of running) {
    if (!child.killed) child.kill('SIGTERM');
  }
  // Hard exit if children don't die within 10s.
  const t = setTimeout(() => {
    for (const { child } of running) {
      if (!child.killed) child.kill('SIGKILL');
    }
    process.exit(1);
  }, 10_000);
  t.unref();
}

process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));

// Ports the 5 services bind. Used only by the preflight check below.
const REQUIRED_PORTS = [8000, 8001, 50051, 50052, 50053, 50054, 51052];

/**
 * Fail fast, and usefully, if a previous run left orphaned services behind.
 *
 * Why this matters: if the launcher is force-killed (terminal closed, kill -9,
 * task manager) its SIGTERM handler never runs, so the 5 children survive and keep
 * their ports. The next `npm run dev:services` then dies with a raw
 * "No address added out of total 1 resolved errors: [listen EADDRINUSE ...]" gRPC
 * stack trace, which says nothing about what to actually do. This turns that into
 * the exact taskkill/kill command to run.
 */
async function preflightPorts() {
  const { execSync } = await import('node:child_process');
  const isWin = process.platform === 'win32';
  let out = '';
  try {
    out = execSync(isWin ? 'netstat -ano' : 'lsof -nP -iTCP -sTCP:LISTEN', {
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'ignore'],
    });
  } catch {
    return; // can't check (no netstat/lsof) — let the services report their own errors
  }

  /** @type {Map<number, string>} port -> pid */
  const busy = new Map();
  for (const line of out.split('\n')) {
    if (isWin) {
      // "  TCP    0.0.0.0:8000    0.0.0.0:0    LISTENING    4216"
      const m = line.match(/^\s*TCP\s+\S+:(\d+)\s+\S+\s+LISTENING\s+(\d+)/);
      if (m) {
        const port = Number(m[1]);
        if (REQUIRED_PORTS.includes(port) && !busy.has(port)) busy.set(port, m[2]);
      }
    } else {
      const m = line.match(/^\S+\s+(\d+)\s+.*:(\d+)\s+\(LISTEN\)/);
      if (m) {
        const port = Number(m[2]);
        if (REQUIRED_PORTS.includes(port) && !busy.has(port)) busy.set(port, m[1]);
      }
    }
  }

  if (busy.size === 0) return;

  console.error(`${RED}[launcher] cannot start — these service ports are already in use:${RESET}`);
  for (const [port, pid] of [...busy].sort((a, b) => a[0] - b[0])) {
    console.error(`${RED}  port ${port} held by PID ${pid}${RESET}`);
  }
  const pids = [...new Set(busy.values())];
  console.error(
    `\n${DIM}Almost certainly orphaned services from an earlier run whose launcher was\n` +
      `force-killed. Stop them, then start again:${RESET}`
  );
  console.error(
    isWin
      ? `\n  ${pids.map((p) => `taskkill /PID ${p} /F`).join('\n  ')}\n`
      : `\n  kill -9 ${pids.join(' ')}\n`
  );
  process.exit(1);
}

// Staggered sequential start.
(async () => {
  await preflightPorts();
  console.log(`${DIM}[launcher] starting ${SERVICES.length} services from ${ROOT}${RESET}`);
  for (const svc of SERVICES) {
    startService(svc);
    await new Promise((r) => setTimeout(r, STAGGER_MS));
  }
  console.log(`${DIM}[launcher] all services launched. Gateway HTTP on :8000. Ctrl-C to stop.${RESET}`);
})();
