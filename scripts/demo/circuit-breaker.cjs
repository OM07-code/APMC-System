// FA-1 §4.2 — FAULT TOLERANCE: Circuit Breaker (+ CAP/BASE trade-off)
//
// Self-contained: needs NO infrastructure. Run with:  npm run demo:circuit-breaker
//
// Demonstrates the three states of the Circuit Breaker pattern protecting a caller
// from a failing remote dependency:
//   CLOSED    → calls pass through; consecutive failures are counted
//   OPEN      → threshold exceeded; calls FAIL FAST (no waiting on a dead service)
//   HALF_OPEN → after resetTimeout, probes are allowed; enough successes close it
//
// This is the AP side of CAP: rather than block every caller waiting on an unreachable
// replica, the system sheds load instantly and stays available (degraded, not dead).

const { circuitBreakerRegistry, CircuitState } = require('@apmc/fault');

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const line = (s) => console.log(s);

// A simulated remote "price feed" service we can flip between healthy and broken.
let serviceHealthy = false;
let callsReachedService = 0;

async function priceFeedCall() {
  callsReachedService++;
  if (!serviceHealthy) {
    throw new Error('ECONNREFUSED: price-feed unreachable');
  }
  return { commodity: 'WHEAT', price: 2450 };
}

// Call through the breaker and report whether the request actually left the process.
async function guardedCall(breaker, label) {
  const before = breaker.getState();
  const reachedBefore = callsReachedService;
  try {
    const res = await breaker.execute(priceFeedCall);
    line(`  ${label}: OK   price=${res.price}   [${before} -> ${breaker.getState()}]`);
  } catch (err) {
    const reachedService = callsReachedService > reachedBefore;
    const how = reachedService
      ? 'reached the service, real failure'
      : 'FAIL-FAST — never left the process';
    line(`  ${label}: FAIL (${how})   [${before} -> ${breaker.getState()}]`);
  }
}

(async () => {
  // The registry is how services share one breaker per dependency (see how
  // packages/broker and the gateway obtain theirs).
  const breaker = circuitBreakerRegistry.getOrCreate('price-feed', {
    failureThreshold: 3, // 3 consecutive failures -> OPEN
    successThreshold: 2, // 2 consecutive successes while HALF_OPEN -> CLOSED
    resetTimeout: 1500,  // stay OPEN 1.5s before allowing a probe
    timeout: 1000,       // per-call timeout guard
  });

  line('=== FA-1 §4.2  Circuit Breaker (packages/fault) ===\n');
  line('config: failureThreshold=3  successThreshold=2  resetTimeout=1500ms\n');
  line(`initial state: ${breaker.getState()}   (expect CLOSED)\n`);

  line('--- Phase 1: dependency is DOWN ---');
  for (let i = 1; i <= 6; i++) await guardedCall(breaker, `call ${i}`);

  line(`\n  requests that actually hit the network: ${callsReachedService} of 6`);
  line('  ^ once OPEN, the breaker rejected the rest instantly. In a real cluster that');
  line('    is the difference between one slow dependency and cascading thread exhaustion.');
  line(`  state: ${breaker.getState()}   (expect OPEN)`);
  line(`  stats: ${JSON.stringify(breaker.getStats())}\n`);

  line('--- Phase 2: still down, but resetTimeout elapses -> probe is allowed ---');
  await sleep(1600);
  line(`  state after 1.6s: ${breaker.getState()}   (expect HALF_OPEN — one probe allowed)`);
  await guardedCall(breaker, 'probe');
  line(`  probe failed while HALF_OPEN -> immediately back to ${breaker.getState()}`);
  line('  ^ a single failed probe re-opens the circuit; it does NOT wait for 3 more.\n');

  line('--- Phase 3: dependency RECOVERS ---');
  serviceHealthy = true;
  await sleep(1600);
  line(`  state after resetTimeout: ${breaker.getState()}   (expect HALF_OPEN)`);
  await guardedCall(breaker, 'probe 1');
  line(`    still HALF_OPEN? ${breaker.getState()}  (needs successThreshold=2 successes)`);
  await guardedCall(breaker, 'probe 2');
  line(`\n  final state: ${breaker.getState()}   (expect CLOSED — dependency trusted again)`);

  line('\n--- Phase 4: normal traffic flows again ---');
  for (let i = 1; i <= 3; i++) await guardedCall(breaker, `call ${i}`);

  line('\n--- Registry view (all breakers in this process) ---');
  line(JSON.stringify(circuitBreakerRegistry.getAllStats(), null, 2));

  line('\n=== DS concept shown: fault tolerance via failure detection, fail-fast load');
  line('    shedding, and a self-healing probe cycle; availability preserved under');
  line('    partial failure (CAP → AP, BASE eventual recovery). ===');
})().catch((e) => {
  console.error('DEMO FAILED:', e);
  process.exit(1);
});
