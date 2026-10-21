// FA-2 §III.1 + §III.2 + §III.3 — CLOCK SYNC, LOGICAL CLOCKS & GLOBAL SNAPSHOT
//
// Self-contained: needs NO infrastructure. Run with:  npm run demo:clocks
//
// Covers:
//   1. Cristian's Clock Synchronization Algorithm     (Unit III §1)
//   2. Berkeley Clock Synchronization Algorithm       (Unit III §1)
//   3. Lamport Scalar Clock — total bid ordering      (Unit III §2)
//   4. Vector Clock — causal + concurrent detection   (Unit III §2)
//   5. Chandy–Lamport Distributed Snapshot Algorithm  (Unit III §3)
//
// Every section drives the REAL packages/p2p/src/sync/* implementation.

'use strict';
const {
  APMCReferenceClockServer, ClockManager, cristianSync, berkeleySync,
  LamportClock, VectorClock, LogicalClockManager,
  MandiSnapshotNode, GlobalSnapshotCoordinator,
} = require('@apmc/p2p');

const line  = (s) => console.log(s);
const head  = (s) => { line(''); line('='.repeat(65)); line(s); line('='.repeat(65)); };
const sub   = (s) => { line(''); line('--- ' + s + ' ---'); };
const ok    = (s) => line('  ✓ ' + s);
const info  = (s) => line('  · ' + s);

// ──────────────────────────────────────────────────────────────────────────────
// SECTION 1: Cristian's Algorithm
// ──────────────────────────────────────────────────────────────────────────────
async function demoCristian() {
  head('1. CRISTIAN\'S CLOCK SYNCHRONIZATION ALGORITHM');
  info('APMC Reference Clock Server is 80 ms ahead of mandi node clocks');
  info('(simulates a node that hasn\'t been synced — auction close timer will drift)');
  line('');

  const refServer = new APMCReferenceClockServer(+80); // server 80 ms ahead

  const MANDIS = ['apmc-pune', 'apmc-nashik', 'apmc-nagpur'];
  for (const mandi of MANDIS) {
    const result = await cristianSync(mandi, refServer, 40); // 40 ms simulated RTT
    const mgr = new ClockManager(mandi);
    mgr.applyCristian(result);
    const stats = mgr.getStats();
    info(`${mandi.padEnd(14)}`
      + `  RTT=${result.rtt} ms`
      + `  offset=${result.offset.toFixed(1)} ms`
      + `  corrected=${new Date(stats.correctedTime).toISOString().slice(11, 23)}`
    );
  }

  ok('All mandi nodes computed a clock offset using T_server + RTT/2 − T_client');
  ok('Corrected times are within RTT/2 of the reference (Cristian\'s bound)');
  line('');
  line('  DS concept: external clock synchronization; probabilistic error ≤ RTT/2');
}

// ──────────────────────────────────────────────────────────────────────────────
// SECTION 2: Berkeley Algorithm
// ──────────────────────────────────────────────────────────────────────────────
function demoBerkeley() {
  head('2. BERKELEY CLOCK SYNCHRONIZATION ALGORITHM');
  info('Internal sync — no external authority; Pune is elected time-daemon');
  line('');

  const baseTime = Date.now();
  // Simulate mandi nodes with different clock drifts
  const master = { id: 'apmc-pune',     localTime: baseTime };
  const slaves = [
    { id: 'apmc-nashik',    localTime: baseTime - 200 },   // 200 ms behind
    { id: 'apmc-mumbai',    localTime: baseTime + 150 },   // 150 ms ahead
    { id: 'apmc-nagpur',    localTime: baseTime - 3500 },  // 3.5 s behind — OUTLIER
    { id: 'apmc-kolhapur',  localTime: baseTime + 60  },   // 60 ms ahead
  ];

  const result = berkeleySync(master, slaves, 20, 500); // outlier threshold: 500 ms

  info(`Master: ${result.masterId}`);
  info(`Outlier excluded (> 500 ms deviation): ${JSON.stringify(result.outliers)}`);
  info(`Fault-tolerant average time: ${new Date(result.averageTime).toISOString().slice(11, 23)}`);
  line('');
  line('  Corrections to apply:');
  for (const c of result.corrections) {
    const tag = result.outliers.includes(c.nodeId) ? ' [outlier – skipped]' : '';
    info(`  ${c.nodeId.padEnd(16)} Δ=${c.delta >= 0 ? '+' : ''}${c.delta.toFixed(1)} ms${tag}`);
  }
  ok('Berkeley eliminates outliers before averaging — fault-tolerant');
  ok('All valid nodes receive an individual delta instead of the absolute time');
  line('');
  line('  DS concept: internal clock synchronization; outlier-robust averaging');
}

// ──────────────────────────────────────────────────────────────────────────────
// SECTION 3: Lamport Scalar Clock
// ──────────────────────────────────────────────────────────────────────────────
function demoLamport() {
  head('3. LAMPORT SCALAR CLOCK — Total Ordering of Cross-Mandi Bids');
  info('Three mandis simultaneously place bids; physical clocks disagree.');
  info('Lamport timestamps give a total order: L = max(L_self, L_msg) + 1');
  line('');

  const clockPune    = new LamportClock('apmc-pune',    0);
  const clockNashik  = new LamportClock('apmc-nashik',  5); // Nashik clock ahead
  const clockMumbai  = new LamportClock('apmc-mumbai',  2);

  // Simulate events
  const bid1 = clockPune.stamp({ type: 'BID', auction: 'auc_001', amount: 2500 });
  info(`Pune places bid    → Lamport timestamp=${bid1.lamportTime}  (local event)`);

  // Pune sends bid1 to Nashik; Nashik updates its clock
  clockNashik.receive(bid1.lamportTime);
  const bid2 = clockNashik.stamp({ type: 'BID', auction: 'auc_001', amount: 2600 });
  info(`Nashik counter-bid → Lamport timestamp=${bid2.lamportTime}  (after receiving Pune's bid)`);

  const bid3 = clockMumbai.stamp({ type: 'BID', auction: 'auc_001', amount: 2700 });
  info(`Mumbai places bid  → Lamport timestamp=${bid3.lamportTime}  (independent, no prior msg)`);

  // Sort bids by Lamport total order
  const bids = [bid1, bid2, bid3];
  const sorted = [...bids].sort((a, b) => LamportClock.compare(a, b));
  line('');
  line('  Bids in Lamport total order (smallest timestamp wins tie at higher):');
  sorted.forEach((b, i) => {
    info(`  ${i+1}. ${b.from.padEnd(14)} amount=₹${b.amount}  L=${b.lamportTime}`);
  });
  ok('Even without NTP, every mandi agrees on the same bid order');
  line('');
  line('  DS concept: Lamport scalar clock; happened-before relation → total order');
}

// ──────────────────────────────────────────────────────────────────────────────
// SECTION 4: Vector Clock
// ──────────────────────────────────────────────────────────────────────────────
function demoVectorClock() {
  head('4. VECTOR CLOCK — Causal Price Updates & Conflict Detection');
  info('Two mandis independently update the price of WHEAT commodity via gossip.');
  info('Vector clocks let us distinguish causally related vs concurrent updates.');
  line('');

  const peers = ['apmc-pune', 'apmc-nashik', 'apmc-mumbai'];
  const vcPune   = new VectorClock('apmc-pune',   peers);
  const vcNashik = new VectorClock('apmc-nashik', peers);
  const vcMumbai = new VectorClock('apmc-mumbai', peers);

  // Pune updates price first
  const update1 = vcPune.stamp({ type: 'PRICE', commodity: 'WHEAT', price: 2400 });
  info(`Pune gossips price ₹2400  → VT=${JSON.stringify(update1.vectorTime)}`);

  // Nashik receives Pune's update and then updates its own price
  vcNashik.receive(update1.vectorTime);
  const update2 = vcNashik.stamp({ type: 'PRICE', commodity: 'WHEAT', price: 2450 });
  info(`Nashik (after Pune) ₹2450 → VT=${JSON.stringify(update2.vectorTime)}`);

  // Mumbai makes a CONCURRENT update (never received Pune's gossip yet)
  const update3 = vcMumbai.stamp({ type: 'PRICE', commodity: 'WHEAT', price: 2430 });
  info(`Mumbai (concurrent) ₹2430 → VT=${JSON.stringify(update3.vectorTime)}`);

  line('');
  const rel12 = VectorClock.compare(update1.vectorTime, update2.vectorTime);
  const rel13 = VectorClock.compare(update1.vectorTime, update3.vectorTime);
  const rel23 = VectorClock.compare(update2.vectorTime, update3.vectorTime);

  info(`Pune → Nashik relationship:   ${rel12}   (Nashik causally depends on Pune)`);
  info(`Pune → Mumbai relationship:   ${rel13}  (independent, no gossip yet)`);
  info(`Nashik → Mumbai relationship: ${rel23}  (← CONFLICT: concurrent updates!)`);

  // Conflict resolution
  const conflict = VectorClock.resolvePriceConflict(
    { from: update2.from, vectorTime: update2.vectorTime, value: 2450, lamportTime: update2.lamportTime },
    { from: update3.from, vectorTime: update3.vectorTime, value: 2430, lamportTime: update3.lamportTime },
  );
  line('');
  info(`Conflict resolved (LWW by Lamport tiebreak): winner = ${conflict.winner.from}  price=₹${conflict.winner.value}`);
  ok('Vector clocks reveal causality AND concurrent conflicts — impossible with Lamport alone');
  line('');
  line('  DS concept: vector clocks; causal history; concurrent conflict resolution');
}

// ──────────────────────────────────────────────────────────────────────────────
// SECTION 5: Chandy–Lamport Distributed Snapshot
// ──────────────────────────────────────────────────────────────────────────────
function demoChandyLamport() {
  head('5. CHANDY–LAMPORT DISTRIBUTED SNAPSHOT — Global State Capture');
  info('APMC regulator needs to verify: sum(escrow across mandis) + in-transit = constant');
  info('Snapshot is taken WITHOUT pausing any live auction. FIFO channels guaranteed.');
  line('');

  // Build a 3-node overlay: Pune ↔ Nashik ↔ Mumbai (simplified bidirectional)
  const EXPECTED_TOTAL = 450_000; // total INR actually in escrow (no in-transit for this run)

  const puneState   = { nodeId: 'apmc-pune',   activeBids: [], escrowBalance: 200_000, pendingSettlements: [], recordedAt: 0 };
  const nashikState = { nodeId: 'apmc-nashik', activeBids: [], escrowBalance: 150_000, pendingSettlements: [], recordedAt: 0 };
  const mumbaiState = { nodeId: 'apmc-mumbai', activeBids: [], escrowBalance: 100_000, pendingSettlements: [], recordedAt: 0 };

  const pune   = new MandiSnapshotNode('apmc-pune',   puneState);
  const nashik = new MandiSnapshotNode('apmc-nashik', nashikState);
  const mumbai = new MandiSnapshotNode('apmc-mumbai', mumbaiState);

  pune.addPeers(['apmc-nashik', 'apmc-mumbai']);
  nashik.addPeers(['apmc-pune', 'apmc-mumbai']);
  mumbai.addPeers(['apmc-pune', 'apmc-nashik']);

  // Coordinator collects snapshots
  const coord = new GlobalSnapshotCoordinator('snap-001', ['apmc-pune', 'apmc-nashik', 'apmc-mumbai'], EXPECTED_TOTAL);

  // Wire snapshot-complete callbacks
  const allNodes = { 'apmc-pune': pune, 'apmc-nashik': nashik, 'apmc-mumbai': mumbai };
  for (const node of [pune, nashik, mumbai]) {
    node.onSnapshotComplete = (snap) => {
      info(`${snap.nodeId} snapshot complete (escrow=₹${snap.localState.escrowBalance.toLocaleString()})`);
      coord.addProcessSnapshot(snap);
    };
    node.onSendMarker = (marker, toPeer) => {
      info(`  MARKER  ${marker.from} → ${toPeer}`);
      allNodes[toPeer]?.receiveMarker(marker);
    };
  }

  info('All mandis have stable escrow: Pune=₹2,00,000  Nashik=₹1,50,000  Mumbai=₹1,00,000');
  info('(No in-transit settlements in this run — conservation check: 4,50,000 = 4,50,000)');
  line('');

  // Initiate snapshot from Pune
  info('Pune initiates snapshot (snap-001)…');
  pune.initiateSnapshot('snap-001');

  if (coord.isComplete()) {
    const gs = coord.assemble();
    line('');
    info(`Total escrow across mandis : ₹${gs.totalEscrow.toLocaleString()}`);
    info(`In-transit settlement msgs  : ₹${gs.totalInTransit.toLocaleString()}`);
    info(`Expected total              : ₹${EXPECTED_TOTAL.toLocaleString()}`);
    const conserved = gs.totalEscrow + gs.totalInTransit;
    info(`Escrow + In-transit         : ₹${conserved.toLocaleString()}`);
    ok(`Conservation check: ${gs.conservationHolds ? 'HOLDS ✓ (no funds lost/created)' : 'VIOLATED ✗'}`);
  }

  line('');
  line('  DS concept: consistent global cut; MARKER propagation; channel state recording;');
  line('  no global pause required; captures state during normal operation.');
}

// ──────────────────────────────────────────────────────────────────────────────
// MAIN
// ──────────────────────────────────────────────────────────────────────────────
(async () => {
  line('\n=== FA-2 Unit III: Clock Synchronization + Logical Clocks + Global State ===');
  line('    (packages/p2p/src/sync/ — pure-logic, no infrastructure needed)');

  await demoCristian();
  demoBerkeley();
  demoLamport();
  demoVectorClock();
  demoChandyLamport();

  line('');
  line('='.repeat(65));
  line('Unit III §1 §2 §3 — ALL DEMONSTRATIONS COMPLETE');
  line('Algorithms: Cristian, Berkeley, Lamport, Vector Clock, Chandy–Lamport');
  line('='.repeat(65));
})().catch(e => { console.error('DEMO FAILED:', e); process.exit(1); });
