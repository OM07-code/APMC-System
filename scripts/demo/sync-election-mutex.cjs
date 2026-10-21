// FA-2 §III.4 + §III.5 + §III.6 — ELECTION, BEACON, MUTEX & DEADLOCK DETECTION
//
// Self-contained: needs NO infrastructure. Run with:  npm run demo:election-mutex
//
// Covers:
//   1. Bully Election Algorithm (FIXED bugs P-H1…P-H4, P-M5)  (Unit III §4)
//   2. Ring Election Algorithm                                   (Unit III §4)
//   3. Beacon Protocol (leader liveness + follower clock sync)   (Unit III §4)
//   4. Ricart–Agrawala Mutual Exclusion                          (Unit III §5)
//   5. Lodha–Kshemkalyani Fair Mutual Exclusion                  (Unit III §5 self-study)
//   6. Knapp's 4-category Deadlock Detection                     (Unit III §6 self-study)
//
// Every section drives the REAL packages/p2p/src/ implementation.

'use strict';
const {
  BullyLeaderElection, NodeState, RingElection,
  BeaconManager,
  RicartAgrawalaMutex, LodhaKshemkalyaniMutex,
  LocalWFG, PathPushingDetector, EdgeChasingDetector,
  DiffusingComputationDetector, GlobalStateDeadlockDetector,
  KnappCategories,
} = require('@apmc/p2p');

const line  = (s) => console.log(s);
const head  = (s) => { line(''); line('='.repeat(65)); line(s); line('='.repeat(65)); };
const sub   = (s) => { line(''); line('--- ' + s + ' ---'); };
const ok    = (s) => line('  ✓ ' + s);
const info  = (s) => line('  · ' + s);

// ──────────────────────────────────────────────────────────────────────────────
// SECTION 1: Bully Election (fixed)
// ──────────────────────────────────────────────────────────────────────────────
async function demoBully() {
  head('1. BULLY ELECTION ALGORITHM (Bugs P-H1…P-H4, P-M5 fixed)');
  info('6 APMC mandi nodes with DISTINCT priorities (P-H2 fix)');
  info('Highest priority node wins.  Crash + re-election demonstrated.');
  line('');

  // APMC mandi nodes with distinct priorities (P-H2 fix)
  const NODES = [
    { id: 'apmc-pune',     priority: 6 },
    { id: 'apmc-nashik',   priority: 5 },
    { id: 'apmc-mumbai',   priority: 4 },
    { id: 'apmc-nagpur',   priority: 3 },
    { id: 'apmc-solapur',  priority: 2 },
    { id: 'apmc-kolhapur', priority: 1 },
  ];

  // Create election nodes
  const nodes = new Map();
  for (const { id, priority } of NODES) {
    const n = new BullyLeaderElection(id, 'localhost', 50053, priority);
    nodes.set(id, n);
  }

  // Register all peers in each node
  for (const [id, node] of nodes) {
    for (const { id: peerId, priority: peerPriority } of NODES) {
      if (peerId !== id) {
        node.addNode({ id: peerId, host: 'localhost', port: 50053, priority: peerPriority, lastHeartbeat: Date.now(), state: NodeState.FOLLOWER });
      }
    }
  }

  // Wire message delivery
  for (const [, node] of nodes) {
    node.setMessageHandler((msg, targetId) => {
      const target = nodes.get(targetId);
      if (target) target.handleElectionMessage(msg);
    });
    node.on('leadership-gained', () => info(`  *** ${node.getNodeId()} elected as LEADER (priority=${node.getPriority()}) ***`));
    node.on('leader-changed',    (l) => info(`  ${node.getNodeId()} acknowledges new leader: ${l}`));
  }

  // 1a. Kolhapur (lowest priority) starts the election
  sub('a) Kolhapur initiates election');
  await nodes.get('apmc-kolhapur').startElection();
  await new Promise(r => setTimeout(r, 500));

  const leader1 = nodes.get('apmc-pune').getCurrentLeader();
  ok(`Round 1 leader: ${leader1} (highest priority should win)`);
  if (leader1 !== 'apmc-pune') line('  ⚠ unexpected leader — check priority logic');

  // 1b. Simulate Pune (leader) crash → Nashik detects and re-elects
  sub('b) Pune (leader) crashes — Nashik initiates new election');
  // Mark Pune as dead (old heartbeat)
  for (const [, node] of nodes) {
    const pn = node.getClusterStatus().nodes.find(n => n.id === 'apmc-pune');
    if (pn) pn.lastHeartbeat = Date.now() - 20_000;  // simulate stale heartbeat
  }
  await nodes.get('apmc-nashik').startElection();
  await new Promise(r => setTimeout(r, 500));

  const leader2 = nodes.get('apmc-nashik').getCurrentLeader();
  ok(`Round 2 leader: ${leader2} (Pune dead → Nashik should win)`);

  line('');
  line('  DS concept: Bully election; fault detection; priority-based leader selection');
}

// ──────────────────────────────────────────────────────────────────────────────
// SECTION 2: Ring Election
// ──────────────────────────────────────────────────────────────────────────────
function demoRing() {
  head('2. RING ELECTION ALGORITHM');
  info('Same 6 mandi nodes form a logical ring (sorted by ID).');
  info('Any node initiates; best candidate is forwarded clockwise.');
  line('');

  const RING_NODES = [
    { id: 'apmc-kolhapur', priority: 1 },
    { id: 'apmc-mumbai',   priority: 4 },
    { id: 'apmc-nagpur',   priority: 3 },
    { id: 'apmc-nashik',   priority: 5 },
    { id: 'apmc-pune',     priority: 6 },
    { id: 'apmc-solapur',  priority: 2 },
  ];
  const ring = RING_NODES.map(n => n.id); // already sorted lexicographically

  const ringNodes = new Map();
  for (const { id, priority } of RING_NODES) {
    const n = new RingElection(id, priority, ring);
    ringNodes.set(id, n);
    n.on('leader-elected', (leaderId) => {
      info(`  ${id} acknowledges ring leader: ${leaderId}`);
    });
  }

  // Wire send → deliver to successor
  for (const [, node] of ringNodes) {
    node.onSend = (msg, toId) => {
      info(`  Ring msg [${msg.type.slice(5)}] ${msg.candidateId} via ${node.nodeId} → ${toId}  (hops=${msg.hops})`);
      ringNodes.get(toId)?.receive(msg);
    };
  }

  // Solapur (lowest priority) initiates
  sub('Solapur initiates Ring election');
  ringNodes.get('apmc-solapur').initiate();

  const ringWinner = ringNodes.get('apmc-pune').getCurrentLeader();
  ok(`Ring election winner: ${ringWinner}`);
  line('');
  line('  DS concept: Ring election; O(N) message complexity; no central coordinator');
}

// ──────────────────────────────────────────────────────────────────────────────
// SECTION 3: Beacon Protocol
// ──────────────────────────────────────────────────────────────────────────────
function demoBeacon() {
  head('3. BEACON PROTOCOL');
  info('Leader broadcasts BEACON frames carrying Lamport time + physical clock.');
  info('Followers update failure-suspicion timers and apply NTP-style offset.');
  line('');

  const leader   = new BeaconManager('apmc-pune',   ['apmc-nashik', 'apmc-mumbai'], 200, 600);
  const follower1 = new BeaconManager('apmc-nashik', ['apmc-pune'],                 200, 600);
  const follower2 = new BeaconManager('apmc-mumbai', ['apmc-pune'],                 200, 600);

  leader.on('became-leader', epoch => info(`Pune became leader (epoch=${epoch})`));
  follower1.on('beacon-received', frame => info(`Nashik received BEACON  epoch=${frame.epoch}  L=${frame.lamportTime}  load=${frame.load}%`));
  follower2.on('beacon-received', frame => info(`Mumbai received BEACON  epoch=${frame.epoch}  L=${frame.lamportTime}  load=${frame.load}%`));
  follower1.on('leader-suspected', id => line(`  ⚠ Nashik SUSPECTS leader ${id} — would trigger new election`));

  // Wire broadcast
  leader.onBroadcastBeacon = (frame) => {
    follower1.receiveBeacon(frame);
    follower2.receiveBeacon(frame);
  };

  // Pune wins election → starts beaconing (epoch 1)
  leader.becomeLeader(1);
  follower1.becomeFollower('apmc-pune', 1);
  follower2.becomeFollower('apmc-pune', 1);

  // Send 3 manual beacons synchronously for the demo
  for (let i = 0; i < 3; i++) {
    leader['_broadcastBeacon']?.();
    // If private, trigger via internal timer not available; simulate via onBroadcastBeacon
    if (leader.onBroadcastBeacon) {
      // Already wired above; trigger via direct internal method if accessible
    }
  }

  const lStats = leader.getStats();
  const f1Stats = follower1.getStats();
  line('');
  info(`Leader stats:   sent=${lStats.beaconsSent}  epoch=${lStats.epoch}  role=${lStats.role}`);
  info(`Nashik stats:   received=${f1Stats.beaconsReceived}  offset=${f1Stats.clockOffset} ms  role=${f1Stats.role}`);
  ok('Beacon protocol combines liveness detection + lightweight time sync');

  leader.shutdown();
  follower1.shutdown();
  follower2.shutdown();

  line('');
  line('  DS concept: beacon protocol; failure detection; NTP-style clock correction');
}

// ──────────────────────────────────────────────────────────────────────────────
// SECTION 4: Ricart–Agrawala Mutual Exclusion
// ──────────────────────────────────────────────────────────────────────────────
function demoRicartAgrawala() {
  head('4. RICART–AGRAWALA MUTUAL EXCLUSION');
  info('3 mandi nodes compete for the settlement ledger lock (critical section).');
  info('Each node broadcasts REQUEST(ts, id) to peers; entry after N-1 REPLYs.');
  line('');

  const PEERS_OF = (id, all) => all.filter(x => x !== id);
  const ALL = ['apmc-pune', 'apmc-nashik', 'apmc-mumbai'];

  // Safety guard: at most ONE node may be inside the CS at any instant.
  // `holder` is cleared BEFORE releaseCS() because a release synchronously
  // grants the CS to the next waiter.
  let holder = null, violations = 0;
  const grantOrder = [];
  const enter = (id) => {
    if (holder !== null) {
      violations++;
      line(`  ✗ MUTUAL EXCLUSION VIOLATED: ${id} entered while ${holder} holds the CS`);
    } else {
      ok(`${id} ENTERED critical section (settlement ledger locked)`);
    }
    holder = id;
    grantOrder.push(id);
  };

  const rNodes = new Map();
  for (const id of ALL) {
    const n = new RicartAgrawalaMutex(id, PEERS_OF(id, ALL), 'settlement:farmer:F001');
    rNodes.set(id, n);
    n.onEnterCS = () => enter(id);
  }

  // Wire
  for (const [, node] of rNodes) {
    node.onSend = (msg, to) => {
      info(`  RA [${msg.type.padEnd(7)}] ${msg.from} → ${to}  ts=${msg.timestamp}`);
      rNodes.get(to)?.receive(msg);
    };
  }

  sub('All 3 nodes request CS simultaneously');
  rNodes.get('apmc-pune').requestCS();
  rNodes.get('apmc-nashik').requestCS();
  rNodes.get('apmc-mumbai').requestCS();

  // Release in Lamport order; each release hands the CS to the next waiter
  sub('Releases — each release sends a deferred REPLY to EVERY waiter');
  for (const id of ['apmc-pune', 'apmc-nashik', 'apmc-mumbai']) {
    holder = null;                      // leave the CS before granting
    rNodes.get(id).releaseCS();
  }

  line('');
  let total = 0;
  for (const [, n] of rNodes) {
    const s = n.getStats();
    total += s.messagesSent;
    info(`${s.nodeId.padEnd(16)} enters=${s.enterCount}  sent=${s.messagesSent}  rcvd=${s.messagesReceived}`);
  }
  line('');
  ok(`Mutual exclusion held: ${violations} violation(s)`);
  ok(`Grant order: ${grantOrder.join(' → ')}  (Lamport-timestamp order)`);
  ok(`Each CS entry costs 2(N-1) = ${2 * (ALL.length - 1)} messages — total ${total}`);
  line('');
  line('  DS concept: timestamp-based mutex; 2(N-1) messages; no coordinator');

  return { total, violations, grantOrder };
}

// ──────────────────────────────────────────────────────────────────────────────
// SECTION 5: Lodha–Kshemkalyani Fair Mutual Exclusion
// ──────────────────────────────────────────────────────────────────────────────
function demoLodhaKshemkalyani(raResult) {
  head('5. LODHA–KSHEMKALYANI FAIR MUTUAL EXCLUSION (Self-Study)');
  info('Same 3-node scenario.  LK adds a Local Request Queue (LRQ) + FLUSH.');
  info('Deferring a REQUEST costs 0 messages (the requester is parked in the LRQ).');
  info('On release, ONE FLUSH hands the CS to the LRQ head — it enters at once.');
  line('');

  const PEERS_OF = (id, all) => all.filter(x => x !== id);
  const ALL = ['apmc-pune', 'apmc-nashik', 'apmc-mumbai'];

  // Same safety guard as Ricart–Agrawala, so the two are directly comparable.
  let holder = null, violations = 0;
  const grantOrder = [];
  const enter = (id) => {
    if (holder !== null) {
      violations++;
      line(`  ✗ MUTUAL EXCLUSION VIOLATED: ${id} entered while ${holder} holds the CS`);
    } else {
      ok(`${id} ENTERED critical section (LK fairness)`);
    }
    holder = id;
    grantOrder.push(id);
  };

  const lkNodes = new Map();
  for (const id of ALL) {
    const n = new LodhaKshemkalyaniMutex(id, PEERS_OF(id, ALL), 'settlement:farmer:F001');
    lkNodes.set(id, n);
    n.onEnterCS = () => enter(id);
  }

  for (const [, node] of lkNodes) {
    node.onSend = (msg, to) => {
      info(`  LK [${msg.type.padEnd(7)}] ${msg.from} → ${to}  ts=${msg.timestamp}`);
      lkNodes.get(to)?.receive(msg);
    };
  }

  sub('All 3 nodes request CS simultaneously');
  lkNodes.get('apmc-pune').requestCS();
  lkNodes.get('apmc-nashik').requestCS();
  lkNodes.get('apmc-mumbai').requestCS();

  sub('LRQ state after contention (who is parked where, 0 messages spent)');
  for (const [id, n] of lkNodes) {
    const q = n.getLRQ().map(r => `${r.from}@ts${r.timestamp}`).join(', ') || '(empty)';
    info(`${id.padEnd(16)} state=${n.getState().padEnd(8)} LRQ=[${q}]`);
  }

  sub('Releases — each release sends exactly ONE FLUSH handoff');
  for (const id of ['apmc-pune', 'apmc-nashik', 'apmc-mumbai']) {
    holder = null;                      // leave the CS before handing over
    lkNodes.get(id).releaseCS();
  }

  line('');
  let totalLK = 0, flushEntries = 0, flushHandoffs = 0;
  for (const [, n] of lkNodes) {
    const s = n.getStats();
    totalLK += s.messagesSent;
    flushEntries += s.flushEntries;
    flushHandoffs += s.flushHandoffs;
    info(`${s.nodeId.padEnd(16)} enters=${s.enterCount}  sent=${s.messagesSent}  rcvd=${s.messagesReceived}  viaFLUSH=${s.flushEntries}`);
  }

  const totalRA = raResult ? raResult.total : null;
  line('');
  ok(`Mutual exclusion held: ${violations} violation(s)`);
  ok(`Grant order: ${grantOrder.join(' → ')}  (strict Lamport order — no starvation)`);
  ok(`${flushHandoffs} FLUSH handoff(s) replaced ${flushHandoffs} fan-outs of N-1 = ${ALL.length - 1} deferred REPLYs`);
  ok(`${flushEntries} of ${grantOrder.length} entries were granted by FLUSH (no REPLY round needed)`);
  if (totalRA !== null) {
    const delta = totalRA - totalLK;
    ok(`Messages: LK ${totalLK} vs Ricart–Agrawala ${totalRA}  →  ${delta >= 0 ? 'saved ' + delta : 'cost ' + -delta}`);
    info(`Saving grows with N: RA sends N-1 deferred REPLYs per exit, LK sends 1 FLUSH.`);
  }
  line('');
  line('  DS concept: fair mutex; silent deferral + FLUSH handoff; Local Request');
  line('              Queue; N-1 ≤ msgs ≤ 2(N-1); strict Lamport fairness');

  return { total: totalLK, violations, grantOrder };
}

// ──────────────────────────────────────────────────────────────────────────────
// SECTION 6: Knapp's 4-category Deadlock Detection
// ──────────────────────────────────────────────────────────────────────────────
function demoKnapp() {
  head('6. KNAPP\'S CLASSIFICATION — 4 Deadlock Detection Algorithms (Self-Study)');
  info('Scenario: Buyer B1 holds WALLET lock, waits for LOT lock.');
  info('          Farmer F1 holds LOT lock, waits for WALLET lock.');
  info('          → Deadlock cycle: B1 → F1 → B1');
  line('');

  // Build the WFG
  const wfgSite1 = new LocalWFG('site-1'); // Buyer side
  const wfgSite2 = new LocalWFG('site-2'); // Farmer side
  wfgSite1.addEdge('B1', 'F1', 'settlement:lot:L001');   // B1 waits for F1 (LOT)
  wfgSite2.addEdge('F1', 'B1', 'settlement:wallet:W001'); // F1 waits for B1 (WALLET)
  // Extra waiter to make it more interesting
  wfgSite1.addEdge('B2', 'F1', 'settlement:lot:L002');   // B2 also waits for F1 (no deadlock by itself)

  const processToSite = { B1: 'site-1', B2: 'site-1', F1: 'site-2' };

  // ─── Category 1: Path Pushing ─────────────────────────────
  sub('Category 1: PATH PUSHING (Obermarck)');
  info('Site-1 exports TWF paths crossing to site-2; site-2 checks for loop-back');

  const pp1 = new PathPushingDetector('site-1', wfgSite1);
  const pp2 = new PathPushingDetector('site-2', wfgSite2);
  let ppDeadlock = false;

  pp1.onExportPath = (path, toSite) => {
    info(`  Path-Push export: [${path.path.join(' → ')}] from ${path.fromSite} to ${toSite}`);
    pp2.receivePath(path);
  };
  pp2.onDeadlockDetected = (cycle) => {
    ppDeadlock = true;
    ok(`Path Pushing: *** DEADLOCK DETECTED *** cycle: [${cycle.join(' → ')}]`);
  };
  pp2.onExportPath = (path, toSite) => {
    info(`  Path-Push export: [${path.path.join(' → ')}] from ${path.fromSite} to ${toSite}`);
    pp1.receivePath(path);
  };
  pp1.onDeadlockDetected = (cycle) => {
    ppDeadlock = true;
    ok(`Path Pushing: *** DEADLOCK DETECTED *** cycle: [${cycle.join(' → ')}]`);
  };

  pp1.exportPaths(['site-2'], processToSite);
  pp2.exportPaths(['site-1'], processToSite);
  if (!ppDeadlock) line('  (No loop-back detected by path pushing in this simplified simulation)');

  // ─── Category 2: Edge Chasing (CMH) ───────────────────────
  sub('Category 2: EDGE CHASING — Chandy–Misra–Haas probe');
  info('B1 initiates probe [B1, B1, F1]; F1 forwards it to B1; B1 detects return');

  // Combine WFG for CMH (processes know both sites' edges in this simulation)
  const combinedWFG = new LocalWFG('combined');
  combinedWFG.addEdge('B1', 'F1', 'lot');
  combinedWFG.addEdge('F1', 'B1', 'wallet');

  const cmhB1 = new EdgeChasingDetector('B1', combinedWFG);
  const cmhF1 = new EdgeChasingDetector('F1', combinedWFG);
  const cmhNodes = new Map([['B1', cmhB1], ['F1', cmhF1]]);

  cmhB1.onSendProbe = (probe, to) => {
    info(`  CMH probe [${probe.initiator}, ${probe.sender}, ${probe.receiver}] → ${to}`);
    cmhNodes.get(to)?.receiveProbe(probe);
  };
  cmhF1.onSendProbe = (probe, to) => {
    info(`  CMH probe [${probe.initiator}, ${probe.sender}, ${probe.receiver}] → ${to}`);
    cmhNodes.get(to)?.receiveProbe(probe);
  };
  cmhB1.onDeadlockDetected = (msg, probe) => ok(`CMH: *** DEADLOCK DETECTED *** initiator=${probe.initiator}`);

  cmhB1.initiateProbe();

  // ─── Category 3: Diffusing Computation (ECHO) ─────────────
  sub('Category 3: DIFFUSING COMPUTATION (ECHO tree)');
  info('B1 sends query to F1; F1 sends query to B1; B1 (initiator) detects cycle');

  const echoB1 = new DiffusingComputationDetector('B1', combinedWFG);
  const echoF1 = new DiffusingComputationDetector('F1', combinedWFG);
  const echoNodes = new Map([['B1', echoB1], ['F1', echoF1]]);

  echoB1.onSendQuery = (q, to) => { info(`  ECHO query   B1→F1  queryId=${q.queryId}`); echoNodes.get(to)?.receiveQuery(q); };
  echoF1.onSendQuery = (q, to) => { info(`  ECHO query   F1→B1  queryId=${q.queryId}`); echoNodes.get(to)?.receiveQuery(q); };
  echoB1.onSendReply = (r, to) => { info(`  ECHO reply   B1→??  blocked=${r.isBlocked}`); echoNodes.get(to)?.receiveReply(r); };
  echoF1.onSendReply = (r, to) => { info(`  ECHO reply   F1→B1  blocked=${r.isBlocked}`); echoNodes.get(to)?.receiveReply(r); };
  echoB1.onDeadlockDetected = (qid, init) => ok(`ECHO: *** DEADLOCK DETECTED *** queryId=${qid} initiator=${init}`);

  echoB1.initiateQuery('q-001');

  // ─── Category 4: Global State Detection ───────────────────
  sub('Category 4: GLOBAL STATE DETECTION (snapshot WFG + DFS)');
  info('Combine local WFGs from Chandy–Lamport snapshot; run DFS cycle detection');

  const gsDetector = new GlobalStateDeadlockDetector();
  gsDetector.addSiteEdges(wfgSite1.getEdges());
  gsDetector.addSiteEdges(wfgSite2.getEdges());
  const cycles = gsDetector.detectCycles();

  if (cycles.length > 0) {
    ok(`Global State: *** DEADLOCK DETECTED *** cycles: ${cycles.map(c => '[' + c.join('→') + ']').join(', ')}`);
  } else {
    line('  Global State: No deadlock found');
  }

  // Summary
  line('');
  line('  Knapp\'s taxonomy summary:');
  for (const [key, cat] of Object.entries(KnappCategories)) {
    info(`  [${key.replace('_', ' ').padEnd(23)}] ${cat.name}`);
    info(`    mechanism: ${cat.mechanism}`);
    info(`    messages:  ${cat.messageComplexity}`);
  }
  line('');
  line('  DS concept: Knapp (1987) classification of 4 orthogonal DDD strategies.');
}

// ──────────────────────────────────────────────────────────────────────────────
// MAIN
// ──────────────────────────────────────────────────────────────────────────────
(async () => {
  line('\n=== FA-2 Unit III: Election + Beacon + Mutex + Deadlock Detection ===');
  line('    (packages/p2p/src/ — pure-logic, no infrastructure needed)');

  await demoBully();
  demoRing();
  demoBeacon();
  const raResult = demoRicartAgrawala();
  demoLodhaKshemkalyani(raResult);
  demoKnapp();

  line('');
  line('='.repeat(65));
  line('Unit III §4 §5 §6 — ALL DEMONSTRATIONS COMPLETE');
  line('Algorithms: Bully, Ring, Beacon, Ricart–Agrawala, Lodha–Kshemkalyani,');
  line('            Knapp 4-category Deadlock Detection');
  line('='.repeat(65));
})().catch(e => { console.error('DEMO FAILED:', e); process.exit(1); });
