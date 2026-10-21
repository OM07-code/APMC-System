// FA-1 §3.4 / §4.1 — P2P OVERLAY: Kademlia DHT (XOR distance, k-buckets, put/get)
//
// Self-contained: needs NO infrastructure. Run with:  npm run demo:dht
//
// Demonstrates a structured P2P overlay: decentralised key→value storage where the
// node responsible for a key is chosen by XOR distance in a 160-bit ID space, with
// no central coordinator. This is the "decentralized" in the project title — mandis
// (APMC markets) share price data peer-to-peer instead of via one master server.

const { KademliaDHT } = require('@apmc/p2p');
const { createHash } = require('crypto');

const line = (s) => console.log(s);
const idFor = (name) => createHash('sha256').update(name).digest('hex').slice(0, 40);

// The mandi nodes forming our overlay.
const MANDIS = ['apmc-pune', 'apmc-nashik', 'apmc-mumbai', 'apmc-nagpur', 'apmc-solapur', 'apmc-kolhapur'];

(async () => {
  line('=== FA-1 §3.4  P2P Overlay: Kademlia DHT (packages/p2p) ===\n');

  // 1. Build the overlay from the perspective of the Pune node.
  const self = 'apmc-pune';
  const dht = new KademliaDHT(idFor(self), 'localhost', 50053);
  line(`self node : ${self}`);
  line(`self id   : ${dht.getNodeId()}  (160-bit / 40 hex chars)\n`);

  line('--- 1. Bootstrap: join the overlay by learning peer nodes ---');
  const peers = MANDIS.filter((m) => m !== self).map((name) => ({
    id: idFor(name),
    host: 'localhost',
    port: 50053,
    lastSeen: Date.now(),
    isSelf: false,
  }));
  await dht.bootstrap(peers);

  MANDIS.filter((m) => m !== self).forEach((name) => {
    const d = KademliaDHT.distance(dht.getNodeId(), idFor(name));
    line(`  ${name.padEnd(14)} id=${idFor(name).slice(0, 16)}…  XOR-distance=${d.toString(16).slice(0, 16)}…`);
  });

  const stats = dht.getRoutingTableStats();
  line(`\n  routing table: ${stats.totalNodes} nodes across ${stats.bucketsUsed} k-buckets`);
  line('  ^ nodes are bucketed by XOR distance — near peers get fine-grained buckets,');
  line('    far peers are grouped coarsely. That is what gives Kademlia O(log N) lookup.\n');

  // 2. Routing: who is responsible for a given key?
  line('--- 2. Lookup routing: which nodes are "closest" to a key? ---');
  for (const key of ['price:WHEAT:A', 'price:ONION:B']) {
    const keyId = idFor(key);
    const closest = dht.findClosestNodes(keyId, 3);
    line(`  key "${key}"`);
    line(`    key id  : ${keyId.slice(0, 24)}…`);
    const names = closest.map((n) => {
      const match = MANDIS.find((m) => idFor(m) === n.id);
      return match || n.id.slice(0, 10);
    });
    line(`    closest : ${names.join(' -> ')}`);
  }
  line('  ^ deterministic and decentralised: every node computes the same answer with');
  line('    NO directory server. Replication would store the value on these k nodes.\n');

  // 3. Store / retrieve with versioning + TTL.
  line('--- 3. put() / get() with versioning ---');
  await dht.put('price:WHEAT:A', Buffer.from(JSON.stringify({ price: 2450, mandi: 'PUN' })));
  let got = await dht.get('price:WHEAT:A');
  line(`  put  price:WHEAT:A = ${got.toString()}`);

  await dht.put('price:WHEAT:A', Buffer.from(JSON.stringify({ price: 2600, mandi: 'PUN' })));
  got = await dht.get('price:WHEAT:A');
  line(`  put  price:WHEAT:A = ${got.toString()}   (version incremented on overwrite)`);

  await dht.put('price:ONION:B', Buffer.from(JSON.stringify({ price: 1800, mandi: 'NSK' })));
  line(`  get  price:ONION:B = ${(await dht.get('price:ONION:B')).toString()}`);
  line(`  get  price:MISSING = ${await dht.get('price:MISSING')}   (absent key -> null)`);
  line(`\n  stored keys: ${JSON.stringify(dht.getStoredKeys())}\n`);

  // 4. TTL expiry — eventual consistency / soft state (BASE).
  line('--- 4. TTL expiry (soft state) ---');
  await dht.put('price:TOMATO:A', Buffer.from('perishable'), 300); // 300 ms TTL
  line(`  immediately     : ${(await dht.get('price:TOMATO:A'))?.toString()}`);
  await new Promise((r) => setTimeout(r, 400));
  line(`  after TTL (400ms): ${await dht.get('price:TOMATO:A')}   (expired, self-cleaning)`);

  line('\n=== DS concept shown: structured P2P overlay, XOR-distance routing, k-buckets,');
  line('    decentralised key-value storage with versioning + soft state (BASE). ===');
})().catch((e) => {
  console.error('DEMO FAILED:', e);
  process.exit(1);
});
