// FA-1 §3.2 — MESSAGE-ORIENTED MIDDLEWARE: Kafka/Redpanda pub-sub, persistence, replay
//
// REQUIRES Redpanda running (memory-capped, safe):
//     npm run docker:up:core
// Then:  npm run demo:mom
//
// Demonstrates why an auction system uses a broker instead of direct RPC for events:
//
//   1. PUB/SUB DECOUPLING — the producer publishes and moves on. It does not know
//      who the subscribers are, how many there are, or whether any exist yet.
//   2. PERSISTENCE — messages are written to a durable, ordered, append-only log.
//      Unlike an RPC call, they survive a subscriber being offline.
//   3. REPLAY — a brand-new consumer group reads the log FROM THE BEGINNING and sees
//      history it was never online for. This is what makes the price feed auditable
//      and lets a crashed analytics service rebuild its state.
//   4. CONSUMER GROUPS — two groups each get their own independent cursor (offset),
//      so adding a subscriber never steals messages from an existing one.
//   5. AT-LEAST-ONCE — the offset is committed only after the handler succeeds. A
//      throwing handler means redelivery, not silent loss (see packages/broker).
//
// Contrast with §3.1 gRPC: RPC is synchronous and both sides must be up at once.
// MOM is asynchronous and time-decoupled — the defining trade-off of Unit 2.

// Host processes reach Redpanda on the EXTERNAL listener. `redpanda:9092` only resolves
// inside the Docker network (see docker-compose.yml advertised listeners).
process.env.KAFKA_BROKERS = process.env.KAFKA_BROKERS || 'localhost:19092';

// Quieten the narration so the demo's own output is readable. Set LOG_LEVEL=info to see
// the broker's real structured logs interleaved.
process.env.LOG_LEVEL = process.env.LOG_LEVEL || 'warn';
process.env.KAFKAJS_NO_PARTITIONER_WARNING = '1';

const { Kafka, logLevel } = require('kafkajs');
const { publishPriceUpdate, publishAuctionEvent, initializeBroker, shutdownBroker } = require('@apmc/broker');

const BROKERS = process.env.KAFKA_BROKERS.split(',');
const TOPIC = 'apmc.price.updates';
const line = (s) => console.log(s);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// A unique suffix so each run creates fresh consumer groups (and therefore replays
// the whole topic rather than resuming a previous run's committed offset).
const RUN = Date.now().toString().slice(-6);

const kafka = new Kafka({ clientId: `demo-mom-${RUN}`, brokers: BROKERS, logLevel: logLevel.NOTHING });

// Consume with a fresh group, from the start of the log, until we have seen `expected`
// messages belonging to `auctionId` (or we time out).
//
// The topic may already hold messages from earlier runs or from flow-test. We print
// those too — they are extra proof that the log is durable across process lifetimes —
// but only THIS run's messages count toward the expected total.
async function replayGroup(groupId, auctionId, expected, timeoutMs = 15000) {
  const consumer = kafka.consumer({ groupId, maxWaitTimeInMs: 100 });
  await consumer.connect();
  await consumer.subscribe({ topic: TOPIC, fromBeginning: true });

  const seen = [];
  let historical = 0;
  let done;
  const finished = new Promise((r) => (done = r));

  await consumer.run({
    eachMessage: async ({ partition, message }) => {
      let value;
      try {
        value = JSON.parse(message.value.toString());
      } catch {
        return; // ignore anything unparseable left in the log
      }

      const mine = value.auction_id === auctionId;
      if (mine) {
        seen.push({ offset: message.offset, partition, value });
      } else {
        historical++;
      }

      line(
        `     [${groupId}] offset=${String(message.offset).padStart(3)} p${partition} ` +
          `key=${message.key ? message.key.toString() : '-'}  ` +
          `${value.commodity}/${value.grade} @ ₹${value.price}` +
          (mine ? '   <-- published by this run' : '   (older run — still on disk)')
      );

      if (seen.length >= expected) done();
    },
  });

  await Promise.race([finished, sleep(timeoutMs)]);
  await consumer.disconnect();
  if (historical > 0) {
    line(`     [${groupId}] ...plus ${historical} message(s) from earlier runs, replayed from disk`);
  }
  return seen;
}

(async () => {
  line('=== FA-1 §3.2  Message-Oriented Middleware (Kafka / Redpanda) ===\n');
  line(`brokers: ${BROKERS.join(', ')}   topic: ${TOPIC}\n`);

  // ---------------------------------------------------------- reachability
  const admin = kafka.admin();
  try {
    await admin.connect();
  } catch (err) {
    line(`Cannot reach Redpanda at ${BROKERS.join(',')}.`);
    line('Start it first:  npm run docker:up:core');
    process.exit(1);
  }

  const topics = await admin.listTopics();
  line(`existing topics: ${JSON.stringify(topics.filter((t) => t.startsWith('apmc')))}\n`);

  // -------------------------------------------- 1. publish (fire and forget)
  line('--- 1. PUBLISH: producer emits price updates and does not wait for consumers ---');
  await initializeBroker();

  const prices = [2400, 2450, 2510, 2575, 2600];

  // One auction whose price climbs as bids land. All five updates share the same
  // auction_id, which packages/broker uses as the Kafka message KEY — so they all hash
  // to the same partition and their relative order is guaranteed (see section 4).
  const AUCTION_ID = `auc-demo-${RUN}`;

  for (let i = 0; i < prices.length; i++) {
    await publishPriceUpdate({
      auction_id: AUCTION_ID,
      commodity: 'WHEAT',
      grade: 'A',
      price: prices[i],
      timestamp: Date.now(),
      apmc_location: 'PUN',
      currency: 'INR',
      source: 1,
    });
    line(`   published price update #${i + 1}: WHEAT/A @ ₹${prices[i]}  (no subscriber online yet)`);
  }

  await publishAuctionEvent({
    event_id: `evt-demo-${RUN}`,
    auction_id: AUCTION_ID,
    type: 4, // AUCTION_ENDED
    timestamp: Date.now(),
    data: { winning_bid: '2600', note: 'demo' },
  });
  line('   published auction event: AUCTION_ENDED  (different topic, same broker)');
  line('\n   ^ Note what did NOT happen: the producer never blocked on a consumer, never');
  line('     learned a subscriber address, and never failed because nobody was listening.');
  line('     That is the decoupling an RPC call cannot give you.\n');

  await sleep(600); // let the log settle

  // ------------------------------------- 2. replay from the start of the log
  line('--- 2. PERSISTENCE + REPLAY: a NEW subscriber reads history it never saw live ---');
  const groupA = `demo-analytics-${RUN}`;
  const seenA = await replayGroup(groupA, AUCTION_ID, prices.length);
  line(`\n   group "${groupA}" replayed ${seenA.length} message(s) from offset 0.`);
  line('   ^ These were published BEFORE this consumer existed. The broker persisted them');
  line('     to an ordered append-only log on disk, so the subscriber caught up on history.');
  line('     An RPC-based design would have lost every one of them.\n');

  // ---------------------------------------- 3. independent consumer groups
  line('--- 3. CONSUMER GROUPS: a second group has its OWN offset cursor ---');
  const groupB = `demo-alerting-${RUN}`;
  const seenB = await replayGroup(groupB, AUCTION_ID, prices.length);
  line(`\n   group "${groupB}" independently replayed ${seenB.length} message(s).`);
  line('   ^ Both groups received the SAME messages. Adding a subscriber did not consume');
  line('     or steal them from the first — this is pub/sub fan-out, not a work queue.');
  line('     (Within one group, partitions are split across members = load balancing.)\n');

  // ---------------------------------------------- 4. offsets / ordering
  if (seenA.length > 0) {
    const offsets = seenA.map((m) => Number(m.offset));
    const ordered = offsets.every((o, i) => i === 0 || o > offsets[i - 1]);
    line('--- 4. ORDERING GUARANTEE ---');
    line(`   offsets received: [${offsets.join(', ')}]`);
    line(`   price sequence  : [${seenA.map((m) => m.value.price).join(' -> ')}]`);
    line(`   monotonically increasing within the partition: ${ordered}`);
    line('   ^ Kafka guarantees order WITHIN a partition, and the partition is chosen by');
    line('     the message key. packages/broker keys price updates by auction_id, so every');
    line('     update for one auction lands in the same partition and is consumed in the');
    line('     order it was published — a buyer can never see a later bid before an earlier');
    line('     one. Across DIFFERENT auctions there is no global order, and the system does');
    line('     not need one. Per-key order instead of total order is the scalability');
    line('     trade-off: partitions can then be consumed in parallel.\n');
  }

  // ---------------------------------------------- 5. at-least-once semantics
  line('--- 5. AT-LEAST-ONCE DELIVERY: a failing handler must NOT lose the message ---');
  const groupC = `demo-flaky-${RUN}`;
  let attempts = 0;

  const flaky = kafka.consumer({ groupId: groupC, maxWaitTimeInMs: 100 });
  await flaky.connect();
  await flaky.subscribe({ topic: TOPIC, fromBeginning: true });

  let firstOffset = null;
  let redelivered = false;
  let settle;
  const settled = new Promise((r) => (settle = r));

  await flaky.run({
    eachMessage: async ({ message }) => {
      attempts++;
      if (attempts === 1) {
        firstOffset = message.offset;
        line(`   attempt 1: handler THROWS on offset ${message.offset} (simulated DB outage)`);
        // Throwing prevents kafkajs from committing this offset. This mirrors the fix
        // in packages/broker/src/consumer.ts, which used to swallow handler errors and
        // therefore silently ACKed messages it had failed to process.
        throw new Error('simulated downstream failure');
      }
      if (message.offset === firstOffset && !redelivered) {
        redelivered = true;
        line(`   attempt ${attempts}: offset ${message.offset} was REDELIVERED and now succeeds`);
        settle();
      }
    },
  });

  await Promise.race([settled, sleep(12000)]);
  await flaky.disconnect();

  if (redelivered) {
    line('\n   ==> Uncommitted offset => Kafka redelivered the message. No silent data loss.');
  } else {
    line(`\n   (handler ran ${attempts} time(s); redelivery may need a rebalance to be observed)`);
  }
  line('   ^ Trade-off: at-least-once means a message can arrive TWICE, so handlers must');
  line('     be idempotent. Exactly-once would need transactions + far more coordination.\n');

  // ---------------------------------------------- wrap up
  const groups = await admin.listGroups();
  const mine = groups.groups.filter((g) => g.groupId.includes(RUN)).map((g) => g.groupId);
  line(`consumer groups created by this run: ${JSON.stringify(mine)}`);

  await admin.disconnect();
  await shutdownBroker().catch(() => {});

  line('\n=== DS concept shown: message-oriented (asynchronous, time-decoupled)');
  line('    communication — publish/subscribe fan-out, durable persistent log, replay of');
  line('    history by new subscribers, per-group offsets, partition ordering, and');
  line('    at-least-once delivery with redelivery on handler failure. ===');
  process.exit(0);
})().catch((e) => {
  console.error('\nDEMO FAILED:', e.message);
  console.error('Is Redpanda running?  npm run docker:up:core');
  process.exit(1);
});
