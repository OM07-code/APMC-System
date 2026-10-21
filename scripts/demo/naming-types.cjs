// FA-1 §4.1 — NAMING: flat, structured, and attribute-based naming
//
// Self-contained: needs NO infrastructure. Run with:  npm run demo:naming
//
// Demonstrates the three naming schemes a distributed system uses to turn a name into
// an address, plus why each exists:
//   FLAT       — location-independent hash IDs, routed by XOR distance (DHT-style).
//                Good: entity can move freely. Bad: unreadable, needs a lookup overlay.
//   STRUCTURED — hierarchical, DNS-like ("auction.pune.apmc.network"). Human-readable,
//                delegable per zone, resolvable by record type (A / SRV / TXT).
//   ATTRIBUTE  — directory/LDAP-style. You don't know the name, you know the PROPERTIES:
//                "find every active auctioneer node in Pune serving WHEAT".

const { FlatNamingService, StructuredNamingService, AttributeNamingService } = require('@apmc/naming');

const line = (s) => console.log(s);

(async () => {
  line('=== FA-1 §4.1  Naming (packages/naming) ===\n');

  // ------------------------------------------------------------------ 1. FLAT
  line('--- 1. FLAT naming (hash-based, location-independent) ---');
  const selfId = FlatNamingService.generateFlatName('apmc-pune');
  const flat = new FlatNamingService(selfId);
  line(`  local node id                : ${selfId}`);

  const wheatName = FlatNamingService.generateFlatName('auction:WHEAT:PUN:2026-09-02');
  line(`  flat name for a WHEAT auction : ${wheatName}`);
  line('  ^ derived purely from content — carries NO location, so the entity can migrate');
  line('    between mandis without its name changing.\n');

  await flat.put(wheatName, Buffer.from(JSON.stringify({ price: 2450, lot: 'L-77' })));
  const resolved = await flat.get(wheatName);
  line(`  get(${wheatName.slice(0, 12)}…) -> ${resolved.toString()}`);

  // Routing: which nodes are responsible for this flat name?
  ['apmc-nashik', 'apmc-mumbai', 'apmc-nagpur', 'apmc-solapur'].forEach((m) => {
    flat.updateNode({ id: FlatNamingService.generateFlatName(m), host: 'localhost', port: 50054, lastSeen: Date.now() });
  });
  const closest = flat.findClosestNodes(wheatName, 3);
  line(`  closest nodes to that name  : ${closest.map((n) => n.id.slice(0, 10) + '…').join(', ')}`);
  line(`  XOR distance to nearest     : ${FlatNamingService.xorDistance(closest[0].id, wheatName).toString(16).slice(0, 20)}…`);
  line('  ^ resolution needs an overlay walk — the price of location independence.\n');

  // ------------------------------------------------------- 2. STRUCTURED
  line('--- 2. STRUCTURED naming (hierarchical, DNS-like) ---');
  const dns = new StructuredNamingService();
  const ZONE = 'apmc.network';

  // A records: name -> host address
  dns.addRecord(ZONE, { name: 'auction.pune.apmc.network', type: 'A', value: '10.0.1.11', ttl: 300 });
  dns.addRecord(ZONE, { name: 'auction.pune.apmc.network', type: 'A', value: '10.0.1.12', ttl: 300 });
  dns.addRecord(ZONE, { name: 'auction.nashik.apmc.network', type: 'A', value: '10.0.2.11', ttl: 300 });
  dns.addRecord(ZONE, { name: 'gateway.apmc.network', type: 'A', value: '10.0.0.8', ttl: 300 });
  dns.addRecord(ZONE, { name: 'signaling.apmc.network', type: 'TXT', value: 'ws://10.0.0.9:51052', ttl: 60 });

  // SRV records: service discovery with priority + weight (failover + load-balance)
  dns.addRecord(ZONE, { name: '_auction._grpc.apmc.network', type: 'SRV', value: 'auction.pune.apmc.network', ttl: 300, priority: 10, weight: 60, port: 50051 });
  dns.addRecord(ZONE, { name: '_auction._grpc.apmc.network', type: 'SRV', value: 'auction.nashik.apmc.network', ttl: 300, priority: 10, weight: 40, port: 50051 });
  dns.addRecord(ZONE, { name: '_auction._grpc.apmc.network', type: 'SRV', value: 'auction.backup.apmc.network', ttl: 300, priority: 20, weight: 100, port: 50051 });

  const parsed = StructuredNamingService.parse('auction.pune.apmc.network');
  line(`  parse("auction.pune.apmc.network") -> labels=[${parsed.labels.join(', ')}]  zone="${parsed.zone}"`);
  line(`  ^ the hierarchy is the delegation boundary: whoever owns "${parsed.zone}" can`);
  line('    delegate "pune." to the Pune mandi without a central registry rewrite.\n');

  line(`  resolve A   auction.pune.apmc.network   -> ${JSON.stringify(await dns.resolve('auction.pune.apmc.network', 'A'))}`);
  line(`  resolve A   auction.nashik.apmc.network -> ${JSON.stringify(await dns.resolve('auction.nashik.apmc.network', 'A'))}`);
  line(`  resolve A   gateway.apmc.network        -> ${JSON.stringify(await dns.resolve('gateway.apmc.network', 'A'))}`);
  line(`  resolve TXT signaling.apmc.network      -> ${JSON.stringify(await dns.resolve('signaling.apmc.network', 'TXT'))}`);
  line(`  resolve A   nosuch.apmc.network         -> ${JSON.stringify(await dns.resolve('nosuch.apmc.network', 'A'))}  (empty = NXDOMAIN)`);

  line('\n  SRV lookup for the auction gRPC service (sorted by priority, then weight):');
  for (const srv of await dns.resolveSRV('auction', 'grpc', 'apmc.network')) {
    line(`    prio=${srv.priority} weight=${srv.weight} -> ${srv.target}:${srv.port}`);
  }
  line('  ^ prio 10 entries are the live pair (60/40 split of traffic); prio 20 is the');
  line('    failover target, only used when both prio-10 nodes are unreachable.');
  line(`  zones served: ${JSON.stringify(dns.listZones())}\n`);

  // -------------------------------------------------------- 3. ATTRIBUTE
  line('--- 3. ATTRIBUTE-based naming (directory service) ---');
  const dir = new AttributeNamingService();

  const puneAuction = dir.register(
    { service: 'auction', mandi: 'PUN', commodity: 'WHEAT', status: 'active', transport: 'grpc' },
    { endpoint: '10.0.1.11:50051' }
  );
  dir.register(
    { service: 'auction', mandi: 'NSK', commodity: 'ONION', status: 'active', transport: 'grpc' },
    { endpoint: '10.0.2.11:50051' }
  );
  dir.register(
    { service: 'auction', mandi: 'PUN', commodity: 'ONION', status: 'draining', transport: 'grpc' },
    { endpoint: '10.0.1.13:50051' }
  );
  dir.register(
    { service: 'signaling', mandi: 'PUN', commodity: 'ANY', status: 'active', transport: 'websocket' },
    { endpoint: '10.0.0.9:51052' }
  );

  line(`  indexed attribute keys: ${JSON.stringify(dir.getAttributeKeys())}`);
  line(`  values for "mandi"    : ${JSON.stringify(dir.getAttributeValues('mandi'))}\n`);

  const show = (label, q) => {
    const hits = dir.query(q);
    line(`  query ${label}`);
    line(`    ${JSON.stringify(q)}`);
    hits.forEach((h) =>
      line(`    -> ${h.attributes.service}/${h.attributes.mandi}/${h.attributes.commodity} @ ${h.metadata.endpoint} [${h.attributes.status}]`)
    );
    if (hits.length === 0) line('    -> (no match)');
  };

  show('by one attribute :', { service: 'auction' });
  show('intersection     :', { service: 'auction', mandi: 'PUN', status: 'active' });
  show('non-name lookup  :', { transport: 'websocket' });
  show('unsatisfiable    :', { service: 'auction', mandi: 'MUM' });

  line('\n  update: mark the Pune WHEAT node as draining, then re-query active PUN nodes');
  dir.update(puneAuction, { status: 'draining' });
  show('after update     :', { service: 'auction', mandi: 'PUN', status: 'active' });
  line('  ^ the caller never knew a NAME — it described what it needed. This is how');
  line('    service discovery survives nodes appearing, draining, and disappearing.');

  line('\n=== DS concept shown: three naming schemes and their trade-offs —');
  line('    flat (location-independent, overlay-routed), structured (readable, delegable,');
  line('    SRV-based failover), attribute (query by property, dynamic discovery). ===');

  // Importing @apmc/naming pulls in grpc-server's module-level InMemoryNamingService,
  // which owns a background reaper timer. Exit explicitly so this script always ends.
  process.exit(0);
})().catch((e) => {
  console.error('DEMO FAILED:', e);
  process.exit(1);
});
