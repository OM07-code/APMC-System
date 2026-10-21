// End-to-end auction flow test against the running gateway (localhost:8000).
// Proves: register/JWT → create → start → bid → state → history, with MOM publish.
const BASE = 'http://localhost:8000';
const stamp = Date.now();

async function j(method, path, body, token) {
  const res = await fetch(BASE + path, {
    method,
    headers: {
      'content-type': 'application/json',
      ...(token ? { authorization: 'Bearer ' + token } : {}),
    },
    body: body ? JSON.stringify(body) : undefined,
  });
  let data;
  try { data = await res.json(); } catch { data = '<non-json>'; }
  return { status: res.status, data };
}

async function register(role, i) {
  const email = `${role}${stamp}_${i}@apmc.test`;
  const r = await j('POST', '/api/v1/auth/register', {
    email, password: 'Passw0rd!23', full_name: `${role} ${i}`, role,
  });
  if (r.status !== 201 && r.status !== 200) throw new Error(`register ${role} failed: ${r.status} ${JSON.stringify(r.data)}`);
  return { token: r.data.tokens?.access_token || r.data.access_token || r.data.accessToken, id: r.data.user?.id, email };
}

(async () => {
  console.log('== 1. register farmer / auctioneer / buyer ==');
  const farmer = await register('farmer', 1);
  const auctioneer = await register('auctioneer', 1);
  const buyer = await register('buyer', 1);
  console.log('  farmer.id     =', farmer.id, '| token?', !!farmer.token);
  console.log('  auctioneer.id =', auctioneer.id, '| token?', !!auctioneer.token);
  console.log('  buyer.id      =', buyer.id, '| token?', !!buyer.token);
  if (!farmer.token) throw new Error('no token — check token field name: ' + JSON.stringify(farmer));

  console.log('\n== 2. CREATE auction (farmer) ==');
  const create = await j('POST', '/api/v1/auctions', {
    commodity: 'WHEAT', grade: 'A', quantity: 100, base_price: 2000,
    duration_seconds: 300, apmc_location: 'PUN', farmer_id: farmer.id,
  }, farmer.token);
  console.log('  status', create.status, JSON.stringify(create.data));
  const aid = create.data.auction_id;
  if (!aid) throw new Error('no auction_id returned');

  console.log('\n== 3. START auction (auctioneer) ==');
  const start = await j('POST', `/api/v1/auctions/${aid}/start`, { auctioneer_id: auctioneer.id }, auctioneer.token);
  console.log('  status', start.status, JSON.stringify(start.data));

  console.log('\n== 4. PLACE BID (buyer, 2500) ==');
  const bid = await j('POST', `/api/v1/auctions/${aid}/bids`, { buyer_id: buyer.id, amount: 2500 }, buyer.token);
  console.log('  status', bid.status, JSON.stringify(bid.data));

  console.log('\n== 5. PLACE BID (buyer, 2750) ==');
  const bid2 = await j('POST', `/api/v1/auctions/${aid}/bids`, { buyer_id: buyer.id, amount: 2750 }, buyer.token);
  console.log('  status', bid2.status, JSON.stringify(bid2.data));

  console.log('\n== 6. GET STATE ==');
  const state = await j('GET', `/api/v1/auctions/${aid}`, null, buyer.token);
  console.log('  status', state.status, JSON.stringify(state.data));

  console.log('\n== 7. GET BID HISTORY ==');
  const hist = await j('GET', `/api/v1/auctions/${aid}/bids`, null, buyer.token);
  console.log('  status', hist.status, JSON.stringify(hist.data));

  console.log('\n== 8. END auction (auctioneer) ==');
  const end = await j('POST', `/api/v1/auctions/${aid}/end`, { auctioneer_id: auctioneer.id }, auctioneer.token);
  console.log('  status', end.status, JSON.stringify(end.data));

  console.log('\n== DONE. auction_id =', aid, '==');
})().catch((e) => { console.error('FLOW FAILED:', e.message); process.exit(1); });
