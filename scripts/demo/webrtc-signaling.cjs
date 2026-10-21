// FA-1 §3.3 — STREAM-ORIENTED COMMUNICATION: WebRTC signaling (SDP + ICE)
//
// REQUIRES the signaling service (and Redis) running:
//     npm run docker:up:core      # Redis
//     npm run dev:services        # starts signaling on ws://localhost:51052
// Then:  npm run demo:webrtc
//
// Two peers — an AUCTIONEER broadcasting the live auction floor and a BUYER watching —
// find each other and negotiate a direct connection through the signaling server:
//
//   auctioneer                signaling server                 buyer
//       |-- register --------------->|<---------- register -------|
//       |<-- peer_joined ------------|                            |
//       |-- get_peers -------------->|                            |
//       |-- offer  (SDP) ----------->|--- offer  (SDP) ---------->|
//       |<-- answer (SDP) -----------|<-- answer (SDP) -----------|
//       |-- ice_candidate ---------->|--- ice_candidate --------->|
//       |<-- ice_candidate ----------|<-- ice_candidate ----------|
//       |                                                         |
//       |========= MEDIA/DATA FLOWS PEER-TO-PEER (never via server) ========|
//
// The KEY DS point: the server only brokers *setup* metadata. Once SDP and ICE are
// exchanged, the audio/video stream goes directly between peers (via STUN/TURN when
// NAT requires a relay). That is what makes this stream-oriented rather than
// request-reply, and it is why the server's bandwidth cost does not grow with stream
// count the way a centralized media server's would.

const WebSocket = require('ws');

const WS_URL = process.env.SIGNALING_WS_URL || 'ws://localhost:51052';
const LOCATION = 'PUN';
const line = (s) => console.log(s);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// Minimal but realistic SDP / ICE payloads (shape matters, not the codec details).
const OFFER_SDP =
  'v=0\r\no=- 4611731400430051336 2 IN IP4 127.0.0.1\r\ns=APMC-auction-floor\r\n' +
  't=0 0\r\nm=video 9 UDP/TLS/RTP/SAVPF 96\r\na=rtpmap:96 VP8/90000\r\na=sendonly\r\n';
const ANSWER_SDP =
  'v=0\r\no=- 7614231400430051221 2 IN IP4 127.0.0.1\r\ns=APMC-buyer-view\r\n' +
  't=0 0\r\nm=video 9 UDP/TLS/RTP/SAVPF 96\r\na=rtpmap:96 VP8/90000\r\na=recvonly\r\n';

// A peer wrapper: connects, tracks its peer_id, and lets us await specific messages.
class DemoPeer {
  constructor(label, peerType) {
    this.label = label;
    this.peerType = peerType;
    this.peerId = null;
    this.iceServers = [];
    this.inbox = [];
    this.waiters = [];
  }

  connect() {
    return new Promise((resolve, reject) => {
      this.ws = new WebSocket(WS_URL);
      this.ws.on('open', resolve);
      this.ws.on('error', reject);
      this.ws.on('message', (raw) => {
        const msg = JSON.parse(raw.toString());
        line(`   [${this.label}] <- ${msg.type}`);

        // Hand the message to a waiter if one is pending, OTHERWISE buffer it.
        // It must not be both: buffering a message that already satisfied a waiter
        // makes the NEXT waitFor() of that type return the stale copy immediately,
        // which silently mismatches labels against payloads (e.g. the 2nd ICE
        // candidate reported as the 1st).
        const waiter = this.waiters.find((w) => w.type === msg.type);
        if (waiter) {
          this.waiters = this.waiters.filter((w) => w !== waiter);
          waiter.resolve(msg);
        } else {
          this.inbox.push(msg);
        }
      });
    });
  }

  send(type, payload) {
    line(`   [${this.label}] -> ${type}`);
    this.ws.send(JSON.stringify({ type, payload }));
  }

  // Wait for a message of a given type (checking already-received ones first).
  waitFor(type, timeoutMs = 5000) {
    const existing = this.inbox.find((m) => m.type === type);
    if (existing) {
      this.inbox.splice(this.inbox.indexOf(existing), 1);
      return Promise.resolve(existing);
    }
    return new Promise((resolve, reject) => {
      const timer = setTimeout(
        () => reject(new Error(`[${this.label}] timed out waiting for "${type}"`)),
        timeoutMs
      );
      this.waiters.push({
        type,
        resolve: (m) => {
          clearTimeout(timer);
          resolve(m);
        },
      });
    });
  }

  async register() {
    this.send('register', {
      peer_type: this.peerType,
      apmc_location: LOCATION,
      metadata: { display_name: this.label },
    });
    const reg = await this.waitFor('registered');
    this.peerId = reg.payload.peer_id;
    this.iceServers = reg.payload.ice_servers || [];
    return reg;
  }

  close() {
    if (this.ws) this.ws.close();
  }
}

(async () => {
  line('=== FA-1 §3.3  WebRTC signaling: stream-oriented communication ===\n');
  line(`signaling server: ${WS_URL}\n`);

  const auctioneer = new DemoPeer('AUCTIONEER', 'auctioneer');
  const buyer = new DemoPeer('BUYER', 'buyer');

  // ------------------------------------------------ 1. connect + register
  line('--- 1. Both peers connect and register with the signaling server ---');
  try {
    await auctioneer.connect();
  } catch (err) {
    line(`\nCannot reach ${WS_URL}.`);
    line('Start the stack first:  npm run docker:up:core  &&  npm run dev:services');
    process.exit(1);
  }
  await buyer.connect();

  await auctioneer.register();
  line(`   auctioneer peer_id = ${auctioneer.peerId}`);
  await buyer.register();
  line(`   buyer      peer_id = ${buyer.peerId}`);

  line('\n   ICE servers handed to the peers (how they traverse NAT):');
  for (const s of auctioneer.iceServers) {
    const kind = String(s.urls).startsWith('turn') ? 'TURN (relay — used when direct fails)' : 'STUN (discovers public address)';
    line(`     ${String(s.urls).padEnd(38)} ${kind}`);
  }
  if (auctioneer.iceServers.length === 0) line('     (none configured)');

  // The auctioneer should have been told a peer joined its location.
  line('\n--- 2. Location-scoped presence notification ---');
  const joined = await auctioneer.waitFor('peer_joined');
  line(`   auctioneer was notified that ${joined.payload.peer_type} ${joined.payload.peer_id} joined ${LOCATION}`);

  // ------------------------------------------------ 3. peer discovery
  line('\n--- 3. Peer discovery (who else is on this mandi floor?) ---');
  auctioneer.send('get_peers', {});
  const list = await auctioneer.waitFor('peer_list');
  line(`   ${list.payload.peers.length} peer(s) at ${LOCATION}:`);
  for (const p of list.payload.peers) {
    line(`     ${p.peer_type.padEnd(11)} ${p.peer_id}  online=${p.is_online}`);
  }

  // ------------------------------------------------ 4. SDP offer/answer
  line('\n--- 4. SDP offer/answer exchange (session negotiation) ---');
  auctioneer.send('offer', {
    to_peer_id: buyer.peerId,
    sdp: OFFER_SDP,
    type: 'offer',
  });

  const offer = await buyer.waitFor('offer');
  line(`   buyer received an offer from ${offer.payload.from_peer_id}`);
  line(`     SDP session name: ${(offer.payload.sdp.match(/^s=(.*)$/m) || [])[1]}`);
  line(`     direction       : ${offer.payload.sdp.includes('sendonly') ? 'sendonly (auctioneer publishes)' : '?'}`);
  if (offer.payload.from_peer_id !== auctioneer.peerId) {
    throw new Error('offer came from the wrong peer — signal routing is broken');
  }

  buyer.send('answer', {
    to_peer_id: auctioneer.peerId,
    sdp: ANSWER_SDP,
    type: 'answer',
  });
  const answer = await auctioneer.waitFor('answer');
  line(`   auctioneer received the answer from ${answer.payload.from_peer_id}`);
  line(`     direction       : ${answer.payload.sdp.includes('recvonly') ? 'recvonly (buyer subscribes)' : '?'}`);

  // ------------------------------------------------ 5. ICE candidates
  line('\n--- 5. ICE candidate trickling (finding a viable network path) ---');
  const candidates = [
    { candidate: 'candidate:1 1 UDP 2130706431 192.168.1.20 54321 typ host', desc: 'host      (same LAN)' },
    { candidate: 'candidate:2 1 UDP 1694498815 49.36.10.7 54322 typ srflx raddr 192.168.1.20 rport 54321', desc: 'srflx     (public addr via STUN)' },
    { candidate: 'candidate:3 1 UDP 41885439 10.0.0.9 54323 typ relay raddr 49.36.10.7 rport 54322', desc: 'relay     (via TURN, last resort)' },
  ];

  for (const c of candidates) {
    auctioneer.send('ice_candidate', {
      to_peer_id: buyer.peerId,
      candidate: c.candidate,
      sdp_mid: '0',
      sdp_mline_index: 0,
    });
    const got = await buyer.waitFor('ice_candidate');
    // Assert we got back the candidate we just sent, not a previously buffered one —
    // otherwise the labels below would describe the wrong payload.
    if (got.payload.candidate !== c.candidate) {
      throw new Error(`ICE candidate mismatch: expected "${c.candidate}", got "${got.payload.candidate}"`);
    }
    const [addr, port] = got.payload.candidate.split(' ').slice(4, 6);
    line(`   buyer got ${c.desc}  ->  ${addr}:${port}`);
  }

  // Reverse direction proves delivery is bidirectional, not just A->B.
  buyer.send('ice_candidate', {
    to_peer_id: auctioneer.peerId,
    candidate: 'candidate:1 1 UDP 2130706431 192.168.1.44 61000 typ host',
    sdp_mid: '0',
    sdp_mline_index: 0,
  });
  await auctioneer.waitFor('ice_candidate');
  line('   auctioneer got the buyer\'s candidate (delivery is bidirectional)');

  line('\n   ==> Negotiation complete. In a real client the RTCPeerConnection would now');
  line('       be CONNECTED and video would flow DIRECTLY between these two peers.');
  line('       The signaling server has done its whole job and carries no media.');

  // ------------------------------------------------ 6. offline queueing
  line('\n--- 6. Fault tolerance: signalling a peer that is not connected ---');
  auctioneer.send('offer', { to_peer_id: 'peer-does-not-exist', sdp: OFFER_SDP, type: 'offer' });
  const err = await auctioneer.waitFor('error');
  line(`   server replied: "${err.payload.message}"`);
  line('   ^ not dropped — persisted to Redis and replayed when that peer registers,');
  line('     so a late-joining buyer still receives the offer (at-least-once signalling).');

  // ------------------------------------------------ 7. disconnect
  line('\n--- 7. Presence teardown on disconnect ---');
  buyer.close();
  const left = await auctioneer.waitFor('peer_left', 8000);
  line(`   auctioneer notified: peer_left ${left.payload.peer_id}`);
  line('   ^ failure detection by connection liveness — the auction UI can drop the tile.');

  auctioneer.close();
  await sleep(200);

  line('\n=== DS concept shown: stream-oriented communication via WebRTC —');
  line('    SDP offer/answer negotiation, ICE/STUN/TURN NAT traversal, presence and');
  line('    peer discovery, queued (at-least-once) signal delivery, and peer-to-peer');
  line('    media that bypasses the server entirely. ===');
  process.exit(0);
})().catch((e) => {
  console.error('\nDEMO FAILED:', e.message);
  console.error('Is the signaling service running?  npm run dev:services');
  process.exit(1);
});
