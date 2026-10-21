import { Server, ServerCredentials, sendUnaryData, ServerUnaryCall, ServerWritableStream, Metadata, loadPackageDefinition } from '@grpc/grpc-js';
import { loadSync } from '@grpc/proto-loader';
import { join } from 'path';
import { config } from '@apmc/config';
import { logger } from '@apmc/middleware';
import { peerManager, Peer, SignalMessage } from './peer-manager';
import { now, generateEventId } from '@apmc/shared';
import { AppError, NotFoundError, ValidationError } from '@apmc/shared';

const PROTO_PATH = join(__dirname, '..', '..', '..', 'proto', 'signaling.proto');

const packageDefinition = loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
});

// loadPackageDefinition is exported by @grpc/grpc-js (imported above), NOT by
// @grpc/proto-loader. The old `require('@grpc/proto-loader')` returned undefined,
// so this line threw `loadPackageDefinition is not a function` at import and
// crashed the signaling service on startup.
const proto = loadPackageDefinition(packageDefinition) as any;
const signalingService = proto.apmc.signaling.v1.SignalingService;

const SIGNAL_TYPE_MAP = {
  offer: 1,
  answer: 2,
  ice_candidate: 3,
  peer_joined: 4,
  peer_left: 5,
};

async function registerPeer(call: ServerUnaryCall<any, any>, callback: sendUnaryData<any>) {
  try {
    const { peer_id, peer_type, apmc_location, metadata } = call.request;
    
    if (!peer_type || !apmc_location) {
      throw new ValidationError('peer_type and apmc_location are required');
    }

    const peer = await peerManager.registerPeer({
      peer_type,
      apmc_location,
      user_id: metadata?.user_id,
      metadata: metadata || {},
    });

    callback(null, {
      success: true,
      message: 'Peer registered successfully',
    });
  } catch (error) {
    const appError = error instanceof AppError ? error : new AppError('INTERNAL_ERROR', 'Failed to register peer');
    logger.error({ err: error }, 'RegisterPeer failed');
    callback({
      code: appError.statusCode >= 500 ? 13 : 3,
      details: appError.message,
      metadata: new Metadata(),
    }, null);
  }
}

async function unregisterPeer(call: ServerUnaryCall<any, any>, callback: sendUnaryData<any>) {
  try {
    const { peer_id } = call.request;
    
    if (!peer_id) {
      throw new ValidationError('peer_id is required');
    }

    await peerManager.unregisterPeer(peer_id);

    callback(null, { success: true });
  } catch (error) {
    const appError = error instanceof AppError ? error : new AppError('INTERNAL_ERROR', 'Failed to unregister peer');
    logger.error({ err: error }, 'UnregisterPeer failed');
    callback({
      code: 13,
      details: appError.message,
      metadata: new Metadata(),
    }, null);
  }
}

async function createOffer(call: ServerUnaryCall<any, any>, callback: sendUnaryData<any>) {
  try {
    const { from_peer_id, to_peer_id, sdp, type } = call.request;
    
    if (!from_peer_id || !to_peer_id || !sdp || !type) {
      throw new ValidationError('Missing required fields');
    }

    const signal: SignalMessage = {
      from_peer_id,
      to_peer_id,
      type: 'offer',
      payload: { sdp, type },
      timestamp: now(),
    };

    await peerManager.deliverSignal(signal);

    callback(null, { success: true, message: 'Offer sent' });
  } catch (error) {
    const appError = error instanceof AppError ? error : new AppError('INTERNAL_ERROR', 'Failed to send offer');
    logger.error({ err: error }, 'CreateOffer failed');
    callback({
      code: 13,
      details: appError.message,
      metadata: new Metadata(),
    }, null);
  }
}

async function createAnswer(call: ServerUnaryCall<any, any>, callback: sendUnaryData<any>) {
  try {
    const { from_peer_id, to_peer_id, sdp, type } = call.request;
    
    if (!from_peer_id || !to_peer_id || !sdp || !type) {
      throw new ValidationError('Missing required fields');
    }

    const signal: SignalMessage = {
      from_peer_id,
      to_peer_id,
      type: 'answer',
      payload: { sdp, type },
      timestamp: now(),
    };

    await peerManager.deliverSignal(signal);

    callback(null, { success: true, message: 'Answer sent' });
  } catch (error) {
    const appError = error instanceof AppError ? error : new AppError('INTERNAL_ERROR', 'Failed to send answer');
    logger.error({ err: error }, 'CreateAnswer failed');
    callback({
      code: 13,
      details: appError.message,
      metadata: new Metadata(),
    }, null);
  }
}

async function addICECandidate(call: ServerUnaryCall<any, any>, callback: sendUnaryData<any>) {
  try {
    const { from_peer_id, to_peer_id, candidate, sdp_mid, sdp_mline_index } = call.request;
    
    if (!from_peer_id || !to_peer_id || !candidate) {
      throw new ValidationError('Missing required fields');
    }

    const signal: SignalMessage = {
      from_peer_id,
      to_peer_id,
      type: 'ice_candidate',
      payload: { candidate, sdp_mid, sdp_mline_index },
      timestamp: now(),
    };

    await peerManager.deliverSignal(signal);

    callback(null, { success: true, message: 'ICE candidate sent' });
  } catch (error) {
    const appError = error instanceof AppError ? error : new AppError('INTERNAL_ERROR', 'Failed to send ICE candidate');
    logger.error({ err: error }, 'AddICECandidate failed');
    callback({
      code: 13,
      details: appError.message,
      metadata: new Metadata(),
    }, null);
  }
}

async function getPeers(call: ServerUnaryCall<any, any>, callback: sendUnaryData<any>) {
  try {
    const { apmc_location, peer_type } = call.request;
    
    if (!apmc_location) {
      throw new ValidationError('apmc_location is required');
    }

    const peers = await peerManager.getOnlinePeers(apmc_location, peer_type);

    callback(null, {
      peers: peers.map(p => ({
        peer_id: p.peer_id,
        peer_type: p.peer_type,
        apmc_location: p.apmc_location,
        metadata: p.metadata,
        is_online: p.is_online,
        last_seen: p.last_seen.toString(),
      })),
    });
  } catch (error) {
    const appError = error instanceof AppError ? error : new AppError('INTERNAL_ERROR', 'Failed to get peers');
    logger.error({ err: error }, 'GetPeers failed');
    callback({
      code: 13,
      details: appError.message,
      metadata: new Metadata(),
    }, null);
  }
}

async function watchSignals(call: ServerWritableStream<any, any>) {
  const { peer_id } = call.request;
  
  if (!peer_id) {
    call.emit('error', { code: 3, details: 'peer_id is required' });
    return;
  }

  // Get queued signals
  const queuedSignals = await peerManager.getQueuedSignals(peer_id);
  
  for (const signal of queuedSignals) {
    call.write({
      from_peer_id: signal.from_peer_id,
      type: SIGNAL_TYPE_MAP[signal.type],
      payload: JSON.stringify(signal.payload),
      timestamp: signal.timestamp.toString(),
    });
  }

  // In a real implementation, this would use Redis pub/sub to listen for new signals
  // For now, we'll just keep the connection open
  const interval = setInterval(async () => {
    // Check for new signals
    const newSignals = await peerManager.getQueuedSignals(peer_id);
    for (const signal of newSignals) {
      call.write({
        from_peer_id: signal.from_peer_id,
        type: SIGNAL_TYPE_MAP[signal.type],
        payload: JSON.stringify(signal.payload),
        timestamp: signal.timestamp.toString(),
      });
    }
  }, 1000);

  call.on('cancelled', () => {
    clearInterval(interval);
  });
}

export function createSignalingServer(): Server {
  const server = new Server();
  
  server.addService(signalingService.service, {
    RegisterPeer: registerPeer,
    UnregisterPeer: unregisterPeer,
    CreateOffer: createOffer,
    CreateAnswer: createAnswer,
    AddICECandidate: addICECandidate,
    GetPeers: getPeers,
    WatchSignals: watchSignals,
  });

  return server;
}

export async function startSignalingServer(): Promise<Server> {
  const server = createSignalingServer();
  
  return new Promise((resolve, reject) => {
    server.bindAsync(
      `0.0.0.0:${config.SIGNALING_GRPC_PORT}`,
      ServerCredentials.createInsecure(),
      (err, port) => {
        if (err) {
          reject(err);
          return;
        }
        server.start();
        logger.info(`Signaling gRPC server started on port ${port}`);
        resolve(server);
      }
    );
  });
}