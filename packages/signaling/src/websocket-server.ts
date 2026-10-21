import { WebSocketServer, WebSocket } from 'ws';
import { createServer, Server as HttpServer } from 'http';
import { config } from '@apmc/config';
import { logger } from '@apmc/middleware';
import { peerManager, Peer } from './peer-manager';
import { now, generateEventId } from '@apmc/shared';

interface WSMessage {
  type: 'register' | 'offer' | 'answer' | 'ice_candidate' | 'get_peers' | 'ping' | 'pong';
  payload: any;
  request_id?: string;
}

interface WSResponse {
  type: 'registered' | 'offer' | 'answer' | 'ice_candidate' | 'peer_list' | 'peer_joined' | 'peer_left' | 'error' | 'pong';
  payload: any;
  request_id?: string;
}

const connectedClients = new Map<string, { ws: WebSocket; peer: Peer }>();

export class SignalingWebSocketServer {
  private wss: WebSocketServer | null = null;
  private httpServer: HttpServer | null = null;
  private cleanupInterval: NodeJS.Timeout | null = null;

  async start(port: number = config.SIGNALING_GRPC_PORT + 1000): Promise<void> {
    this.httpServer = createServer();
    this.wss = new WebSocketServer({ server: this.httpServer });

    this.wss.on('connection', (ws: WebSocket, req) => {
      this.handleConnection(ws, req);
    });

    this.wss.on('error', (error) => {
      logger.error({ err: error }, 'WebSocket server error');
    });

    return new Promise((resolve) => {
      this.httpServer!.listen(port, () => {
        logger.info(`Signaling WebSocket server started on port ${port}`);
        
        // Start cleanup interval
        this.cleanupInterval = setInterval(() => {
          this.cleanupStaleConnections();
        }, 30000);
        
        resolve();
      });
    });
  }

  async stop(): Promise<void> {
    if (this.cleanupInterval) {
      clearInterval(this.cleanupInterval);
    }
    
    // Close all connections
    for (const [socketId, client] of connectedClients) {
      client.ws.close(1001, 'Server shutting down');
    }
    connectedClients.clear();
    
    if (this.wss) {
      await new Promise<void>((resolve) => {
        this.wss!.close(() => resolve());
      });
    }
    
    if (this.httpServer) {
      await new Promise<void>((resolve) => {
        this.httpServer!.close(() => resolve());
      });
    }
    
    logger.info('Signaling WebSocket server stopped');
  }

  private handleConnection(ws: WebSocket, req: any): void {
    const socketId = req.socket.remoteAddress + ':' + req.socket.remotePort + ':' + Date.now();
    logger.debug({ socketId }, 'New WebSocket connection');
    
    let currentPeer: Peer | null = null;
    
    ws.on('message', async (data: Buffer) => {
      try {
        const message: WSMessage = JSON.parse(data.toString());
        currentPeer = await this.handleMessage(ws, socketId, message, currentPeer);
      } catch (error) {
        logger.error({ err: error, socketId }, 'Failed to parse WebSocket message');
        this.send(ws, { type: 'error', payload: { message: 'Invalid message format' } });
      }
    });
    
    ws.on('close', async (code, reason) => {
      logger.debug({ socketId, code, reason: reason.toString() }, 'WebSocket closed');
      
      if (currentPeer) {
        connectedClients.delete(socketId);
        await this.handlePeerDisconnect(currentPeer);
      }
    });
    
    ws.on('error', (error) => {
      logger.error({ err: error, socketId }, 'WebSocket error');
    });
    
    ws.on('pong', () => {
      // Heartbeat received
    });
  }

  private async handleMessage(
    ws: WebSocket, 
    socketId: string, 
    message: WSMessage, 
    currentPeer: Peer | null
  ): Promise<Peer | null> {
    switch (message.type) {
      case 'register': {
        const { peer_type, apmc_location, user_id, metadata } = message.payload;
        
        if (!peer_type || !apmc_location) {
          this.send(ws, { 
            type: 'error', 
            payload: { message: 'peer_type and apmc_location are required' },
            request_id: message.request_id 
          });
          return currentPeer;
        }
        
        // Unregister previous peer if any
        if (currentPeer) {
          await peerManager.unregisterPeer(currentPeer.peer_id);
          connectedClients.delete(socketId);
        }
        
        // Register new peer
        const peer = await peerManager.registerPeer({
          peer_type,
          apmc_location,
          user_id,
          metadata: metadata || {},
        });
        
        currentPeer = peer;
        connectedClients.set(socketId, { ws, peer });
        
        // Update socket ID
        await peerManager.updatePeerHeartbeat(peer.peer_id, socketId);
        
        // Send registration confirmation
        this.send(ws, {
          type: 'registered',
          payload: { peer_id: peer.peer_id, ice_servers: peerManager.getICEServers() },
          request_id: message.request_id,
        });
        
        // Deliver any signals that arrived while this peer was offline (late joiner
        // or WebRTC renegotiation). Drains and clears the Redis-backed queue.
        const queued = await peerManager.getQueuedSignals(peer.peer_id);
        for (const sig of queued) {
          this.send(ws, {
            type: sig.type,
            payload: { from_peer_id: sig.from_peer_id, ...sig.payload },
          });
        }

        // Notify others in same location
        this.broadcastToLocation(peer.apmc_location, {
          type: 'peer_joined',
          payload: { peer_id: peer.peer_id, peer_type: peer.peer_type, metadata: peer.metadata },
        }, socketId);
        
        logger.info({ peer_id: peer.peer_id, socketId }, 'Peer registered via WebSocket');
        break;
      }
      
      case 'offer':
      case 'answer':
      case 'ice_candidate': {
        if (!currentPeer) {
          this.send(ws, { 
            type: 'error', 
            payload: { message: 'Not registered' },
            request_id: message.request_id 
          });
          return currentPeer;
        }
        
        const { to_peer_id, ...payload } = message.payload;
        
        if (!to_peer_id) {
          this.send(ws, { 
            type: 'error', 
            payload: { message: 'to_peer_id is required' },
            request_id: message.request_id 
          });
          return currentPeer;
        }
        
        // Relay the SDP offer/answer or ICE candidate straight to the target peer's
        // live socket on THIS instance. This is the essence of WebRTC signaling: the
        // server only brokers connection setup — once the peers exchange SDP + ICE,
        // media/data flows peer-to-peer (via STUN/TURN) and never touches this server.
        const delivered = this.deliverToPeer(to_peer_id, {
          type: message.type,
          payload: { from_peer_id: currentPeer.peer_id, ...payload },
        });

        if (!delivered) {
          // Target isn't connected here → persist so it drains on (re)register.
          await peerManager.queueSignal({
            from_peer_id: currentPeer.peer_id,
            to_peer_id,
            type: message.type,
            payload,
            timestamp: now(),
          });
          this.send(ws, {
            type: 'error',
            payload: { message: 'Target peer offline, signal queued' },
            request_id: message.request_id,
          });
        }

        break;
      }
      
      case 'get_peers': {
        if (!currentPeer) {
          this.send(ws, { 
            type: 'error', 
            payload: { message: 'Not registered' },
            request_id: message.request_id 
          });
          return currentPeer;
        }
        
        const { peer_type } = message.payload;
        const peers = await peerManager.getOnlinePeers(currentPeer.apmc_location, peer_type);
        
        this.send(ws, {
          type: 'peer_list',
          payload: { peers: peers.map(p => ({
            peer_id: p.peer_id,
            peer_type: p.peer_type,
            apmc_location: p.apmc_location,
            metadata: p.metadata,
            is_online: p.is_online,
            last_seen: p.last_seen,
          })) },
          request_id: message.request_id,
        });
        break;
      }
      
      case 'ping': {
        this.send(ws, { type: 'pong', payload: { timestamp: now() }, request_id: message.request_id });
        if (currentPeer) {
          // Keep the in-memory view fresh too — cleanupStaleConnections() reaps by
          // client.peer.last_seen, so without this an actively-pinging peer would be
          // closed at the 60s stale threshold.
          currentPeer.last_seen = now();
          await peerManager.updatePeerHeartbeat(currentPeer.peer_id, socketId);
        }
        break;
      }
    }
    
    return currentPeer;
  }

  private async handlePeerDisconnect(peer: Peer): Promise<void> {
    await peerManager.unregisterPeer(peer.peer_id);
    
    // Notify others in same location
    this.broadcastToLocation(peer.apmc_location, {
      type: 'peer_left',
      payload: { peer_id: peer.peer_id },
    });
    
    logger.info({ peer_id: peer.peer_id }, 'Peer disconnected');
  }

  // Find the live socket for a peer_id on THIS instance and push a message to it.
  // Returns false if that peer isn't connected here (caller then queues the signal).
  private deliverToPeer(toPeerId: string, response: WSResponse): boolean {
    for (const [, client] of connectedClients) {
      if (client.peer.peer_id === toPeerId) {
        this.send(client.ws, response);
        return true;
      }
    }
    return false;
  }

  private send(ws: WebSocket, response: WSResponse): void {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify(response));
    }
  }

  private broadcastToLocation(location: string, message: WSResponse, excludeSocketId?: string): void {
    for (const [socketId, client] of connectedClients) {
      if (socketId !== excludeSocketId && client.peer.apmc_location === location) {
        this.send(client.ws, message);
      }
    }
  }

  private async cleanupStaleConnections(): Promise<void> {
    const now = Date.now();
    const staleThreshold = 60000; // 60 seconds
    
    for (const [socketId, client] of connectedClients) {
      if (now - client.peer.last_seen > staleThreshold) {
        client.ws.close(1008, 'Stale connection');
        connectedClients.delete(socketId);
        await this.handlePeerDisconnect(client.peer);
      }
    }
    
    // Also cleanup Redis
    const cleaned = await peerManager.cleanupStalePeers();
    if (cleaned > 0) {
      logger.info({ cleaned }, 'Cleaned up stale peers');
    }
  }
}

export const signalingWsServer = new SignalingWebSocketServer();