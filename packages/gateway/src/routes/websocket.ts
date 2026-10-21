import { FastifyInstance } from 'fastify';
import { config } from '@apmc/config';
import { logger } from '@apmc/middleware';
import { publishAuctionEvent } from '@apmc/broker';
import { now } from '@apmc/shared';

// Use any for WebSocket to avoid Fastify type issues
type WebSocket = any;

interface WSClient {
  ws: WebSocket;
  subscriptions: Set<string>;
  userId?: string;
}

const connectedClients = new Map<string, WSClient>();

export async function websocketRoutes(fastify: FastifyInstance): Promise<void> {
  // Auction WebSocket
  fastify.register(async (fastify) => {
    // @ts-ignore - websocket option added by @fastify/websocket plugin
    fastify.get('/ws/auctions/:auctionId', { websocket: true }, (connection: any, request: any) => {
      const { auctionId } = request.params as { auctionId: string };
      const clientId = `${request.socket.remoteAddress}:${request.socket.remotePort}:${Date.now()}`;
      
      const client: WSClient = {
        ws: connection.socket,
        subscriptions: new Set([`auction:${auctionId}`]),
      };
      
      connectedClients.set(clientId, client);
      logger.debug({ clientId, auctionId }, 'WebSocket client connected');
      
      // Send initial state
      sendAuctionState(connection.socket, auctionId);
      
      connection.socket.on('message', async (message: Buffer) => {
        try {
          const data = JSON.parse(message.toString());
          
          if (data.type === 'subscribe') {
            if (data.auction_id) {
              client.subscriptions.add(`auction:${data.auction_id}`);
            }
          } else if (data.type === 'unsubscribe') {
            if (data.auction_id) {
              client.subscriptions.delete(`auction:${data.auction_id}`);
            }
          } else if (data.type === 'ping') {
            connection.socket.send(JSON.stringify({ type: 'pong', timestamp: now() }));
          }
        } catch (error) {
          logger.error({ err: error, clientId }, 'WebSocket message error');
        }
      });
      
      connection.socket.on('close', () => {
        connectedClients.delete(clientId);
        logger.debug({ clientId }, 'WebSocket client disconnected');
      });
      
      connection.socket.on('error', (error: Error) => {
        logger.error({ err: error, clientId }, 'WebSocket error');
        connectedClients.delete(clientId);
      });
    });
  });

  // Market data WebSocket
  fastify.register(async (fastify) => {
    // @ts-ignore - websocket option added by @fastify/websocket plugin
    fastify.get('/ws/market', { websocket: true }, (connection: any, request: any) => {
      const clientId = `${request.socket.remoteAddress}:${request.socket.remotePort}:${Date.now()}`;
      
      const client: WSClient = {
        ws: connection.socket,
        subscriptions: new Set(['market:prices', 'market:alerts', 'market:index']),
      };
      
      connectedClients.set(clientId, client);
      logger.debug({ clientId }, 'Market WebSocket client connected');
      
      connection.socket.on('message', (message: Buffer) => {
        try {
          const data = JSON.parse(message.toString());
          
          if (data.type === 'subscribe') {
            if (data.channels) {
              for (const channel of data.channels) {
                client.subscriptions.add(channel);
              }
            }
          } else if (data.type === 'unsubscribe') {
            if (data.channels) {
              for (const channel of data.channels) {
                client.subscriptions.delete(channel);
              }
            }
          } else if (data.type === 'ping') {
            connection.socket.send(JSON.stringify({ type: 'pong', timestamp: now() }));
          }
        } catch (error) {
          logger.error({ err: error, clientId }, 'Market WebSocket message error');
        }
      });
      
      connection.socket.on('close', () => {
        connectedClients.delete(clientId);
        logger.debug({ clientId }, 'Market WebSocket client disconnected');
      });
    });
  });
}

async function sendAuctionState(ws: WebSocket, auctionId: string): Promise<void> {
  try {
    const auction = await require('@apmc/auction').auctionRepository.findByAuctionId(auctionId);
    if (!auction) return;
    
    const commodityResult = await require('@apmc/auction').query('SELECT name FROM commodities WHERE id = $1', [auction.commodity_id]);
    const gradeResult = await require('@apmc/auction').query('SELECT name FROM grades WHERE id = $1', [auction.grade_id]);
    const locationResult = await require('@apmc/auction').query('SELECT code FROM apmc_locations WHERE id = $1', [auction.apmc_location_id]);
    
    ws.send(JSON.stringify({
      type: 'auction_state',
      payload: {
        auction_id: auction.auction_id,
        commodity: commodityResult.rows[0]?.name || '',
        grade: gradeResult.rows[0]?.name || '',
        quantity: auction.quantity,
        base_price: auction.base_price,
        current_highest_bid: auction.current_highest_bid,
        current_winner: auction.current_winner_id,
        status: auction.status,
        start_time: auction.start_time?.toISOString(),
        end_time: auction.end_time?.toISOString(),
        apmc_location: locationResult.rows[0]?.code || '',
      },
    }));
  } catch (error) {
    logger.error({ err: error, auctionId }, 'Failed to send auction state');
  }
}

export function broadcastToAuction(auctionId: string, message: any): void {
  const topic = `auction:${auctionId}`;
  for (const [clientId, client] of connectedClients) {
    if (client.subscriptions.has(topic) && client.ws.readyState === 1) {
      client.ws.send(JSON.stringify(message));
    }
  }
}

export function broadcastToMarket(channel: string, message: any): void {
  const topic = `market:${channel}`;
  for (const [clientId, client] of connectedClients) {
    if (client.subscriptions.has(topic) && client.ws.readyState === 1) {
      client.ws.send(JSON.stringify(message));
    }
  }
}

export function broadcastToAll(message: any): void {
  for (const [clientId, client] of connectedClients) {
    if (client.ws.readyState === 1) {
      client.ws.send(JSON.stringify(message));
    }
  }
}

export function getConnectedClientsCount(): number {
  return connectedClients.size;
}