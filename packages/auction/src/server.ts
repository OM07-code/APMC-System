import { Server, ServerCredentials, sendUnaryData, ServerUnaryCall, ServerWritableStream, Metadata, loadPackageDefinition } from '@grpc/grpc-js';
import { loadSync } from '@grpc/proto-loader';
import { join } from 'path';
import { config } from '@apmc/config';
import { logger } from '@apmc/middleware';
import { circuitBreakerRegistry, withRetry, retryableErrors } from '@apmc/fault';
import { auctionRepository, Auction, Bid } from './db/repository';
import { generateAuctionId, generateBidId, now } from '@apmc/shared';
import { AppError, NotFoundError, ConflictError, ValidationError } from '@apmc/shared';

const PROTO_PATH = join(__dirname, '..', '..', '..', 'proto', 'auction.proto');

const packageDefinition = loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
});

const proto = loadPackageDefinition(packageDefinition) as any;

const auctionService = proto.apmc.auction.v1.AuctionService;

interface AuctionStateResponse {
  auction_id: string;
  commodity: string;
  grade: string;
  quantity: string;
  base_price: string;
  current_highest_bid: string;
  current_winner: string;
  start_time: string;
  end_time: string;
  status: number;
  apmc_location: string;
}

interface AuctionUpdate {
  auction_id: string;
  type: number;
  new_bid: string;
  bidder_id: string;
  timestamp: string;
  status: number;
}

const AUCTION_STATUS_MAP = {
  pending: 1,
  active: 2,
  ended: 3,
  cancelled: 4,
};

const UPDATE_TYPE_MAP = {
  new_bid: 1,
  status_change: 2,
  auction_ended: 3,
};

async function placeBid(call: ServerUnaryCall<any, any>, callback: sendUnaryData<any>) {
  try {
    const { auction_id, buyer_id, amount, timestamp, signature } = call.request;
    
    if (!auction_id || !buyer_id || !amount) {
      throw new ValidationError('Missing required fields: auction_id, buyer_id, amount');
    }

    const bidAmount = parseFloat(amount);
    if (isNaN(bidAmount) || bidAmount <= 0) {
      throw new ValidationError('Invalid bid amount');
    }

    const { bid, auction } = await auctionRepository.placeBid({
      auction_id,
      buyer_id,
      amount: bidAmount,
      signature,
    });

    await auctionRepository.recordPriceHistory(auction_id, bidAmount, 0);
    
    await auctionRepository.recordAuctionEvent(auction_id, 'BID_PLACED', {
      bid_id: bid.bid_id,
      buyer_id: bid.buyer_id,
      amount: bidAmount,
    });

    callback(null, {
      success: true,
      bid_id: bid.bid_id,
      current_highest_bid: auction.current_highest_bid.toString(),
      message: 'Bid placed successfully',
    });
  } catch (error) {
    const appError = error instanceof AppError ? error : new AppError('INTERNAL_ERROR', 'Failed to place bid');
    logger.error({ err: error, auction_id: call.request.auction_id }, 'PlaceBid failed');
    callback({
      code: appError.statusCode >= 500 ? 13 : 3,
      details: appError.message,
      metadata: new Metadata(),
    }, null);
  }
}

async function getAuctionState(call: ServerUnaryCall<any, any>, callback: sendUnaryData<any>) {
  try {
    const { auction_id } = call.request;
    
    if (!auction_id) {
      throw new ValidationError('Missing auction_id');
    }

    const auction = await auctionRepository.findByAuctionId(auction_id);
    if (!auction) {
      throw new NotFoundError('Auction', auction_id);
    }

    const commodityResult = await require('./db/pool').query('SELECT name FROM commodities WHERE id = $1', [auction.commodity_id]);
    const gradeResult = await require('./db/pool').query('SELECT name FROM grades WHERE id = $1', [auction.grade_id]);
    const locationResult = await require('./db/pool').query('SELECT code FROM apmc_locations WHERE id = $1', [auction.apmc_location_id]);

    const response: AuctionStateResponse = {
      auction_id: auction.auction_id,
      commodity: commodityResult.rows[0]?.name || auction.commodity_id,
      grade: gradeResult.rows[0]?.name || auction.grade_id,
      quantity: auction.quantity.toString(),
      base_price: auction.base_price.toString(),
      current_highest_bid: auction.current_highest_bid.toString(),
      current_winner: auction.current_winner_id || '',
      start_time: auction.start_time?.getTime().toString() || '0',
      end_time: auction.end_time?.getTime().toString() || '0',
      status: AUCTION_STATUS_MAP[auction.status] || 0,
      apmc_location: locationResult.rows[0]?.code || auction.apmc_location_id,
    };

    callback(null, response);
  } catch (error) {
    const appError = error instanceof AppError ? error : new AppError('INTERNAL_ERROR', 'Failed to get auction state');
    logger.error({ err: error, auction_id: call.request.auction_id }, 'GetAuctionState failed');
    callback({
      code: appError.statusCode === 404 ? 5 : 13,
      details: appError.message,
      metadata: new Metadata(),
    }, null);
  }
}

async function watchAuction(call: ServerWritableStream<any, any>) {
  const { auction_id } = call.request;
  
  if (!auction_id) {
    call.emit('error', { code: 3, details: 'Missing auction_id' });
    return;
  }

  const auction = await auctionRepository.findByAuctionId(auction_id);
  if (auction) {
    const initialUpdate: AuctionUpdate = {
      auction_id: auction.auction_id,
      type: UPDATE_TYPE_MAP.status_change,
      new_bid: '0',
      bidder_id: '',
      timestamp: now().toString(),
      status: AUCTION_STATUS_MAP[auction.status] || 0,
    };
    call.write(initialUpdate);
  }

  const interval = setInterval(async () => {
    const current = await auctionRepository.findByAuctionId(auction_id);
    if (current && current.status === 'ended') {
      const update: AuctionUpdate = {
        auction_id: current.auction_id,
        type: UPDATE_TYPE_MAP.auction_ended,
        new_bid: current.current_highest_bid.toString(),
        bidder_id: current.current_winner_id || '',
        timestamp: now().toString(),
        status: AUCTION_STATUS_MAP.ended,
      };
      call.write(update);
      clearInterval(interval);
      call.end();
    }
  }, 5000);

  call.on('cancelled', () => {
    clearInterval(interval);
  });
}

async function createAuction(call: ServerUnaryCall<any, any>, callback: sendUnaryData<any>) {
  try {
    const { commodity, grade, quantity, base_price, duration_seconds, apmc_location, farmer_id, images } = call.request;
    
    if (!commodity || !grade || !quantity || !base_price || !duration_seconds || !apmc_location || !farmer_id) {
      throw new ValidationError('Missing required fields');
    }

    const commodityResult = await require('./db/pool').query('SELECT id FROM commodities WHERE code = $1', [commodity]);
    const gradeResult = await require('./db/pool').query(
      'SELECT g.id FROM grades g JOIN commodities c ON g.commodity_id = c.id WHERE c.code = $1 AND g.code = $2',
      [commodity, grade]
    );
    const locationResult = await require('./db/pool').query('SELECT id FROM apmc_locations WHERE code = $1', [apmc_location]);

    if (commodityResult.rows.length === 0) throw new NotFoundError('Commodity', commodity);
    if (gradeResult.rows.length === 0) throw new NotFoundError('Grade', grade);
    if (locationResult.rows.length === 0) throw new NotFoundError('APMC Location', apmc_location);

    const auction = await auctionRepository.create({
      commodity_id: commodityResult.rows[0].id,
      grade_id: gradeResult.rows[0].id,
      quantity: parseFloat(quantity),
      base_price: parseFloat(base_price),
      duration_seconds: parseInt(duration_seconds, 10),
      apmc_location_id: locationResult.rows[0].id,
      farmer_id,
      images: images || [],
    });

    await auctionRepository.recordAuctionEvent(auction.auction_id, 'AUCTION_CREATED', {
      commodity,
      grade,
      quantity,
      base_price,
    });

    callback(null, {
      auction_id: auction.auction_id,
      success: true,
      message: 'Auction created successfully',
    });
  } catch (error) {
    const appError = error instanceof AppError ? error : new AppError('INTERNAL_ERROR', 'Failed to create auction');
    logger.error({ err: error }, 'CreateAuction failed');
    callback({
      code: appError.statusCode >= 500 ? 13 : 3,
      details: appError.message,
      metadata: new Metadata(),
    }, null);
  }
}

async function endAuction(call: ServerUnaryCall<any, any>, callback: sendUnaryData<any>) {
  try {
    const { auction_id, auctioneer_id } = call.request;
    
    if (!auction_id || !auctioneer_id) {
      throw new ValidationError('Missing required fields');
    }

    const auction = await auctionRepository.findByAuctionId(auction_id);
    if (!auction) {
      throw new NotFoundError('Auction', auction_id);
    }

    if (auction.auctioneer_id && auction.auctioneer_id !== auctioneer_id) {
      throw new AppError('FORBIDDEN', 'Not authorized to end this auction', 403);
    }

    const endedAuction = await auctionRepository.endAuction(auction.id);
    
    await auctionRepository.recordAuctionEvent(auction_id, 'AUCTION_ENDED', {
      winner_id: endedAuction.current_winner_id,
      winning_bid: endedAuction.current_highest_bid,
    });

    await require('@apmc/broker').publishPriceUpdate({
      auction_id,
      commodity: (await require('./db/pool').query('SELECT name FROM commodities WHERE id = $1', [auction.commodity_id])).rows[0]?.name || '',
      grade: (await require('./db/pool').query('SELECT name FROM grades WHERE id = $1', [auction.grade_id])).rows[0]?.name || '',
      price: endedAuction.current_highest_bid,
      timestamp: now(),
      apmc_location: (await require('./db/pool').query('SELECT code FROM apmc_locations WHERE id = $1', [auction.apmc_location_id])).rows[0]?.code || '',
      currency: 'INR',
      source: 1,
    });

    callback(null, {
      success: true,
      winner_id: endedAuction.current_winner_id || '',
      winning_bid: endedAuction.current_highest_bid.toString(),
      message: 'Auction ended successfully',
    });
  } catch (error) {
    const appError = error instanceof AppError ? error : new AppError('INTERNAL_ERROR', 'Failed to end auction');
    logger.error({ err: error, auction_id: call.request.auction_id }, 'EndAuction failed');
    callback({
      code: appError.statusCode >= 500 ? 13 : 3,
      details: appError.message,
      metadata: new Metadata(),
    }, null);
  }
}

async function getBidHistory(call: ServerUnaryCall<any, any>, callback: sendUnaryData<any>) {
  try {
    const { auction_id, limit, offset } = call.request;
    
    if (!auction_id) {
      throw new ValidationError('Missing auction_id');
    }

    const bids = await auctionRepository.getBids(auction_id, limit || 50, offset || 0);
    const total = await auctionRepository.getBidCount(auction_id);

    callback(null, {
      bids: bids.map(b => ({
        bid_id: b.bid_id,
        buyer_id: b.buyer_id,
        amount: b.amount.toString(),
        timestamp: b.timestamp.getTime().toString(),
        is_winning: b.is_winning,
      })),
      total,
    });
  } catch (error) {
    const appError = error instanceof AppError ? error : new AppError('INTERNAL_ERROR', 'Failed to get bid history');
    logger.error({ err: error, auction_id: call.request.auction_id }, 'GetBidHistory failed');
    callback({
      code: 13,
      details: appError.message,
      metadata: new Metadata(),
    }, null);
  }
}

export function createAuctionServer(): Server {
  const server = new Server();
  
  server.addService(auctionService.service, {
    PlaceBid: placeBid,
    GetAuctionState: getAuctionState,
    WatchAuction: watchAuction,
    CreateAuction: createAuction,
    EndAuction: endAuction,
    GetBidHistory: getBidHistory,
  });

  return server;
}

export async function startAuctionServer(): Promise<Server> {
  const server = createAuctionServer();
  
  return new Promise((resolve, reject) => {
    server.bindAsync(
      `0.0.0.0:${config.AUCTION_GRPC_PORT}`,
      ServerCredentials.createInsecure(),
      (err, port) => {
        if (err) {
          reject(err);
          return;
        }
        server.start();
        logger.info(`Auction gRPC server started on port ${port}`);
        resolve(server);
      }
    );
  });
}