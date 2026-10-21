import { FastifyInstance } from 'fastify';
import { config } from '@apmc/config';
import { logger } from '@apmc/middleware';
import { AppError, ValidationError, NotFoundError } from '@apmc/shared';
import { auctionRepository, CreateAuctionInput, PlaceBidInput } from '@apmc/auction';
import { generateAuctionId, generateBidId, now } from '@apmc/shared';
import {
  publishAuctionEvent as brokerPublishAuctionEvent,
  publishPriceUpdate as brokerPublishPriceUpdate,
} from '@apmc/broker';

// Best-effort publish wrappers: the DB write is the source of truth, so a Kafka
// hiccup must NOT turn a successful create/bid/start/end into an HTTP 500. If the
// broker is unavailable we log a warning and continue; the event stream degrades
// gracefully instead of failing the request.
async function publishAuctionEvent(msg: Parameters<typeof brokerPublishAuctionEvent>[0]): Promise<void> {
  try {
    await brokerPublishAuctionEvent(msg);
  } catch (err) {
    logger.warn({ err, auction_id: msg.auction_id }, 'auction event publish failed (non-fatal)');
  }
}

async function publishPriceUpdate(msg: Parameters<typeof brokerPublishPriceUpdate>[0]): Promise<void> {
  try {
    await brokerPublishPriceUpdate(msg);
  } catch (err) {
    logger.warn({ err, auction_id: msg.auction_id }, 'price update publish failed (non-fatal)');
  }
}

// The authenticate() hook attaches the verified JWT claims here. Sourcing identity
// from this — never from the request body — is what stops a caller from acting as
// an arbitrary farmer_id / buyer_id / auctioneer_id (impersonation).
interface AuthUser {
  id: string;
  email: string;
  role: string;
}

function requireRole(request: any, role: string, action: string): AuthUser {
  const user = request.user as AuthUser | undefined;
  if (!user) {
    throw new AppError('AUTHENTICATION_ERROR', 'Authentication required', 401);
  }
  if (user.role !== role) {
    throw new AppError('AUTHORIZATION_ERROR', `Only ${role}s can ${action}`, 403);
  }
  return user;
}

export async function auctionRoutes(fastify: FastifyInstance): Promise<void> {
  // Create auction (farmer only)
  fastify.post<{
    Body: {
      commodity: string;
      grade: string;
      quantity: number;
      base_price: number;
      duration_seconds: number;
      apmc_location: string;
      farmer_id?: string;
      images?: string[];
    };
  }>('/api/v1/auctions', async (request, reply) => {
    try {
      // Identity from the verified JWT, not the body.
      const farmer_id = requireRole(request, 'farmer', 'create auctions').id;

      const { commodity, grade, quantity, base_price, duration_seconds, apmc_location, images } = request.body;

      if (!commodity || !grade || !quantity || !base_price || !duration_seconds || !apmc_location) {
        throw new ValidationError('Missing required fields');
      }

      // Resolve IDs
      const commodityResult = await require('@apmc/auction').query('SELECT id FROM commodities WHERE code = $1', [commodity]);
      const gradeResult = await require('@apmc/auction').query(
        'SELECT g.id FROM grades g JOIN commodities c ON g.commodity_id = c.id WHERE c.code = $1 AND g.code = $2',
        [commodity, grade]
      );
      const locationResult = await require('@apmc/auction').query('SELECT id FROM apmc_locations WHERE code = $1', [apmc_location]);

      if (commodityResult.rows.length === 0) throw new NotFoundError('Commodity', commodity);
      if (gradeResult.rows.length === 0) throw new NotFoundError('Grade', grade);
      if (locationResult.rows.length === 0) throw new NotFoundError('APMC Location', apmc_location);

      const auction = await auctionRepository.create({
        commodity_id: commodityResult.rows[0].id,
        grade_id: gradeResult.rows[0].id,
        quantity,
        base_price,
        duration_seconds,
        apmc_location_id: locationResult.rows[0].id,
        farmer_id,
        images: images || [],
      });

      await publishAuctionEvent({
        event_id: generateAuctionId(),
        auction_id: auction.auction_id,
        type: 1, // AUCTION_CREATED
        timestamp: now(),
        data: { commodity, grade, quantity: quantity.toString(), base_price: base_price.toString() },
      });

      return reply.code(201).send({
        success: true,
        auction_id: auction.auction_id,
        message: 'Auction created successfully',
      });
    } catch (error) {
      if (!(error instanceof AppError)) logger.error({ err: error, url: request.url }, 'Failed to create auction');
      const appError = error instanceof AppError ? error : new AppError('INTERNAL_ERROR', 'Failed to create auction');
      return reply.code(appError.statusCode).send({ error: appError.message, code: appError.code });
    }
  });

  // Get auction state
  fastify.get<{
    Params: { auctionId: string };
  }>('/api/v1/auctions/:auctionId', async (request, reply) => {
    try {
      const { auctionId } = request.params;

      const auction = await auctionRepository.findByAuctionId(auctionId);
      if (!auction) {
        throw new NotFoundError('Auction', auctionId);
      }

      const commodityResult = await require('@apmc/auction').query('SELECT name FROM commodities WHERE id = $1', [auction.commodity_id]);
      const gradeResult = await require('@apmc/auction').query('SELECT name FROM grades WHERE id = $1', [auction.grade_id]);
      const locationResult = await require('@apmc/auction').query('SELECT code FROM apmc_locations WHERE id = $1', [auction.apmc_location_id]);

      return {
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
        images: auction.images,
      };
    } catch (error) {
      if (!(error instanceof AppError)) logger.error({ err: error, url: request.url }, 'Failed to get auction');
      const appError = error instanceof AppError ? error : new AppError('INTERNAL_ERROR', 'Failed to get auction');
      return reply.code(appError.statusCode).send({ error: appError.message, code: appError.code });
    }
  });

  // Place bid (buyer only)
  fastify.post<{
    Params: { auctionId: string };
    Body: {
      buyer_id?: string;
      amount: number;
      signature?: string;
    };
  }>('/api/v1/auctions/:auctionId/bids', async (request, reply) => {
    try {
      const { auctionId } = request.params;
      // Bidder identity from the JWT, not the body.
      const buyer_id = requireRole(request, 'buyer', 'place bids').id;

      const { amount, signature } = request.body;

      // Coerce + validate the money value. The gRPC path already did this; the HTTP
      // path did not, so "2500" (string) or a NaN could slip into the bid comparison.
      const bidAmount = Number(amount);
      if (!Number.isFinite(bidAmount) || bidAmount <= 0) {
        throw new ValidationError('amount must be a positive number');
      }

      const { bid, auction } = await auctionRepository.placeBid({
        auction_id: auctionId,
        buyer_id,
        amount: bidAmount,
        signature,
      });

      // Publish price update
      await publishPriceUpdate({
        auction_id: auctionId,
        commodity: (await require('@apmc/auction').query('SELECT name FROM commodities WHERE id = $1', [auction.commodity_id])).rows[0]?.name || '',
        grade: (await require('@apmc/auction').query('SELECT name FROM grades WHERE id = $1', [auction.grade_id])).rows[0]?.name || '',
        price: auction.current_highest_bid,
        timestamp: now(),
        apmc_location: (await require('@apmc/auction').query('SELECT code FROM apmc_locations WHERE id = $1', [auction.apmc_location_id])).rows[0]?.code || '',
        currency: 'INR',
        source: 1,
      });

      await publishAuctionEvent({
        event_id: generateAuctionId(),
        auction_id: auctionId,
        type: 3, // BID_PLACED
        timestamp: now(),
        data: { bid_id: bid.bid_id, buyer_id, amount: bidAmount.toString() },
      });

      return reply.code(201).send({
        success: true,
        bid_id: bid.bid_id,
        current_highest_bid: auction.current_highest_bid,
        message: 'Bid placed successfully',
      });
    } catch (error) {
      if (!(error instanceof AppError)) logger.error({ err: error, url: request.url }, 'Failed to place bid');
      const appError = error instanceof AppError ? error : new AppError('INTERNAL_ERROR', 'Failed to place bid');
      return reply.code(appError.statusCode).send({ error: appError.message, code: appError.code });
    }
  });

  // Get bid history
  fastify.get<{
    Params: { auctionId: string };
    Querystring: { limit?: string; offset?: string };
  }>('/api/v1/auctions/:auctionId/bids', async (request, reply) => {
    try {
      const { auctionId } = request.params;
      const limit = parseInt(request.query.limit || '50', 10);
      const offset = parseInt(request.query.offset || '0', 10);

      const bids = await auctionRepository.getBids(auctionId, limit, offset);
      const total = await auctionRepository.getBidCount(auctionId);

      return {
        bids: bids.map(b => ({
          bid_id: b.bid_id,
          buyer_id: b.buyer_id,
          amount: b.amount,
          timestamp: b.timestamp.toISOString(),
          is_winning: b.is_winning,
        })),
        total,
      };
    } catch (error) {
      if (!(error instanceof AppError)) logger.error({ err: error, url: request.url }, 'Failed to get bid history');
      const appError = error instanceof AppError ? error : new AppError('INTERNAL_ERROR', 'Failed to get bid history');
      return reply.code(appError.statusCode).send({ error: appError.message, code: appError.code });
    }
  });

  // Start auction (auctioneer only)
  fastify.post<{
    Params: { auctionId: string };
    Body: { auctioneer_id?: string };
  }>('/api/v1/auctions/:auctionId/start', async (request, reply) => {
    try {
      const { auctionId } = request.params;
      // Auctioneer identity from the JWT, not the body.
      const auctioneer_id = requireRole(request, 'auctioneer', 'start auctions').id;

      const auction = await auctionRepository.findByAuctionId(auctionId);
      if (!auction) {
        throw new NotFoundError('Auction', auctionId);
      }

      const startedAuction = await auctionRepository.startAuction(auction.id, auctioneer_id);

      await publishAuctionEvent({
        event_id: generateAuctionId(),
        auction_id: auctionId,
        type: 2, // AUCTION_STARTED
        timestamp: now(),
        data: {},
      });

      return {
        success: true,
        message: 'Auction started',
        start_time: startedAuction.start_time?.toISOString(),
        end_time: startedAuction.end_time?.toISOString(),
      };
    } catch (error) {
      if (!(error instanceof AppError)) logger.error({ err: error, url: request.url }, 'Failed to start auction');
      const appError = error instanceof AppError ? error : new AppError('INTERNAL_ERROR', 'Failed to start auction');
      return reply.code(appError.statusCode).send({ error: appError.message, code: appError.code });
    }
  });

  // End auction (auctioneer only)
  fastify.post<{
    Params: { auctionId: string };
    Body: { auctioneer_id?: string };
  }>('/api/v1/auctions/:auctionId/end', async (request, reply) => {
    try {
      const { auctionId } = request.params;
      // Auctioneer identity from the JWT, not the body.
      const auctioneer_id = requireRole(request, 'auctioneer', 'end auctions').id;

      const auction = await auctionRepository.findByAuctionId(auctionId);
      if (!auction) {
        throw new NotFoundError('Auction', auctionId);
      }

      if (auction.auctioneer_id && auction.auctioneer_id !== auctioneer_id) {
        throw new AppError('FORBIDDEN', 'Not authorized to end this auction', 403);
      }

      const endedAuction = await auctionRepository.endAuction(auction.id);

      await publishPriceUpdate({
        auction_id: auctionId,
        commodity: (await require('@apmc/auction').query('SELECT name FROM commodities WHERE id = $1', [auction.commodity_id])).rows[0]?.name || '',
        grade: (await require('@apmc/auction').query('SELECT name FROM grades WHERE id = $1', [auction.grade_id])).rows[0]?.name || '',
        price: endedAuction.current_highest_bid,
        timestamp: now(),
        apmc_location: (await require('@apmc/auction').query('SELECT code FROM apmc_locations WHERE id = $1', [auction.apmc_location_id])).rows[0]?.code || '',
        currency: 'INR',
        source: 1,
      });

      await publishAuctionEvent({
        event_id: generateAuctionId(),
        auction_id: auctionId,
        type: 4, // AUCTION_ENDED
        timestamp: now(),
        data: { winner_id: endedAuction.current_winner_id || '', winning_bid: endedAuction.current_highest_bid.toString() },
      });

      return {
        success: true,
        winner_id: endedAuction.current_winner_id,
        winning_bid: endedAuction.current_highest_bid,
        message: 'Auction ended successfully',
      };
    } catch (error) {
      if (!(error instanceof AppError)) logger.error({ err: error, url: request.url }, 'Failed to end auction');
      const appError = error instanceof AppError ? error : new AppError('INTERNAL_ERROR', 'Failed to end auction');
      return reply.code(appError.statusCode).send({ error: appError.message, code: appError.code });
    }
  });

  // List active auctions
  fastify.get<{
    Querystring: { location?: string; commodity?: string; limit?: string; offset?: string };
  }>('/api/v1/auctions', async (request, reply) => {
    try {
      const { location, commodity, limit, offset } = request.query;

      let auctions: any[];
      if (location) {
        const locationResult = await require('@apmc/auction').query('SELECT id FROM apmc_locations WHERE code = $1', [location]);
        if (locationResult.rows.length > 0) {
          auctions = await auctionRepository.findActiveByLocation(locationResult.rows[0].id);
        } else {
          auctions = [];
        }
      } else {
        auctions = await auctionRepository.findByStatus('active');
      }

      // Filter by commodity if specified
      if (commodity && auctions.length > 0) {
        const commodityResult = await require('@apmc/auction').query('SELECT id FROM commodities WHERE code = $1', [commodity]);
        if (commodityResult.rows.length > 0) {
          auctions = auctions.filter(a => a.commodity_id === commodityResult.rows[0].id);
        } else {
          auctions = [];
        }
      }

      const total = auctions.length;
      const start = parseInt(offset || '0', 10);
      const end = start + parseInt(limit || '20', 10);
      const paginated = auctions.slice(start, end);

      // Enrich with names
      const enriched = await Promise.all(paginated.map(async (a) => {
        const commodityResult = await require('@apmc/auction').query('SELECT name FROM commodities WHERE id = $1', [a.commodity_id]);
        const gradeResult = await require('@apmc/auction').query('SELECT name FROM grades WHERE id = $1', [a.grade_id]);
        const locationResult = await require('@apmc/auction').query('SELECT code FROM apmc_locations WHERE id = $1', [a.apmc_location_id]);

        return {
          auction_id: a.auction_id,
          commodity: commodityResult.rows[0]?.name || '',
          grade: gradeResult.rows[0]?.name || '',
          quantity: a.quantity,
          base_price: a.base_price,
          current_highest_bid: a.current_highest_bid,
          status: a.status,
          start_time: a.start_time?.toISOString(),
          end_time: a.end_time?.toISOString(),
          apmc_location: locationResult.rows[0]?.code || '',
        };
      }));

      return { auctions: enriched, total };
    } catch (error) {
      if (!(error instanceof AppError)) logger.error({ err: error, url: request.url }, 'Failed to list auctions');
      const appError = error instanceof AppError ? error : new AppError('INTERNAL_ERROR', 'Failed to list auctions');
      return reply.code(appError.statusCode).send({ error: appError.message, code: appError.code });
    }
  });
}
