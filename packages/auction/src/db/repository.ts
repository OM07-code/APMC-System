import { query, transaction, getClient } from './pool';
import { generateAuctionId, generateBidId, now } from '@apmc/shared';
import { AppError, NotFoundError, ConflictError } from '@apmc/shared';

export interface Auction {
  id: string;
  auction_id: string;
  commodity_id: string;
  grade_id: string;
  quantity: number;
  base_price: number;
  current_highest_bid: number;
  current_winner_id: string | null;
  apmc_location_id: string;
  farmer_id: string;
  auctioneer_id: string | null;
  status: 'pending' | 'active' | 'ended' | 'cancelled';
  start_time: Date | null;
  end_time: Date | null;
  duration_seconds: number;
  images: string[];
  metadata: Record<string, any>;
  created_at: Date;
  updated_at: Date;
}

export interface Bid {
  id: string;
  bid_id: string;
  auction_id: string;
  buyer_id: string;
  amount: number;
  timestamp: Date;
  signature: string | null;
  is_winning: boolean;
  created_at: Date;
}

export interface CreateAuctionInput {
  commodity_id: string;
  grade_id: string;
  quantity: number;
  base_price: number;
  duration_seconds: number;
  apmc_location_id: string;
  farmer_id: string;
  images?: string[];
  metadata?: Record<string, any>;
}

export interface PlaceBidInput {
  auction_id: string;
  buyer_id: string;
  amount: number;
  signature?: string;
}

export class AuctionRepository {
  async create(input: CreateAuctionInput): Promise<Auction> {
    const auctionId = generateAuctionId();
    const result = await query<Auction>(
      `INSERT INTO auctions (auction_id, commodity_id, grade_id, quantity, base_price, 
       current_highest_bid, apmc_location_id, farmer_id, duration_seconds, images, metadata, status)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 'pending')
       RETURNING *`,
      [
        auctionId,
        input.commodity_id,
        input.grade_id,
        input.quantity,
        input.base_price,
        input.base_price,
        input.apmc_location_id,
        input.farmer_id,
        input.duration_seconds,
        input.images || [],
        JSON.stringify(input.metadata || {}),
      ]
    );
    return result.rows[0] as Auction;
  }

  async findById(id: string): Promise<Auction | null> {
    const result = await query<Auction>('SELECT * FROM auctions WHERE id = $1', [id]);
    return (result.rows[0] as Auction) || null;
  }

  async findByAuctionId(auctionId: string): Promise<Auction | null> {
    const result = await query<Auction>('SELECT * FROM auctions WHERE auction_id = $1', [auctionId]);
    return (result.rows[0] as Auction) || null;
  }

  async findByStatus(status: Auction['status']): Promise<Auction[]> {
    const result = await query<Auction>('SELECT * FROM auctions WHERE status = $1 ORDER BY created_at DESC', [status]);
    return result.rows;
  }

  async findActiveByLocation(locationId: string): Promise<Auction[]> {
    const result = await query<Auction>(
      `SELECT * FROM auctions 
       WHERE apmc_location_id = $1 AND status = 'active' 
       AND end_time > NOW()
       ORDER BY start_time ASC`,
      [locationId]
    );
    return result.rows;
  }

  async startAuction(id: string, auctioneerId: string): Promise<Auction> {
    const startTime = new Date();
    const result = await query<Auction>(
      `UPDATE auctions 
       SET status = 'active', start_time = $1::timestamptz, end_time = $1::timestamptz + COALESCE(duration_seconds, 0) * INTERVAL '1 second', auctioneer_id = $2
       WHERE id = $3 AND status = 'pending'
       RETURNING *`,
      [startTime, auctioneerId, id]
    );
    if (result.rows.length === 0) {
      throw new NotFoundError('Auction', id);
    }
    return result.rows[0] as Auction;
  }

  async endAuction(id: string): Promise<Auction> {
    const result = await query<Auction>(
      `UPDATE auctions SET status = 'ended' WHERE id = $1 AND status = 'active' RETURNING *`,
      [id]
    );
    if (result.rows.length === 0) {
      throw new NotFoundError('Auction', id);
    }
    return result.rows[0] as Auction;
  }

  async cancelAuction(id: string): Promise<Auction> {
    const result = await query<Auction>(
      `UPDATE auctions SET status = 'cancelled' WHERE id = $1 AND status IN ('pending', 'active') RETURNING *`,
      [id]
    );
    if (result.rows.length === 0) {
      throw new NotFoundError('Auction', id);
    }
    return result.rows[0] as Auction;
  }

  async placeBid(input: PlaceBidInput): Promise<{ bid: Bid; auction: Auction }> {
    return transaction(async (client) => {
      // Lock the auction row for update
      const auctionResult = await client.query<Auction>(
        'SELECT * FROM auctions WHERE auction_id = $1 AND status = $2 FOR UPDATE',
        [input.auction_id, 'active']
      );
      
      if (auctionResult.rows.length === 0) {
        throw new NotFoundError('Auction', input.auction_id);
      }
      
      const auction = auctionResult.rows[0] as Auction;
      
      // Check if auction has ended
      if (auction.end_time && new Date() > auction.end_time) {
        throw new ConflictError('Auction has ended');
      }
      
      // Check bid amount
      if (input.amount <= auction.current_highest_bid) {
        throw new ConflictError(`Bid must be higher than current highest bid: ${auction.current_highest_bid}`);
      }
      
      if (input.amount < auction.base_price) {
        throw new ConflictError(`Bid must be at least base price: ${auction.base_price}`);
      }
      
      // Create bid
      const bidId = generateBidId();
      const bidResult = await client.query<Bid>(
        `INSERT INTO bids (bid_id, auction_id, buyer_id, amount, signature, is_winning)
         VALUES ($1, $2, $3, $4, $5, true)
         RETURNING *`,
        [bidId, auction.id, input.buyer_id, input.amount, input.signature || null]
      );
      
      const bid = bidResult.rows[0] as Bid;
      
      // Mark previous winning bid as not winning
      await client.query(
        'UPDATE bids SET is_winning = false WHERE auction_id = $1 AND is_winning = true AND id != $2',
        [auction.id, bid.id]
      );
      
      // Update auction with new highest bid
      const auctionUpdateResult = await client.query<Auction>(
        `UPDATE auctions 
         SET current_highest_bid = $1, current_winner_id = $2
         WHERE id = $3
         RETURNING *`,
        [input.amount, input.buyer_id, auction.id]
      );
      
      return { bid, auction: auctionUpdateResult.rows[0] as Auction };
    });
  }

  async getBids(auctionId: string, limit: number = 50, offset: number = 0): Promise<Bid[]> {
    const result = await query<Bid>(
      `SELECT b.* FROM bids b
       JOIN auctions a ON b.auction_id = a.id
       WHERE a.auction_id = $1
       ORDER BY b.amount DESC, b.timestamp ASC
       LIMIT $2 OFFSET $3`,
      [auctionId, limit, offset]
    );
    return result.rows;
  }

  async getBidCount(auctionId: string): Promise<number> {
    const result = await query<{ count: string }>(
      `SELECT COUNT(*) as count FROM bids b
       JOIN auctions a ON b.auction_id = a.id
       WHERE a.auction_id = $1`,
      [auctionId]
    );
    return parseInt(result.rows[0]?.count || '0', 10);
  }

  async recordPriceHistory(auctionId: string, price: number, volume: number): Promise<void> {
    const auction = await this.findByAuctionId(auctionId);
    if (!auction) return;
    
    await query(
      `INSERT INTO price_history (commodity_id, grade_id, apmc_location_id, price, volume, source)
       VALUES ($1, $2, $3, $4, $5, 'auction')`,
      [auction.commodity_id, auction.grade_id, auction.apmc_location_id, price, volume]
    );
  }

  async recordAuctionEvent(auctionId: string, eventType: string, data: Record<string, any>): Promise<void> {
    const auction = await this.findByAuctionId(auctionId);
    if (!auction) return;
    
    await query(
      `INSERT INTO auction_events (event_id, auction_id, type, data)
       VALUES ($1, $2, $3, $4)`,
      [`evt-${generateAuctionId()}`, auction.id, eventType, JSON.stringify(data)]
    );
  }
}

export const auctionRepository = new AuctionRepository();