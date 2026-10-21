import { FastifyInstance } from 'fastify';
import { config } from '@apmc/config';
import { logger } from '@apmc/middleware';
import { AppError, NotFoundError } from '@apmc/shared';
import { now } from '@apmc/shared';

export async function marketRoutes(fastify: FastifyInstance): Promise<void> {
  // Get price history for a commodity
  fastify.get<{
    Querystring: { 
      commodity: string; 
      grade?: string; 
      location?: string; 
      from?: string; 
      to?: string; 
      limit?: string; 
    };
  }>('/api/v1/market/prices', async (request, reply) => {
    try {
      const { commodity, grade, location, from, to, limit } = request.query;
      
      if (!commodity) {
        throw new AppError('VALIDATION_ERROR', 'commodity is required', 400);
      }

      const commodityResult = await require('@apmc/auction').query('SELECT id FROM commodities WHERE code = $1', [commodity]);
      if (commodityResult.rows.length === 0) {
        throw new NotFoundError('Commodity', commodity);
      }
      const commodityId = commodityResult.rows[0].id;

      let gradeId: string | null = null;
      if (grade) {
        const gradeResult = await require('@apmc/auction').query(
          'SELECT g.id FROM grades g JOIN commodities c ON g.commodity_id = c.id WHERE c.code = $1 AND g.code = $2',
          [commodity, grade]
        );
        if (gradeResult.rows.length > 0) {
          gradeId = gradeResult.rows[0].id;
        }
      }

      let locationId: string | null = null;
      if (location) {
        const locationResult = await require('@apmc/auction').query('SELECT id FROM apmc_locations WHERE code = $1', [location]);
        if (locationResult.rows.length > 0) {
          locationId = locationResult.rows[0].id;
        }
      }

      const fromTime = from ? new Date(from) : new Date(Date.now() - 7 * 24 * 60 * 60 * 1000);
      const toTime = to ? new Date(to) : new Date();
      const limitNum = parseInt(limit || '100', 10);

      let sql = `
        SELECT ph.*, c.name as commodity, g.name as grade, l.code as location
        FROM price_history ph
        JOIN commodities c ON ph.commodity_id = c.id
        JOIN grades g ON ph.grade_id = g.id
        JOIN apmc_locations l ON ph.apmc_location_id = l.id
        WHERE ph.commodity_id = $1 AND ph.recorded_at BETWEEN $2 AND $3
      `;
      const params: any[] = [commodityId, fromTime, toTime];
      let paramIndex = 4;

      if (gradeId) {
        sql += ` AND ph.grade_id = $${paramIndex++}`;
        params.push(gradeId);
      }

      if (locationId) {
        sql += ` AND ph.apmc_location_id = $${paramIndex++}`;
        params.push(locationId);
      }

      sql += ` ORDER BY ph.recorded_at DESC LIMIT $${paramIndex}`;
      params.push(limitNum);

      const result = await require('@apmc/auction').query(sql, params);

      return {
        prices: result.rows.map((row: any) => ({
          commodity: row.commodity,
          grade: row.grade,
          price: parseFloat(row.price),
          volume: parseFloat(row.volume || 0),
          location: row.location,
          source: row.source,
          timestamp: row.recorded_at,
        })),
      };
    } catch (error) {
      if (!(error instanceof AppError)) logger.error({ err: error, url: request.url }, 'Failed to get prices');
      const appError = error instanceof AppError ? error : new AppError('INTERNAL_ERROR', 'Failed to get prices');
      return reply.code(appError.statusCode).send({ error: appError.message, code: appError.code });
    }
  });

  // Get commodity price index
  fastify.get<{
    Querystring: { commodity: string; grade?: string; };
  }>('/api/v1/market/index', async (request, reply) => {
    try {
      const { commodity, grade } = request.query;
      
      if (!commodity) {
        throw new AppError('VALIDATION_ERROR', 'commodity is required', 400);
      }

      const commodityResult = await require('@apmc/auction').query('SELECT id FROM commodities WHERE code = $1', [commodity]);
      if (commodityResult.rows.length === 0) {
        throw new NotFoundError('Commodity', commodity);
      }
      const commodityId = commodityResult.rows[0].id;

      let gradeId: string | null = null;
      if (grade) {
        const gradeResult = await require('@apmc/auction').query(
          'SELECT g.id FROM grades g JOIN commodities c ON g.commodity_id = c.id WHERE c.code = $1 AND g.code = $2',
          [commodity, grade]
        );
        if (gradeResult.rows.length > 0) {
          gradeId = gradeResult.rows[0].id;
        }
      }

      let sql = `
        SELECT 
          AVG(ph.price) as average_price,
          MIN(ph.price) as min_price,
          MAX(ph.price) as max_price,
          SUM(ph.volume) as total_volume,
          l.code as location,
          AVG(ph.price) as location_avg_price,
          SUM(ph.volume) as location_volume
        FROM price_history ph
        JOIN apmc_locations l ON ph.apmc_location_id = l.id
        WHERE ph.commodity_id = $1 AND ph.recorded_at > NOW() - INTERVAL '24 hours'
      `;
      const params: any[] = [commodityId];

      if (gradeId) {
        sql += ` AND ph.grade_id = $2`;
        params.push(gradeId);
      }

      sql += ` GROUP BY l.code ORDER BY location_avg_price DESC`;

      const result = await require('@apmc/auction').query(sql, params);

      const regionalPrices = result.rows.map((row: any) => ({
        apmc_location: row.location,
        price: parseFloat(row.location_avg_price),
        volume: parseFloat(row.location_volume || 0),
      }));

      const allPrices = result.rows.map((row: any) => parseFloat(row.location_avg_price));
      const averagePrice = allPrices.length > 0 ? allPrices.reduce((a: number, b: number) => a + b, 0) / allPrices.length : 0;
      const minPrice = allPrices.length > 0 ? Math.min(...allPrices) : 0;
      const maxPrice = allPrices.length > 0 ? Math.max(...allPrices) : 0;
      const totalVolume = result.rows.reduce((sum: number, row: any) => sum + parseFloat(row.location_volume || 0), 0);

      return {
        commodity,
        grade: grade || 'all',
        average_price: averagePrice,
        min_price: minPrice,
        max_price: maxPrice,
        volume: totalVolume,
        timestamp: now(),
        regional_prices: regionalPrices,
      };
    } catch (error) {
      if (!(error instanceof AppError)) logger.error({ err: error, url: request.url }, 'Failed to get index');
      const appError = error instanceof AppError ? error : new AppError('INTERNAL_ERROR', 'Failed to get index');
      return reply.code(appError.statusCode).send({ error: appError.message, code: appError.code });
    }
  });

  // Get market alerts
  fastify.get<{
    Querystring: { commodity?: string; unread_only?: string; limit?: string; };
  }>('/api/v1/market/alerts', async (request, reply) => {
    try {
      const { commodity, unread_only, limit } = request.query;
      
      let sql = `
        SELECT ma.*, c.name as commodity_name
        FROM market_alerts ma
        LEFT JOIN commodities c ON ma.commodity_id = c.id
        WHERE 1=1
      `;
      const params: any[] = [];
      let paramIndex = 1;

      if (commodity) {
        sql += ` AND c.code = $${paramIndex++}`;
        params.push(commodity);
      }

      if (unread_only === 'true') {
        sql += ` AND ma.is_read = false`;
      }

      sql += ` ORDER BY ma.created_at DESC LIMIT $${paramIndex}`;
      params.push(parseInt(limit || '50', 10));

      const result = await require('@apmc/auction').query(sql, params);

      return {
        alerts: result.rows.map((row: any) => ({
          alert_id: row.alert_id,
          type: row.type,
          commodity: row.commodity_name,
          message: row.message,
          metadata: row.metadata,
          is_read: row.is_read,
          created_at: row.created_at,
        })),
      };
    } catch (error) {
      if (!(error instanceof AppError)) logger.error({ err: error, url: request.url }, 'Failed to get alerts');
      const appError = error instanceof AppError ? error : new AppError('INTERNAL_ERROR', 'Failed to get alerts');
      return reply.code(appError.statusCode).send({ error: appError.message, code: appError.code });
    }
  });

  // Mark alert as read
  fastify.post<{
    Params: { alertId: string };
  }>('/api/v1/market/alerts/:alertId/read', async (request, reply) => {
    try {
      const { alertId } = request.params;
      
      await require('@apmc/auction').query(
        'UPDATE market_alerts SET is_read = true WHERE alert_id = $1',
        [alertId]
      );

      return { success: true };
    } catch (error) {
      if (!(error instanceof AppError)) logger.error({ err: error, url: request.url }, 'Failed to mark alert as read');
      const appError = error instanceof AppError ? error : new AppError('INTERNAL_ERROR', 'Failed to mark alert as read');
      return reply.code(appError.statusCode).send({ error: appError.message, code: appError.code });
    }
  });
}