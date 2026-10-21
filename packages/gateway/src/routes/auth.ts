import { FastifyInstance } from 'fastify';
import { config } from '@apmc/config';
import { logger } from '@apmc/middleware';
import { AppError, ValidationError, AuthenticationError } from '@apmc/shared';
import { generateId } from '@apmc/shared';
import { sign, verify, SignOptions } from 'jsonwebtoken';
import { query, transaction } from '@apmc/auction';
import bcrypt from 'bcryptjs';

const JWT_SECRET = config.JWT_SECRET || 'default-secret-change-in-production';

// Parse TTL strings like "15m" to seconds
function parseTTL(ttl: string): number {
  const match = ttl.match(/^(\d+)([smhd])$/);
  if (!match) return 900; // default 15m
  const value = parseInt(match[1] || '0', 10);
  const unit = match[2] || 's';
  switch (unit) {
    case 's': return value;
    case 'm': return value * 60;
    case 'h': return value * 3600;
    case 'd': return value * 86400;
    default: return 900;
  }
}

const ACCESS_TOKEN_TTL = parseTTL(config.JWT_ACCESS_TTL || '15m');
const REFRESH_TOKEN_TTL = parseTTL(config.JWT_REFRESH_TTL || '7d');

interface TokenPayload {
  sub: string;
  email: string;
  role: string;
  type: 'access' | 'refresh';
}

export function createAccessToken(user: { id: string; email: string; role: string }): string {
  const options: SignOptions = { expiresIn: ACCESS_TOKEN_TTL };
  return sign(
    { sub: user.id, email: user.email, role: user.role, type: 'access' },
    JWT_SECRET,
    options
  );
}

export function createRefreshToken(user: { id: string; email: string; role: string }): string {
  const options: SignOptions = { expiresIn: REFRESH_TOKEN_TTL };
  return sign(
    { sub: user.id, email: user.email, role: user.role, type: 'refresh' },
    JWT_SECRET,
    options
  );
}

export function verifyToken(token: string): TokenPayload | null {
  try {
    return verify(token, JWT_SECRET) as TokenPayload;
  } catch {
    return null;
  }
}

export async function authRoutes(fastify: FastifyInstance): Promise<void> {
  // Register
  fastify.post<{
    Body: { email: string; password: string; full_name: string; role: string; phone?: string; };
  }>('/api/v1/auth/register', async (request, reply) => {
    try {
      const { email, password, full_name, role, phone } = request.body;
      
      if (!email || !password || !full_name || !role) {
        throw new ValidationError('Missing required fields');
      }
      
      if (!['farmer', 'buyer', 'auctioneer'].includes(role)) {
        throw new ValidationError('Invalid role');
      }
      
      const existing = await query('SELECT id FROM users WHERE email = $1', [email]);
      if (existing.rows.length > 0) {
        throw new AppError('CONFLICT', 'Email already registered', 409);
      }
      
      const passwordHash = await bcrypt.hash(password, 10);
      
      const result = await query(
        `INSERT INTO users (email, password_hash, full_name, role, phone, kyc_status)
         VALUES ($1, $2, $3, $4, $5, 'pending')
         RETURNING id, email, full_name, role, kyc_status, created_at`,
        [email, passwordHash, full_name, role, phone || null]
      );
      
      const user = result.rows[0];
      const accessToken = createAccessToken({ id: user.id, email: user.email, role: user.role });
      const refreshToken = createRefreshToken({ id: user.id, email: user.email, role: user.role });
      
      return reply.code(201).send({
        user: {
          id: user.id,
          email: user.email,
          full_name: user.full_name,
          role: user.role,
          kyc_status: user.kyc_status,
        },
        access_token: accessToken,
        refresh_token: refreshToken,
      });
    } catch (error) {
      if (!(error instanceof AppError)) logger.error({ err: error, url: request.url }, 'Registration failed');
      const appError = error instanceof AppError ? error : new AppError('INTERNAL_ERROR', 'Registration failed');
      return reply.code(appError.statusCode).send({ error: appError.message, code: appError.code });
    }
  });

  // Login
  fastify.post<{
    Body: { email: string; password: string; };
  }>('/api/v1/auth/login', async (request, reply) => {
    try {
      const { email, password } = request.body;
      
      if (!email || !password) {
        throw new ValidationError('Email and password are required');
      }
      
      const result = await query('SELECT * FROM users WHERE email = $1', [email]);
      if (result.rows.length === 0) {
        throw new AuthenticationError('Invalid credentials');
      }
      
      const user = result.rows[0];
      const validPassword = await bcrypt.compare(password, user.password_hash);
      if (!validPassword) {
        throw new AuthenticationError('Invalid credentials');
      }
      
      const accessToken = createAccessToken({ id: user.id, email: user.email, role: user.role });
      const refreshToken = createRefreshToken({ id: user.id, email: user.email, role: user.role });
      
      return {
        user: {
          id: user.id,
          email: user.email,
          full_name: user.full_name,
          role: user.role,
          kyc_status: user.kyc_status,
        },
        access_token: accessToken,
        refresh_token: refreshToken,
      };
    } catch (error) {
      if (!(error instanceof AppError)) logger.error({ err: error, url: request.url }, 'Login failed');
      const appError = error instanceof AppError ? error : new AppError('INTERNAL_ERROR', 'Login failed');
      return reply.code(appError.statusCode).send({ error: appError.message, code: appError.code });
    }
  });

  // Refresh token
  fastify.post<{
    Body: { refresh_token: string; };
  }>('/api/v1/auth/refresh', async (request, reply) => {
    try {
      const { refresh_token } = request.body;
      
      if (!refresh_token) {
        throw new ValidationError('Refresh token is required');
      }
      
      const payload = verifyToken(refresh_token);
      if (!payload || payload.type !== 'refresh') {
        throw new AuthenticationError('Invalid refresh token');
      }
      
      const userResult = await query('SELECT id, email, role FROM users WHERE id = $1', [payload.sub]);
      if (userResult.rows.length === 0) {
        throw new AuthenticationError('User not found');
      }
      
      const user = userResult.rows[0];
      const accessToken = createAccessToken({ id: user.id, email: user.email, role: user.role });
      const newRefreshToken = createRefreshToken({ id: user.id, email: user.email, role: user.role });
      
      return { access_token: accessToken, refresh_token: newRefreshToken };
    } catch (error) {
      if (!(error instanceof AppError)) logger.error({ err: error, url: request.url }, 'Token refresh failed');
      const appError = error instanceof AppError ? error : new AppError('INTERNAL_ERROR', 'Token refresh failed');
      return reply.code(appError.statusCode).send({ error: appError.message, code: appError.code });
    }
  });

  // Get current user
  fastify.get('/api/v1/auth/me', async (request, reply) => {
    try {
      const authHeader = request.headers.authorization;
      if (!authHeader?.startsWith('Bearer ')) {
        throw new AuthenticationError();
      }
      
      const token = authHeader.slice(7);
      const payload = verifyToken(token);
      if (!payload || payload.type !== 'access') {
        throw new AuthenticationError('Invalid access token');
      }
      
      const userResult = await query(
        'SELECT id, email, full_name, role, phone, kyc_status, created_at FROM users WHERE id = $1',
        [payload.sub]
      );
      
      if (userResult.rows.length === 0) {
        throw new AuthenticationError('User not found');
      }
      
      return { user: userResult.rows[0] };
    } catch (error) {
      if (!(error instanceof AppError)) logger.error({ err: error, url: request.url }, 'Failed to get user');
      const appError = error instanceof AppError ? error : new AppError('INTERNAL_ERROR', 'Failed to get user');
      return reply.code(appError.statusCode).send({ error: appError.message, code: appError.code });
    }
  });

  // Logout (client-side only, but we can blacklist tokens if needed)
  fastify.post('/api/v1/auth/logout', async (request, reply) => {
    return { success: true, message: 'Logged out successfully' };
  });
}

export function authenticate(fastify: FastifyInstance): void {
  fastify.addHook('preHandler', async (request, reply) => {
    // Skip auth for public routes
    const publicPaths = ['/health', '/ready', '/metrics', '/api/v1/auth/login', '/api/v1/auth/register', '/api/v1/auth/refresh'];
    if (publicPaths.some(p => request.url.startsWith(p))) {
      return;
    }
    
    // Skip auth for WebSocket upgrades
    if (request.url.startsWith('/ws/')) {
      return;
    }
    
    const authHeader = request.headers.authorization;
    if (!authHeader?.startsWith('Bearer ')) {
      return reply.code(401).send({ error: 'Authentication required', code: 'AUTHENTICATION_ERROR' });
    }
    
    const token = authHeader.slice(7);
    const payload = verifyToken(token);
    if (!payload || payload.type !== 'access') {
      return reply.code(401).send({ error: 'Invalid or expired token', code: 'AUTHENTICATION_ERROR' });
    }
    
    // Attach user to request
    (request as any).user = { id: payload.sub, email: payload.email, role: payload.role };
  });
}

export function authorize(...roles: string[]): (fastify: FastifyInstance) => void {
  return (fastify) => {
    fastify.addHook('preHandler', async (request, reply) => {
      const user = (request as any).user;
      if (!user || !roles.includes(user.role)) {
        return reply.code(403).send({ error: 'Insufficient permissions', code: 'AUTHORIZATION_ERROR' });
      }
    });
  };
}