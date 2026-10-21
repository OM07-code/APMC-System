import { Metadata } from '@grpc/grpc-js';
import { config } from '@apmc/config';
import { AppError, AuthenticationError } from '@apmc/shared';
import { verify } from 'jsonwebtoken';

function getMetadataString(metadata: Metadata, key: string): string | undefined {
  const value = metadata.get(key)[0];
  if (value === undefined) return undefined;
  return Buffer.isBuffer(value) ? value.toString() : value;
}

export interface AuthenticatedMetadata extends Metadata {
  userId?: string;
  roles?: string[];
}

export function extractAuthMetadata(metadata: Metadata): AuthenticatedMetadata {
  const authMetadata = metadata as AuthenticatedMetadata;
  
  const authHeader = getMetadataString(metadata, 'authorization');
  if (authHeader?.startsWith('Bearer ')) {
    const token = authHeader.slice(7);
    try {
      const decoded = verify(token, config.JWT_SECRET) as { sub: string; roles?: string[] };
      authMetadata.userId = decoded.sub;
      authMetadata.roles = decoded.roles || [];
    } catch {
      // Invalid token - continue without auth
    }
  }
  
  return authMetadata;
}

export function requireAuth(metadata: Metadata): { userId: string; roles: string[] } {
  const authMetadata = extractAuthMetadata(metadata);
  if (!authMetadata.userId) {
    throw new AuthenticationError();
  }
  return { userId: authMetadata.userId, roles: authMetadata.roles || [] };
}

export function requireRole(metadata: Metadata, ...roles: string[]): { userId: string; roles: string[] } {
  const auth = requireAuth(metadata);
  if (!auth.roles.some(r => roles.includes(r))) {
    throw new AppError('FORBIDDEN', 'Insufficient permissions', 403);
  }
  return auth;
}

export function createRateLimitChecker(maxRequests: number, windowMs: number) {
  const requests = new Map<string, { count: number; resetAt: number }>();
  
  return (metadata: Metadata): boolean => {
    const clientId = getMetadataString(metadata, 'x-client-id') || 
                     getMetadataString(metadata, 'x-forwarded-for') || 'unknown';
    const now = Date.now();
    
    const record = requests.get(clientId);
    if (!record || now > record.resetAt) {
      requests.set(clientId, { count: 1, resetAt: now + windowMs });
      return true;
    } else if (record.count >= maxRequests) {
      return false;
    } else {
      record.count++;
      return true;
    }
  };
}

export function addTraceHeaders(metadata: Metadata): void {
  const traceId = getMetadataString(metadata, 'x-trace-id') || `trace-${Date.now()}-${Math.random().toString(36).slice(2)}`;
  const spanId = `span-${Date.now()}-${Math.random().toString(36).slice(2)}`;
  
  metadata.set('x-trace-id', traceId);
  metadata.set('x-span-id', spanId);
}