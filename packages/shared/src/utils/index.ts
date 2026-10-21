import { randomUUID } from 'crypto';

export function generateId(prefix?: string): string {
  const uuid = randomUUID().replace(/-/g, '');
  return prefix ? `${prefix}_${uuid}` : uuid;
}

export function generateAuctionId(): string {
  return generateId('auc');
}

export function generateBidId(): string {
  return generateId('bid');
}

export function generatePeerId(): string {
  return generateId('peer');
}

export function generateAlertId(): string {
  return generateId('alert');
}

export function generateEventId(): string {
  return generateId('evt');
}

export function now(): number {
  return Date.now();
}

export function nowISO(): string {
  return new Date().toISOString();
}

export function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

export async function retry<T>(
  fn: () => Promise<T>,
  options: {
    maxAttempts: number;
    baseDelay: number;
    maxDelay: number;
    backoff?: 'exponential' | 'linear';
    retryable?: (error: unknown) => boolean;
  }
): Promise<T> {
  const { maxAttempts, baseDelay, maxDelay, backoff = 'exponential', retryable = () => true } = options;
  
  let lastError: unknown;
  
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      return await fn();
    } catch (error) {
      lastError = error;
      if (attempt === maxAttempts || !retryable(error)) {
        throw error;
      }
      const delay = backoff === 'exponential' 
        ? Math.min(baseDelay * Math.pow(2, attempt - 1), maxDelay)
        : Math.min(baseDelay * attempt, maxDelay);
      await sleep(delay);
    }
  }
  
  throw lastError;
}

export function debounce<T extends (...args: unknown[]) => unknown>(
  fn: T,
  delay: number
): (...args: Parameters<T>) => void {
  let timeoutId: NodeJS.Timeout | null = null;
  return (...args: Parameters<T>) => {
    if (timeoutId) clearTimeout(timeoutId);
    timeoutId = setTimeout(() => fn(...args), delay);
  };
}

export function throttle<T extends (...args: unknown[]) => unknown>(
  fn: T,
  limit: number
): (...args: Parameters<T>) => void {
  let inThrottle = false;
  return (...args: Parameters<T>) => {
    if (!inThrottle) {
      fn(...args);
      inThrottle = true;
      setTimeout(() => (inThrottle = false), limit);
    }
  };
}

export function clamp(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, value));
}

export function parseIntSafe(value: string | number | undefined, fallback: number = 0): number {
  if (value === undefined || value === null) return fallback;
  const parsed = typeof value === 'string' ? parseInt(value, 10) : value;
  return Number.isNaN(parsed) ? fallback : parsed;
}

export function parseFloatSafe(value: string | number | undefined, fallback: number = 0): number {
  if (value === undefined || value === null) return fallback;
  const parsed = typeof value === 'string' ? parseFloat(value) : value;
  return Number.isNaN(parsed) ? fallback : parsed;
}

export function sanitizeString(input: string, maxLength: number = 256): string {
  return input.trim().slice(0, maxLength);
}

export function chunkArray<T>(array: T[], size: number): T[][] {
  const chunks: T[][] = [];
  for (let i = 0; i < array.length; i += size) {
    chunks.push(array.slice(i, i + size));
  }
  return chunks;
}

export function pick<T extends Record<string, unknown>, K extends keyof T>(
  obj: T,
  keys: K[]
): Pick<T, K> {
  const result = {} as Pick<T, K>;
  for (const key of keys) {
    if (key in obj) result[key] = obj[key];
  }
  return result;
}

export function omit<T extends Record<string, unknown>, K extends keyof T>(
  obj: T,
  keys: K[]
): Omit<T, K> {
  const result = { ...obj };
  for (const key of keys) {
    delete result[key];
  }
  return result;
}