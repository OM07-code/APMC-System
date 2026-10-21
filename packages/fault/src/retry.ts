import { sleep } from '@apmc/shared';

export interface RetryOptions {
  maxAttempts: number;
  baseDelay: number;
  maxDelay: number;
  backoff?: 'exponential' | 'linear' | 'constant';
  jitter?: boolean;
  retryable?: (error: unknown) => boolean;
  onRetry?: (error: unknown, attempt: number) => void;
}

export interface RetryResult<T> {
  success: boolean;
  result?: T;
  error?: Error;
  attempts: number;
  totalTime: number;
}

export async function withRetry<T>(
  operation: () => Promise<T>,
  options: Partial<RetryOptions> = {}
): Promise<RetryResult<T>> {
  const opts: Required<RetryOptions> = {
    maxAttempts: options.maxAttempts ?? 3,
    baseDelay: options.baseDelay ?? 1000,
    maxDelay: options.maxDelay ?? 10000,
    backoff: options.backoff ?? 'exponential',
    jitter: options.jitter ?? true,
    retryable: options.retryable ?? (() => true),
    onRetry: options.onRetry ?? (() => {}),
  };

  const startTime = Date.now();
  let lastError: Error | undefined;

  for (let attempt = 1; attempt <= opts.maxAttempts; attempt++) {
    try {
      const result = await operation();
      return {
        success: true,
        result,
        attempts: attempt,
        totalTime: Date.now() - startTime,
      };
    } catch (error) {
      lastError = error instanceof Error ? error : new Error(String(error));
      
      if (attempt === opts.maxAttempts || !opts.retryable(lastError)) {
        return {
          success: false,
          error: lastError,
          attempts: attempt,
          totalTime: Date.now() - startTime,
        };
      }

      opts.onRetry(lastError, attempt);

      const delay = calculateDelay(attempt, opts);
      await sleep(delay);
    }
  }

  return {
    success: false,
    error: lastError,
    attempts: opts.maxAttempts,
    totalTime: Date.now() - startTime,
  };
}

function calculateDelay(attempt: number, options: Required<RetryOptions>): number {
  let delay: number;
  
  switch (options.backoff) {
    case 'exponential':
      delay = options.baseDelay * Math.pow(2, attempt - 1);
      break;
    case 'linear':
      delay = options.baseDelay * attempt;
      break;
    case 'constant':
    default:
      delay = options.baseDelay;
      break;
  }
  
  delay = Math.min(delay, options.maxDelay);
  
  if (options.jitter) {
    delay = delay * (0.5 + Math.random() * 0.5);
  }
  
  return Math.floor(delay);
}

export function createRetryableErrorCheck(...errorCodes: string[]): (error: unknown) => boolean {
  return (error: unknown) => {
    if (error instanceof Error) {
      const appError = error as { code?: string };
      return errorCodes.includes(appError.code || '');
    }
    return false;
  };
}

export const retryableErrors = createRetryableErrorCheck(
  'SERVICE_UNAVAILABLE',
  'TIMEOUT',
  'INTERNAL_ERROR',
  'NETWORK_ERROR',
  'ECONNREFUSED',
  'ETIMEDOUT',
  'ENOTFOUND'
);