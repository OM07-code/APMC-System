import { sleep } from '@apmc/shared';

export class TimeoutError extends Error {
  constructor(message: string = 'Operation timed out') {
    super(message);
    this.name = 'TimeoutError';
  }
}

export interface TimeoutOptions {
  timeout: number;
  onTimeout?: () => void;
}

export async function withTimeout<T>(
  operation: () => Promise<T>,
  options: TimeoutOptions
): Promise<T> {
  let timeoutId: NodeJS.Timeout;
  let timedOut = false;

  const timeoutPromise = new Promise<never>((_, reject) => {
    timeoutId = setTimeout(() => {
      timedOut = true;
      options.onTimeout?.();
      reject(new TimeoutError(`Operation timed out after ${options.timeout}ms`));
    }, options.timeout);
  });

  try {
    const result = await Promise.race([operation(), timeoutPromise]);
    clearTimeout(timeoutId!);
    return result;
  } catch (error) {
    clearTimeout(timeoutId!);
    if (timedOut) {
      throw new TimeoutError(`Operation timed out after ${options.timeout}ms`);
    }
    throw error;
  }
}

export function createTimeoutSignal(timeout: number): AbortSignal {
  const controller = new AbortController();
  setTimeout(() => controller.abort(), timeout);
  return controller.signal;
}

export async function withTimeoutAndRetry<T>(
  operation: (signal: AbortSignal) => Promise<T>,
  timeout: number,
  maxRetries: number = 3,
  baseDelay: number = 1000
): Promise<T> {
  let lastError: Error | undefined;

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    const signal = createTimeoutSignal(timeout);
    
    try {
      return await operation(signal);
    } catch (error) {
      lastError = error instanceof Error ? error : new Error(String(error));
      
      if (error instanceof TimeoutError || signal.aborted) {
        if (attempt === maxRetries) throw lastError;
        await sleep(baseDelay * Math.pow(2, attempt - 1));
        continue;
      }
      
      throw error;
    }
  }

  throw lastError;
}