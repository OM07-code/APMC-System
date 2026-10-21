export interface BulkheadOptions {
  maxConcurrent: number;
  queueSize: number;
  timeout?: number;
}

export interface BulkheadStats {
  running: number;
  queued: number;
  maxConcurrent: number;
  queueSize: number;
}

export class BulkheadRejectedError extends Error {
  constructor(message: string = 'Bulkhead queue full') {
    super(message);
    this.name = 'BulkheadRejectedError';
  }
}

export class BulkheadTimeoutError extends Error {
  constructor(message: string = 'Bulkhead execution timeout') {
    super(message);
    this.name = 'BulkheadTimeoutError';
  }
}

export class Bulkhead {
  private running = 0;
  private queue: Array<{
    resolve: (value: unknown) => void;
    reject: (reason: Error) => void;
    operation: () => Promise<unknown>;
  }> = [];
  private readonly options: Required<BulkheadOptions>;

  constructor(options: BulkheadOptions) {
    this.options = {
      maxConcurrent: options.maxConcurrent,
      queueSize: options.queueSize,
      timeout: options.timeout ?? 30000,
    };
  }

  async execute<T>(operation: () => Promise<T>): Promise<T> {
    if (this.running >= this.options.maxConcurrent) {
      if (this.queue.length >= this.options.queueSize) {
        throw new BulkheadRejectedError();
      }
      
      return new Promise((resolve, reject) => {
        this.queue.push({
          resolve: resolve as (value: unknown) => void,
          reject,
          operation: operation as () => Promise<unknown>,
        });
      });
    }

    return this.runOperation(operation);
  }

  private async runOperation<T>(operation: () => Promise<T>): Promise<T> {
    this.running++;
    
    try {
      const result = await Promise.race([
        operation(),
        new Promise<never>((_, reject) => {
          setTimeout(() => reject(new BulkheadTimeoutError()), this.options.timeout);
        }),
      ]);
      
      this.running--;
      this.processQueue();
      return result;
    } catch (error) {
      this.running--;
      this.processQueue();
      throw error;
    }
  }

  private processQueue(): void {
    if (this.queue.length === 0 || this.running >= this.options.maxConcurrent) {
      return;
    }

    const next = this.queue.shift()!;
    this.running++;
    
    Promise.resolve()
      .then(() => next.operation())
      .then(next.resolve)
      .catch(next.reject)
      .finally(() => {
        this.running--;
        this.processQueue();
      });
  }

  getStats(): BulkheadStats {
    return {
      running: this.running,
      queued: this.queue.length,
      maxConcurrent: this.options.maxConcurrent,
      queueSize: this.options.queueSize,
    };
  }

  isAvailable(): boolean {
    return this.running < this.options.maxConcurrent || this.queue.length < this.options.queueSize;
  }

  clearQueue(): number {
    const rejected = this.queue.length;
    for (const item of this.queue) {
      item.reject(new BulkheadRejectedError('Bulkhead cleared'));
    }
    this.queue = [];
    return rejected;
  }
}

export class BulkheadRegistry {
  private bulkheads = new Map<string, Bulkhead>();

  getOrCreate(name: string, options: BulkheadOptions): Bulkhead {
    let bulkhead = this.bulkheads.get(name);
    if (!bulkhead) {
      bulkhead = new Bulkhead(options);
      this.bulkheads.set(name, bulkhead);
    }
    return bulkhead;
  }

  get(name: string): Bulkhead | undefined {
    return this.bulkheads.get(name);
  }

  remove(name: string): boolean {
    return this.bulkheads.delete(name);
  }

  getAllStats(): Record<string, BulkheadStats> {
    const stats: Record<string, BulkheadStats> = {};
    for (const [name, bulkhead] of this.bulkheads) {
      stats[name] = bulkhead.getStats();
    }
    return stats;
  }
}

export const bulkheadRegistry = new BulkheadRegistry();