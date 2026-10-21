import { config } from '@apmc/config';

// Set test environment
process.env.NODE_ENV = 'test';
process.env.DATABASE_URL = 'postgresql://apmc:apmc@localhost:5432/apmc_test';
process.env.REDIS_URL = 'redis://localhost:6379';
process.env.KAFKA_BROKERS = 'localhost:9092';
process.env.JWT_SECRET = 'test-secret-key-for-testing-only-min-32-chars';
process.env.LOG_LEVEL = 'silent';

// Extend default timeout
jest.setTimeout(30000);

// Global test utilities
global.testUtils = {
  waitFor: (condition: () => boolean, timeout = 5000, interval = 100) => {
    return new Promise<void>((resolve, reject) => {
      const start = Date.now();
      const check = () => {
        if (condition()) {
          resolve();
        } else if (Date.now() - start > timeout) {
          reject(new Error('Timeout waiting for condition'));
        } else {
          setTimeout(check, interval);
        }
      };
      check();
    });
  },
};

declare global {
  namespace jest {
    interface Globals {
      testUtils: {
        waitFor: (condition: () => boolean, timeout?: number, interval?: number) => Promise<void>;
      };
    }
  }
}