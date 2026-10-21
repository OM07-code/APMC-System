import { jest } from '@jest/globals';
import { generateId, generateAuctionId, generateBidId, now, sleep, retry } from '@apmc/shared';
import { AppError, ValidationError, NotFoundError, ConflictError, toAppError, isAppError } from '@apmc/shared';
import { CircuitBreaker, CircuitState, circuitBreakerRegistry } from '@apmc/fault';
import { Bulkhead, bulkheadRegistry, BulkheadRejectedError } from '@apmc/fault';
import { withTimeout, TimeoutError } from '@apmc/fault';
import { InMemoryNamingService, createServiceInstance } from '@apmc/naming';
import { KademliaDHT } from '@apmc/p2p';
import { BullyLeaderElection, NodeState } from '@apmc/p2p';
import { GossipProtocol } from '@apmc/p2p';

describe('Shared Utilities', () => {
  describe('ID Generation', () => {
    it('should generate unique IDs', () => {
      const ids = new Set<string>();
      for (let i = 0; i < 1000; i++) {
        ids.add(generateId());
      }
      expect(ids.size).toBe(1000);
    });

    it('should generate prefixed IDs', () => {
      const id = generateId('test');
      expect(id).toMatch(/^test_[a-f0-9]{32}$/);
    });

    it('should generate auction IDs', () => {
      const id = generateAuctionId();
      expect(id).toMatch(/^auc_[a-f0-9]{32}$/);
    });

    it('should generate bid IDs', () => {
      const id = generateBidId();
      expect(id).toMatch(/^bid_[a-f0-9]{32}$/);
    });
  });

  describe('Time utilities', () => {
    it('should return current timestamp', () => {
      const before = Date.now();
      const ts = now();
      const after = Date.now();
      expect(ts).toBeGreaterThanOrEqual(before);
      expect(ts).toBeLessThanOrEqual(after);
    });
  });

  describe('Sleep', () => {
    it('should sleep for specified time', async () => {
      const start = Date.now();
      await sleep(50);
      const elapsed = Date.now() - start;
      expect(elapsed).toBeGreaterThanOrEqual(40);
    });
  });

  describe('Retry', () => {
    it('should succeed on first attempt', async () => {
      const result = await retry(async () => 'success', { maxAttempts: 3, baseDelay: 10, maxDelay: 100 });
      expect(result).toBe('success');
    });

    it('should retry on failure', async () => {
      let attempts = 0;
      await expect(
        retry(async () => {
          attempts++;
          if (attempts < 3) throw new Error('fail');
          return 'success';
        }, { maxAttempts: 3, baseDelay: 10, maxDelay: 100 })
      ).rejects.toThrow();
    });

    it('should not retry non-retryable errors', async () => {
      let attempts = 0;
      await expect(
        retry(async () => {
          attempts++;
          throw new ValidationError('invalid');
        }, { 
          maxAttempts: 3, 
          baseDelay: 10, 
          maxDelay: 100,
          retryable: () => false
        })
      ).rejects.toThrow(ValidationError);
      expect(attempts).toBe(1);
    });
  });
});

describe('Error Handling', () => {
  it('should create AppError with correct properties', () => {
    const error = new AppError('TEST_ERROR', 'Test message', 400, { detail: 'test' });
    expect(error.code).toBe('TEST_ERROR');
    expect(error.message).toBe('Test message');
    expect(error.statusCode).toBe(400);
    expect(error.details).toEqual({ detail: 'test' });
  });

  it('should create specific error types', () => {
    expect(new ValidationError('invalid').statusCode).toBe(400);
    expect(new NotFoundError('User', '123').statusCode).toBe(404);
    expect(new ConflictError('conflict').statusCode).toBe(409);
    expect(new AppError('INTERNAL', 'internal').statusCode).toBe(500);
  });

  it('should identify AppError', () => {
    expect(isAppError(new AppError('TEST', 'test'))).toBe(true);
    expect(isAppError(new Error('test'))).toBe(false);
    expect(isAppError('string')).toBe(false);
  });

  it('should convert unknown errors to AppError', () => {
    const error = toAppError(new Error('original'));
    expect(error).toBeInstanceOf(AppError);
    expect(error.code).toBe('INTERNAL_ERROR');
    expect(error.message).toBe('original');
  });
});

describe('Circuit Breaker', () => {
  let breaker: CircuitBreaker;

  beforeEach(() => {
    breaker = new CircuitBreaker({
      failureThreshold: 3,
      successThreshold: 2,
      timeout: 1000,
      resetTimeout: 1000,
    });
  });

  it('should start in CLOSED state', () => {
    expect(breaker.getState()).toBe(CircuitState.CLOSED);
  });

  it('should execute successful operation', async () => {
    const result = await breaker.execute(async () => 'success');
    expect(result).toBe('success');
  });

  it('should open after failure threshold', async () => {
    for (let i = 0; i < 3; i++) {
      try {
        await breaker.execute(async () => { throw new Error('fail'); });
      } catch {}
    }
    expect(breaker.getState()).toBe(CircuitState.OPEN);
  });

  it('should reject when OPEN', async () => {
    breaker.forceOpen();
    await expect(breaker.execute(async () => 'success')).rejects.toThrow();
  });

  it('should transition to HALF_OPEN after reset timeout', async () => {
    breaker.forceOpen();
    await sleep(1100);
    expect(breaker.getState()).toBe(CircuitState.HALF_OPEN);
  });

  it('should close after success threshold in HALF_OPEN', async () => {
    breaker.forceOpen();
    await sleep(1100);
    
    await breaker.execute(async () => 'success');
    await breaker.execute(async () => 'success');
    
    expect(breaker.getState()).toBe(CircuitState.CLOSED);
  });

  it('should provide stats', () => {
    const stats = breaker.getStats();
    expect(stats).toHaveProperty('state');
    expect(stats).toHaveProperty('failures');
    expect(stats).toHaveProperty('successes');
  });
});

describe('Circuit Breaker Registry', () => {
  it('should create and retrieve breakers', () => {
    const breaker1 = circuitBreakerRegistry.getOrCreate('test1');
    const breaker2 = circuitBreakerRegistry.getOrCreate('test1');
    expect(breaker1).toBe(breaker2);
    
    const breaker3 = circuitBreakerRegistry.getOrCreate('test2');
    expect(breaker3).not.toBe(breaker1);
  });

  it('should get all stats', () => {
    circuitBreakerRegistry.getOrCreate('stat1');
    circuitBreakerRegistry.getOrCreate('stat2');
    const stats = circuitBreakerRegistry.getAllStats();
    expect(Object.keys(stats)).toContain('stat1');
    expect(Object.keys(stats)).toContain('stat2');
  });
});

describe('Bulkhead', () => {
  let bulkhead: Bulkhead;

  beforeEach(() => {
    bulkhead = new Bulkhead({ maxConcurrent: 2, queueSize: 2 });
  });

  it('should execute when under limit', async () => {
    const result = await bulkhead.execute(async () => 'success');
    expect(result).toBe('success');
  });

  it('should queue when at limit', async () => {
    const promises = [
      bulkhead.execute(async () => { await sleep(50); return 1; }),
      bulkhead.execute(async () => { await sleep(50); return 2; }),
      bulkhead.execute(async () => 3), // This should queue
    ];
    
    const results = await Promise.all(promises);
    expect(results).toEqual([1, 2, 3]);
  });

  it('should reject when queue full', async () => {
    const promises = [
      bulkhead.execute(async () => { await sleep(100); return 1; }),
      bulkhead.execute(async () => { await sleep(100); return 2; }),
      bulkhead.execute(async () => { await sleep(100); return 3; }),
      bulkhead.execute(async () => 4), // This should reject
    ];
    
    await expect(Promise.all(promises)).rejects.toThrow(BulkheadRejectedError);
  });

  it('should provide stats', () => {
    const stats = bulkhead.getStats();
    expect(stats.maxConcurrent).toBe(2);
    expect(stats.queueSize).toBe(2);
  });
});

describe('Bulkhead Registry', () => {
  it('should create and retrieve bulkheads', () => {
    const bh1 = bulkheadRegistry.getOrCreate('test1', { maxConcurrent: 10, queueSize: 5 });
    const bh2 = bulkheadRegistry.getOrCreate('test1', { maxConcurrent: 10, queueSize: 5 });
    expect(bh1).toBe(bh2);
  });
});

describe('Timeout', () => {
  it('should resolve before timeout', async () => {
    const result = await withTimeout(async () => {
      await sleep(10);
      return 'success';
    }, { timeout: 100 });
    expect(result).toBe('success');
  });

  it('should reject on timeout', async () => {
    await expect(
      withTimeout(async () => {
        await sleep(100);
        return 'success';
      }, { timeout: 10 })
    ).rejects.toThrow(TimeoutError);
  });
});

describe('Naming Service', () => {
  let naming: InMemoryNamingService;

  beforeEach(() => {
    naming = new InMemoryNamingService(5000);
  });

  afterEach(() => {
    naming.shutdown();
  });

  it('should register and lookup services', async () => {
    const instance = createServiceInstance('test-service', 'localhost', 5000, { version: '1.0' });
    await naming.register(instance);
    
    const results = await naming.lookup({ name: 'test-service' });
    expect(results.length).toBe(1);
    expect(results[0].id).toBe(instance.id);
    expect(results[0].metadata.version).toBe('1.0');
  });

  it('should deregister services', async () => {
    const instance = createServiceInstance('test-service', 'localhost', 5000);
    await naming.register(instance);
    await naming.deregister(instance.id);
    
    const results = await naming.lookup({ name: 'test-service' });
    expect(results.length).toBe(0);
  });

  it('should handle heartbeats', async () => {
    const instance = createServiceInstance('test-service', 'localhost', 5000);
    await naming.register(instance);
    
    await naming.heartbeat(instance.id);
    
    const results = await naming.lookup({ name: 'test-service', health: 'healthy' });
    expect(results.length).toBe(1);
    expect(results[0].health).toBe('healthy');
  });

  it('should watch for changes', async () => {
    const changes: any[] = [];
    const unwatch = naming.watch({ name: 'test-service' }, (instances) => {
      changes.push(instances);
    });
    
    const instance = createServiceInstance('test-service', 'localhost', 5000);
    await naming.register(instance);
    
    await sleep(100);
    expect(changes.length).toBeGreaterThan(0);
    
    unwatch();
  });

  it('should filter by metadata', async () => {
    await naming.register(createServiceInstance('svc', 'localhost', 5000, { env: 'prod' }));
    await naming.register(createServiceInstance('svc', 'localhost', 5001, { env: 'dev' }));
    
    const prod = await naming.lookup({ name: 'svc', metadata: { env: 'prod' } });
    expect(prod.length).toBe(1);
    expect(prod[0].metadata.env).toBe('prod');
  });
});

describe('Kademlia DHT', () => {
  let dht: KademliaDHT;

  beforeEach(() => {
    dht = new KademliaDHT('local-node-id');
  });

  it('should calculate XOR distance', () => {
    const dist = KademliaDHT.distance('a'.repeat(40), 'b'.repeat(40));
    expect(dist).toBeGreaterThan(0);
  });

  it('should store and retrieve values', async () => {
    const key = 'test-key';
    const value = Buffer.from('test-value');
    
    await dht.put(key, value);
    const retrieved = await dht.get(key);
    
    expect(retrieved).toEqual(value);
  });

  it('should return null for missing keys', async () => {
    const value = await dht.get('non-existent');
    expect(value).toBeNull();
  });

  it('should expire values after TTL', async () => {
    const dht2 = new KademliaDHT('node2');
    await dht2.put('key', Buffer.from('value'), 10); // 10ms TTL
    
    await sleep(20);
    const value = await dht2.get('key');
    expect(value).toBeNull();
  });

  it('should find closest nodes', () => {
    const node1: any = { id: '1'.repeat(40), host: 'localhost', port: 5000, lastSeen: Date.now(), isSelf: false };
    const node2: any = { id: '2'.repeat(40), host: 'localhost', port: 5001, lastSeen: Date.now(), isSelf: false };
    
    dht.addNode(node1);
    dht.addNode(node2);
    
    const closest = dht.findClosestNodes('1'.repeat(40), 1);
    expect(closest.length).toBe(1);
    expect(closest[0].id).toBe(node1.id);
  });
});

describe('Bully Leader Election', () => {
  let election: BullyLeaderElection;

  beforeEach(() => {
    election = new BullyLeaderElection('node-1', 'localhost', 5000, 1);
  });

  afterEach(() => {
    election.shutdown();
  });

  it('should start as FOLLOWER', () => {
    expect(election.getState()).toBe(NodeState.FOLLOWER);
  });

  it('should become leader when no higher priority nodes', async () => {
    election.addNode({ id: 'node-2', host: 'localhost', port: 5001, priority: 0, lastHeartbeat: Date.now(), state: NodeState.FOLLOWER });
    
    await election.startElection();
    
    expect(election.getState()).toBe(NodeState.LEADER);
    expect(election.getCurrentLeader()).toBe('node-1');
  });

  it('should handle incoming ELECTION message', async () => {
    const higherPriorityElection = new BullyLeaderElection('node-2', 'localhost', 5001, 10);
    higherPriorityElection.addNode({ id: 'node-1', host: 'localhost', port: 5000, priority: 1, lastHeartbeat: Date.now(), state: NodeState.FOLLOWER });
    
    // Simulate node-1 sending ELECTION to node-2
    higherPriorityElection.simulateMessage({
      type: 'ELECTION',
      from: 'node-1',
      timestamp: Date.now(),
      priority: 1,
    });
    
    // node-2 should respond with OK and start its own election
    await sleep(200);
    expect(higherPriorityElection.getState()).toBe(NodeState.LEADER);
    
    higherPriorityElection.shutdown();
  });

  it('should handle COORDINATOR message', async () => {
    election.simulateMessage({
      type: 'COORDINATOR',
      from: 'node-2',
      timestamp: Date.now(),
    });
    
    expect(election.getCurrentLeader()).toBe('node-2');
    expect(election.getState()).toBe(NodeState.FOLLOWER);
  });
});

describe('Gossip Protocol', () => {
  let gossip1: GossipProtocol;
  let gossip2: GossipProtocol;

  beforeEach(() => {
    gossip1 = new GossipProtocol('node-1');
    gossip2 = new GossipProtocol('node-2');
    
    gossip1.addPeer('node-2', 'localhost', 5001);
    gossip2.addPeer('node-1', 'localhost', 5000);
    
    gossip1.setMessageHandler((msg, target) => {
      if (target === 'node-2') gossip2.simulateMessage(msg, 'node-1');
    });
    
    gossip2.setMessageHandler((msg, target) => {
      if (target === 'node-1') gossip1.simulateMessage(msg, 'node-2');
    });
  });

  afterEach(() => {
    gossip1.stop();
    gossip2.stop();
  });

  it('should sync state between nodes', async () => {
    gossip1.setState('key1', 'value1');
    
    // Trigger gossip
    await gossip1['gossipRound']();
    
    await sleep(100);
    
    expect(gossip2.getState('key1')).toBe('value1');
  });

  it('should handle concurrent updates with version vectors', async () => {
    gossip1.setState('key1', 'value1');
    await gossip1['gossipRound']();
    await sleep(50);
    
    gossip2.setState('key1', 'value2');
    await gossip2['gossipRound']();
    await sleep(50);
    
    // Higher version should win
    expect(gossip1.getState('key1')).toBe('value2');
  });

  it('should notify specific updates', async () => {
    gossip1.setState('key1', 'value1');
    await gossip1.notify('key1', 'value1');
    
    await sleep(50);
    
    expect(gossip2.getState('key1')).toBe('value1');
  });
});