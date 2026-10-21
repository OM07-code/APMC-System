export interface ServiceInstance {
  id: string;
  name: string;
  host: string;
  port: number;
  metadata: Record<string, string>;
  health: 'healthy' | 'unhealthy' | 'unknown';
  registeredAt: number;
  lastHeartbeat: number;
}

export interface ServiceQuery {
  name?: string;
  metadata?: Record<string, string>;
  health?: 'healthy' | 'unhealthy' | 'unknown';
  limit?: number;
  offset?: number;
}

export interface NamingService {
  register(instance: ServiceInstance): Promise<void>;
  deregister(serviceId: string): Promise<void>;
  heartbeat(serviceId: string): Promise<void>;
  lookup(query: ServiceQuery): Promise<ServiceInstance[]>;
  watch(query: ServiceQuery, callback: (instances: ServiceInstance[]) => void): () => void;
  resolve(name: string): Promise<ServiceInstance | null>;
}

export class InMemoryNamingService implements NamingService {
  private services = new Map<string, ServiceInstance>();
  private watchers = new Map<string, Set<(instances: ServiceInstance[]) => void>>();
  private heartbeatInterval?: NodeJS.Timeout;

  constructor(private readonly ttl: number = 30000) {
    this.startHeartbeatChecker();
  }

  private startHeartbeatChecker(): void {
    this.heartbeatInterval = setInterval(() => {
      const now = Date.now();
      for (const [id, instance] of this.services) {
        if (now - instance.lastHeartbeat > this.ttl) {
          instance.health = 'unhealthy';
          this.notifyWatchers(instance.name);
        }
      }
    }, 5000);

    // unref() so this reaper timer does not by itself hold the Node event loop open.
    // grpc-server.ts constructs an InMemoryNamingService at module scope, so merely
    // importing @apmc/naming (e.g. a CLI/demo using the naming classes) previously
    // hung the process forever on exit. A real server still stays alive via its
    // listening socket, so the timer keeps firing exactly as before.
    this.heartbeatInterval.unref?.();
  }

  async register(instance: ServiceInstance): Promise<void> {
    const key = `${instance.name}:${instance.id}`;
    this.services.set(key, {
      ...instance,
      registeredAt: Date.now(),
      lastHeartbeat: Date.now(),
    });
    this.notifyWatchers(instance.name);
  }

  async deregister(serviceId: string): Promise<void> {
    for (const [key, instance] of this.services) {
      if (instance.id === serviceId) {
        const name = instance.name;
        this.services.delete(key);
        this.notifyWatchers(name);
        break;
      }
    }
  }

  async heartbeat(serviceId: string): Promise<void> {
    for (const instance of this.services.values()) {
      if (instance.id === serviceId) {
        instance.lastHeartbeat = Date.now();
        instance.health = 'healthy';
        this.notifyWatchers(instance.name);
        return;
      }
    }
    throw new (require('@apmc/shared').NotFoundError)('Service', serviceId);
  }

  async lookup(query: ServiceQuery): Promise<ServiceInstance[]> {
    let results = Array.from(this.services.values());

    if (query.name) {
      results = results.filter(s => s.name === query.name);
    }

    if (query.metadata) {
      results = results.filter(s => 
        Object.entries(query.metadata!).every(([k, v]) => s.metadata[k] === v)
      );
    }

    if (query.health) {
      results = results.filter(s => s.health === query.health);
    }

    results.sort((a, b) => b.lastHeartbeat - a.lastHeartbeat);

    if (query.offset) {
      results = results.slice(query.offset);
    }

    if (query.limit) {
      results = results.slice(0, query.limit);
    }

    return results;
  }

  watch(query: ServiceQuery, callback: (instances: ServiceInstance[]) => void): () => void {
    const key = this.getWatcherKey(query);
    if (!this.watchers.has(key)) {
      this.watchers.set(key, new Set());
    }
    this.watchers.get(key)!.add(callback);

    // Initial callback
    this.lookup(query).then(callback);

    return () => {
      this.watchers.get(key)?.delete(callback);
    };
  }

  async resolve(name: string): Promise<ServiceInstance | null> {
    const instances = await this.lookup({ name, health: 'healthy', limit: 1 });
    return instances[0] || null;
  }

  private notifyWatchers(serviceName: string): void {
    for (const [key, callbacks] of this.watchers) {
      if (key.startsWith(serviceName + ':') || key === serviceName) {
        this.lookup(this.parseWatcherKey(key)).then(instances => {
          for (const cb of callbacks) {
            cb(instances);
          }
        });
      }
    }
  }

  private getWatcherKey(query: ServiceQuery): string {
    const parts = [query.name || '*'];
    if (query.metadata) {
      parts.push(JSON.stringify(query.metadata));
    }
    if (query.health) {
      parts.push(query.health);
    }
    return parts.join(':');
  }

  private parseWatcherKey(key: string): ServiceQuery {
    const parts = key.split(':');
    const query: ServiceQuery = {};
    if (parts[0] !== '*') query.name = parts[0];
    if (parts[1] && !['healthy', 'unhealthy', 'unknown'].includes(parts[1])) {
      try {
        query.metadata = JSON.parse(parts[1]);
      } catch {}
    }
    const lastPart = parts[parts.length - 1];
    if (lastPart && ['healthy', 'unhealthy', 'unknown'].includes(lastPart)) {
      query.health = lastPart as ServiceQuery['health'];
    }
    return query;
  }

  shutdown(): void {
    if (this.heartbeatInterval) {
      clearInterval(this.heartbeatInterval);
    }
    this.watchers.clear();
    this.services.clear();
  }
}

export function createServiceInstance(
  name: string,
  host: string,
  port: number,
  metadata: Record<string, string> = {}
): ServiceInstance {
  return {
    id: `${name}-${host}-${port}-${Date.now()}`,
    name,
    host,
    port,
    metadata: {
      ...metadata,
      version: metadata['version'] || '1.0.0',
    },
    health: 'healthy',
    registeredAt: Date.now(),
    lastHeartbeat: Date.now(),
  };
}