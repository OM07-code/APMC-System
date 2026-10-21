import { Server, ServerCredentials, sendUnaryData, ServerUnaryCall, ServerWritableStream, Metadata, loadPackageDefinition } from '@grpc/grpc-js';
import { loadSync } from '@grpc/proto-loader';
import { join } from 'path';
import { config } from '@apmc/config';
import { logger } from '@apmc/middleware';
import { InMemoryNamingService, createServiceInstance, ServiceInstance } from './service-discovery';
import { AppError, ValidationError, NotFoundError } from '@apmc/shared';

const PROTO_PATH = join(__dirname, '..', '..', '..', 'proto', 'naming.proto');

let packageDefinition: any;
let proto: any;

try {
  packageDefinition = loadSync(PROTO_PATH, {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
  });
  // loadPackageDefinition lives in @grpc/grpc-js (NOT @grpc/proto-loader). The
  // previous require('@grpc/proto-loader') returned undefined here, the call threw,
  // and the swallowing catch left `proto` undefined so ZERO RPCs were registered
  // (every call returned UNIMPLEMENTED). Import it from grpc-js and let real load
  // failures surface instead of hiding them.
  proto = loadPackageDefinition(packageDefinition);
} catch (err) {
  logger.error({ err, protoPath: PROTO_PATH }, 'Failed to load naming.proto — naming RPCs will be unavailable');
}

const namingService = proto?.apmc?.naming?.v1?.NamingService;

const namingServiceImpl = new InMemoryNamingService();

function createNamingServer(): Server {
  const server = new Server();
  
  if (namingService) {
    server.addService(namingService.service, {
      Register: register,
      Deregister: deregister,
      Heartbeat: heartbeat,
      Lookup: lookup,
      Watch: watch,
      Resolve: resolve,
    });
  }
  
  return server;
}

async function register(call: ServerUnaryCall<any, any>, callback: sendUnaryData<any>) {
  try {
    const { name, host, port, metadata } = call.request;
    
    if (!name || !host || !port) {
      throw new ValidationError('name, host, and port are required');
    }

    const instance = createServiceInstance(name, host, port, metadata || {});
    await namingServiceImpl.register(instance);

    callback(null, { success: true, instance_id: instance.id });
  } catch (error) {
    const appError = error instanceof AppError ? error : new AppError('INTERNAL_ERROR', 'Failed to register service');
    logger.error({ err: error }, 'Register failed');
    callback({
      code: appError.statusCode >= 500 ? 13 : 3,
      details: appError.message,
      metadata: new Metadata(),
    }, null);
  }
}

async function deregister(call: ServerUnaryCall<any, any>, callback: sendUnaryData<any>) {
  try {
    const { instance_id } = call.request;
    
    if (!instance_id) {
      throw new ValidationError('instance_id is required');
    }

    await namingServiceImpl.deregister(instance_id);

    callback(null, { success: true });
  } catch (error) {
    const appError = error instanceof AppError ? error : new AppError('INTERNAL_ERROR', 'Failed to deregister service');
    logger.error({ err: error }, 'Deregister failed');
    callback({
      code: 13,
      details: appError.message,
      metadata: new Metadata(),
    }, null);
  }
}

async function heartbeat(call: ServerUnaryCall<any, any>, callback: sendUnaryData<any>) {
  try {
    const { instance_id } = call.request;
    
    if (!instance_id) {
      throw new ValidationError('instance_id is required');
    }

    await namingServiceImpl.heartbeat(instance_id);

    callback(null, { success: true });
  } catch (error) {
    const appError = error instanceof AppError ? error : new AppError('INTERNAL_ERROR', 'Failed to heartbeat');
    logger.error({ err: error }, 'Heartbeat failed');
    callback({
      code: 13,
      details: appError.message,
      metadata: new Metadata(),
    }, null);
  }
}

async function lookup(call: ServerUnaryCall<any, any>, callback: sendUnaryData<any>) {
  try {
    const { name, metadata, health, limit, offset } = call.request;
    
    const instances = await namingServiceImpl.lookup({
      name,
      metadata: metadata || {},
      health,
      limit: limit || 100,
      offset: offset || 0,
    });

    callback(null, {
      instances: instances.map((i: ServiceInstance) => ({
        id: i.id,
        name: i.name,
        host: i.host,
        port: i.port,
        metadata: i.metadata,
        health: i.health,
        registered_at: i.registeredAt.toString(),
        last_heartbeat: i.lastHeartbeat.toString(),
      })),
    });
  } catch (error) {
    const appError = error instanceof AppError ? error : new AppError('INTERNAL_ERROR', 'Failed to lookup');
    logger.error({ err: error }, 'Lookup failed');
    callback({
      code: 13,
      details: appError.message,
      metadata: new Metadata(),
    }, null);
  }
}

async function watch(call: ServerWritableStream<any, any>) {
  const { name, metadata, health } = call.request;
  
  const unwatch = namingServiceImpl.watch(
    { name, metadata: metadata || {}, health },
    (instances: ServiceInstance[]) => {
      call.write({
        instances: instances.map((i: ServiceInstance) => ({
          id: i.id,
          name: i.name,
          host: i.host,
          port: i.port,
          metadata: i.metadata,
          health: i.health,
          registered_at: i.registeredAt.toString(),
          last_heartbeat: i.lastHeartbeat.toString(),
        })),
      });
    }
  );
  
  call.on('cancelled', () => {
    unwatch();
  });
}

async function resolve(call: ServerUnaryCall<any, any>, callback: sendUnaryData<any>) {
  try {
    const { name } = call.request;
    
    if (!name) {
      throw new ValidationError('name is required');
    }

    const instance = await namingServiceImpl.resolve(name);
    
    if (!instance) {
      callback({
        code: 5,
        details: 'Service not found',
        metadata: new Metadata(),
      }, null);
      return;
    }

    callback(null, {
      instance: {
        id: instance.id,
        name: instance.name,
        host: instance.host,
        port: instance.port,
        metadata: instance.metadata,
        health: instance.health,
        registered_at: instance.registeredAt.toString(),
        last_heartbeat: instance.lastHeartbeat.toString(),
      },
    });
  } catch (error) {
    const appError = error instanceof AppError ? error : new AppError('INTERNAL_ERROR', 'Failed to resolve');
    logger.error({ err: error }, 'Resolve failed');
    callback({
      code: 13,
      details: appError.message,
      metadata: new Metadata(),
    }, null);
  }
}

export async function startNamingServer(): Promise<Server> {
  const server = createNamingServer();
  
  return new Promise((resolve, reject) => {
    server.bindAsync(
      `0.0.0.0:${config.NAMING_GRPC_PORT}`,
      ServerCredentials.createInsecure(),
      (err, port) => {
        if (err) {
          reject(err);
          return;
        }
        server.start();
        logger.info(`Naming gRPC server started on port ${port}`);
        resolve(server);
      }
    );
  });
}