import { NodeSDK } from '@opentelemetry/sdk-node';
import { getNodeAutoInstrumentations } from '@opentelemetry/auto-instrumentations-node';
import { PrometheusExporter } from '@opentelemetry/exporter-prometheus';
import { JaegerExporter } from '@opentelemetry/exporter-jaeger';
import { Resource } from '@opentelemetry/resources';
import { SemanticResourceAttributes } from '@opentelemetry/semantic-conventions';
import { config } from '@apmc/config';
import { logger } from './logger';

let sdk: NodeSDK | null = null;

export async function initTracing(): Promise<void> {
  let prometheusExporter;
  
  // Prometheus exporter for metrics
  try {
    prometheusExporter = new PrometheusExporter({
      port: config.PROMETHEUS_PORT,
      endpoint: '/metrics',
    }, () => {
      logger.info(`Prometheus metrics server started on port ${config.PROMETHEUS_PORT}`);
    });
  } catch (err: any) {
    if (err.code === 'EADDRINUSE') {
      logger.warn(`Prometheus port ${config.PROMETHEUS_PORT} in use, skipping metrics exporter`);
    } else {
      logger.error({ err }, 'Failed to start Prometheus exporter');
    }
  }
  
  // Jaeger exporter for traces
  let jaegerExporter;
  if (config.JAEGER_ENDPOINT) {
    jaegerExporter = new JaegerExporter({
      endpoint: config.JAEGER_ENDPOINT,
    });
  }
  
  sdk = new NodeSDK({
    resource: new Resource({
      [SemanticResourceAttributes.SERVICE_NAME]: config.SERVICE_NAME,
      [SemanticResourceAttributes.SERVICE_VERSION]: config.SERVICE_VERSION,
      [SemanticResourceAttributes.DEPLOYMENT_ENVIRONMENT]: config.NODE_ENV,
    }),
    instrumentations: [getNodeAutoInstrumentations()],
    metricReader: prometheusExporter,
    traceExporter: jaegerExporter,
  });
  
  try {
    sdk.start();
    logger.info('OpenTelemetry initialized');
  } catch (err) {
    logger.error({ err }, 'Failed to start OpenTelemetry SDK');
  }
  
  process.on('SIGTERM', () => {
    shutdownTracing();
  });
}

export async function shutdownTracing(): Promise<void> {
  if (sdk) {
    await sdk.shutdown();
    logger.info('OpenTelemetry shut down');
  }
}

export function getTracer(name: string) {
  return require('@opentelemetry/api').trace.getTracer(name);
}