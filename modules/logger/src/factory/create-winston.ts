import winston, { format, transport } from 'winston'
import LokiTransport from 'winston-loki'
import { utilities as nestWinstonModuleUtilities } from 'nest-winston'

import { NodeSDK } from '@opentelemetry/sdk-node'
import { getNodeAutoInstrumentations } from '@opentelemetry/auto-instrumentations-node'
import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-http'
import { resourceFromAttributes } from '@opentelemetry/resources'
import { context, trace } from '@opentelemetry/api'

import type { TransformableInfo } from 'logform'
import type { Format } from 'logform'

let sdk: NodeSDK | null = null
export const otelCorrelationFormat = (): Format =>
  format((info: TransformableInfo) => {
    const span = trace.getSpan(context.active())
    if (!span) return info

    const spanContext = span.spanContext()
      ; (info as any).trace_id = spanContext.traceId
      ; (info as any).span_id = spanContext.spanId

    return info
  })()

export const startTracing = (): void => {
  if (sdk) return
  const url =
    process.env.OTEL_EXPORTER_OTLP_TRACES_ENDPOINT ??
    process.env.OTEL_COLLECTOR_URL ??
    'http://otel-collector:4318/v1/traces'

  sdk = new NodeSDK({
    resource: resourceFromAttributes({
      'service.name': process.env.APP_NAME || 'Unknown Service',
      'deployment.environment': process.env.MDM_ENVIRONMENT || 'unknown',
    }),
    traceExporter: new OTLPTraceExporter({
      url,
      keepAlive: true,
      concurrencyLimit: 2,
      timeoutMillis: 60_000,
      httpAgentOptions: { maxSockets: 120 },
    }),
    instrumentations: [getNodeAutoInstrumentations()],
    autoDetectResources: true,
  })

  sdk.start()

  const shutdown = () =>
    sdk
      ?.shutdown()
      .catch(err => console.error('Error terminating tracing', err))
      .finally(() => process.exit(0))

  process.on('SIGTERM', shutdown)
  process.on('SIGINT', shutdown)
}

export type CreateLoggerOptions = {
  /**
   * Additional formatter to enrich log entries (e.g., OTEL trace/span correlation).
   * Keep this optional to avoid coupling the logger lib with tracer libs.
   */
  enrichFormat?: winston.Logform.Format
}

export const createLogger = (serviceName: string, options?: CreateLoggerOptions) => {
  const baseFormat = winston.format.combine(
    otelCorrelationFormat(),
    winston.format.timestamp(),
    winston.format.errors({ stack: true }),
  )

  const consoleFormat = winston.format.combine(
    baseFormat,
    winston.format.ms(),
    nestWinstonModuleUtilities.format.nestLike('AuthoParts', {
      colors: true,
      prettyPrint: true,
      processId: true,
      appName: true,
    }),
  )

  const lokiFormat = winston.format.combine(baseFormat, winston.format.json())

  const transports: transport[] = [
    new winston.transports.Console({
      level: 'info',
      format: consoleFormat,
    }),
  ]

  if (process.env.NODE_ENV !== 'test') {
    transports.push(
      new LokiTransport({
        host: process.env.LOKI_URL ?? 'http://mdm-loki.comolatti-mdm.local:3100',
        level: 'info',
        batching: true,
        json: true,
        format: lokiFormat,
        labels: {
          service_name: process.env.APP_NAME ?? 'AuthoParts',
          environment: process.env.MDM_ENVIRONMENT ?? 'unknown',
        },
        interval: 12,
        onConnectionError: (err: unknown) => {
          const message = err instanceof Error ? err.message : String(err)
          console.error('Erro ao conectar ao Loki:', message)
        },
      }),
    )
  }
  return winston.createLogger({
    level: 'info',
    transports,
  })
}
