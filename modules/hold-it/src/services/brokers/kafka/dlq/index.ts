import { Injectable, Logger } from '@nestjs/common'
import crypto from 'node:crypto'
import type { IHeaders, KafkaMessage } from 'kafkajs'
import { DlqEnvelope, DlqErrorInfo, FailureType } from '@app/hold-it/interfaces/kafka-dlq.interface'

@Injectable()
export class KafkaDlqService {
  private readonly logger = new Logger(KafkaDlqService.name)

  /**
   * @description
   * Safely converts KafkaJS headers to a string map, dropping non-serializable values.
   */
  normalizeHeaders(headers?: IHeaders): Record<string, string> | undefined {
    if (!headers) return undefined
    const out: Record<string, string> = {}
    for (const [k, v] of Object.entries(headers)) {
      if (v == null) continue
      if (typeof v === 'string') out[k] = v
      else if (Buffer.isBuffer(v)) out[k] = v.toString('utf8')
      else out[k] = String(v)
    }
    return Object.keys(out).length ? out : undefined
  }

  /**
   * @description
   * Truncates strings defensively to avoid oversized DLQ messages and log payloads.
   */
  truncate(value: string | undefined, max: number): string | undefined {
    if (!value) return value
    return value.length > max ? `${value.slice(0, max)}…(truncated)` : value
  }

  /**
   * @description
   * Serializes an error into a safe, JSON-friendly structure.
   */
  toDlqErrorInfo(err: unknown): DlqErrorInfo {
    const e = err as any
    return {
      name: this.truncate(e?.name ?? 'Error', 200) ?? 'Error',
      message: this.truncate(e?.message ?? String(err), 1_000) ?? 'Unknown error',
      stack: this.truncate(e?.stack, 16_000),
      code: this.truncate(e?.code ?? e?.errno ?? e?.statusCode, 200),
      cause: e?.cause,
    }
  }

  /**
   * @description
   * Extracts correlation identifiers from headers or payload (best-effort).
   */
  extractCorrelation(parsedMessage: any, headers?: Record<string, string>) {
    const traceId =
      headers?.['traceid'] || headers?.['trace-id'] || headers?.['x-b3-traceid'] || headers?.['x-trace-id']

    const correlationId = headers?.['correlationid'] || headers?.['correlation-id'] || headers?.['x-correlation-id']

    const requestId = headers?.['x-request-id'] || headers?.['request-id']

    // Optional fallback: parsedMessage fields commonly used in events
    return {
      traceId: traceId ?? parsedMessage?.traceId ?? parsedMessage?.trace_id,
      correlationId: correlationId ?? parsedMessage?.correlationId ?? parsedMessage?.correlation_id,
      requestId: requestId ?? parsedMessage?.requestId ?? parsedMessage?.request_id,
    }
  }

  /**
   * @description
   * Classifies failure type (transient vs permanent) using pragmatic heuristics.
   * Extend this mapping based on your domain / dependencies.
   */
  classifyFailure(errInfo: DlqErrorInfo): FailureType {
    const msg = (errInfo.message || '').toLowerCase()
    const code = (errInfo.code || '').toLowerCase()
    const name = (errInfo.name || '').toLowerCase()

    // Transient signals: timeouts, temporary network, 5xx, broker issues, etc.
    if (
      msg.includes('timeout') ||
      msg.includes('econnreset') ||
      msg.includes('etimedout') ||
      msg.includes('temporar') ||
      msg.includes('too many requests') ||
      code.includes('429') ||
      code.includes('503') ||
      code.includes('502') ||
      code.includes('504') ||
      name.includes('timeout')
    ) {
      return 'TRANSIENT'
    }

    // Permanent signals: validation, schema, bad request, etc.
    if (msg.includes('validation') || msg.includes('invalid') || code.includes('400') || code.includes('422')) {
      return 'PERMANENT'
    }

    return 'UNKNOWN'
  }

  /**
   * @description
   * Computes a stable fingerprint for grouping failures (dedupe/triage).
   */
  computeFingerprint(input: { topic: string; key?: string; parsedMessage: unknown; err: DlqErrorInfo }): string {
    // Keep it stable: use topic + key + error name + a small stable projection of payload.
    const payloadProjection =
      typeof input.parsedMessage === 'object'
        ? JSON.stringify(input.parsedMessage, Object.keys(input.parsedMessage as any).sort()).slice(0, 4_000)
        : String(input.parsedMessage).slice(0, 4_000)

    const base = [
      `topic=${input.topic}`,
      `key=${input.key ?? ''}`,
      `err=${input.err.name}:${input.err.message}`,
      `payload=${payloadProjection}`,
    ].join('|')

    return crypto.createHash('sha256').update(base).digest('hex')
  }

  /**
   * @description
   * Creates a rich DLQ envelope for a failed message processing attempt.
   */
  buildDlqEnvelope<TPayload>(params: {
    parsedMessage: TPayload
    err: unknown
    topic: string
    partition: number
    message: KafkaMessage
    dlqTopic: string
    serviceName: string
    environment: string
    groupId?: string
    instanceId?: string
    attempt?: number
    firstFailureAt?: string
  }): DlqEnvelope<TPayload> {
    const headers = this.normalizeHeaders(params.message.headers)
    const errInfo = this.toDlqErrorInfo(params.err)

    const keyStr = params.message.key ? params.message.key.toString('utf8') : undefined

    const occurredAt = new Date().toISOString()
    const correlation = this.extractCorrelation(params.parsedMessage as any, headers)

    const fingerprint = this.computeFingerprint({
      topic: params.topic,
      key: keyStr,
      parsedMessage: params.parsedMessage,
      err: errInfo,
    })

    return {
      schemaVersion: 1,
      failureId: crypto.randomUUID(),
      fingerprint,

      service: {
        name: params.serviceName,
        environment: params.environment,
        instanceId: params.instanceId,
        groupId: params.groupId,
      },

      occurredAt,
      firstFailureAt: params.firstFailureAt,
      attempt: params.attempt ?? 1,
      failureType: this.classifyFailure(errInfo),

      correlation: {
        traceId: correlation.traceId,
        correlationId: correlation.correlationId,
        requestId: correlation.requestId,
      },

      kafka: {
        original: {
          topic: params.topic,
          partition: params.partition,
          offset: params.message.offset,
          timestamp: params.message.timestamp,
          key: keyStr,
          headers,
        },
        dlq: { topic: params.dlqTopic },
      },

      payload: {
        parsedMessage: params.parsedMessage,
        // Optional raw backup (only if you store message.value somewhere; KafkaJS message.value is not available here in your snippet)
        // raw: { valueBase64: params.message.value?.toString('base64'), keyBase64: params.message.key?.toString('base64') },
      },

      error: errInfo,
    }
  }
}
