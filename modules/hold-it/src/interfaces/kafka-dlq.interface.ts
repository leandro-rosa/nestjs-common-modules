/**
 * @description
 * Normalized failure classification used to drive retry / replay policies.
 */
export type FailureType = 'TRANSIENT' | 'PERMANENT' | 'UNKNOWN'

/**
 * @description
 * Structured error information intended to be safely serialized and stored.
 */
export interface DlqErrorInfo {
  name: string
  message: string
  stack?: string
  code?: string
  cause?: unknown
}

/**
 * @description
 * Kafka metadata describing the original record that failed processing.
 */
export interface DlqKafkaMeta {
  topic: string
  partition: number
  offset: string
  timestamp?: string
  key?: string
  headers?: Record<string, string>
}

/**
 * @description
 * DLQ envelope carrying original payload plus operational metadata that enables
 * triage, deduplication, and controlled reprocessing.
 */
export interface DlqEnvelope<TPayload = unknown> {
  schemaVersion: 1
  failureId: string
  fingerprint: string

  service: {
    name: string
    environment: string
    instanceId?: string
    groupId?: string
  }

  occurredAt: string
  firstFailureAt?: string
  attempt: number
  failureType: FailureType

  correlation?: {
    traceId?: string
    correlationId?: string
    requestId?: string
  }

  kafka: {
    original: DlqKafkaMeta
    dlq: { topic: string }
  }

  payload: {
    parsedMessage: TPayload
    raw?: {
      valueBase64?: string
      keyBase64?: string
    }
  }

  error: DlqErrorInfo
}
