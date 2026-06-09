import { Injectable, Logger, Post } from '@nestjs/common'
import type { MappingTypeMapping } from '@elastic/elasticsearch/lib/api/types'
import { ElasticsearchClientService } from '@app/elasticsearch/services/client'
import { DlqEnvelope } from '@app/hold-it/interfaces/kafka-dlq.interface'

/**
 * @description
 * Service responsible for provisioning the Kafka DLQ "necropolis" index family
 *
 * Strategy:
 * - ILM policy: rollover + delete after 90d
 * - Index template: applies mappings/settings + policy to kafka-necropolis-*
 * - Write alias: kafka-necropolis -> points to current write index
 */
@Injectable()
export class HoldItElasticsearchService {
  private readonly logger = new Logger(HoldItElasticsearchService.name)

  constructor(private readonly elasticsearchClientService: ElasticsearchClientService) {}

  /**
   * @description
   * Creates or updates the ILM policy, index template, and initial write index for kafka-necropolis.
   * This endpoint is safe to run multiple times (idempotent), except the index creation step which
   * only runs if the initial index does not exist.
   */
  async provisionKafkaNecropolis() {
    const es = this.elasticsearchClientService.getInstance()

    const ilmPolicyName = 'kafka-necropolis-3m'
    const indexTemplateName = 'kafka-necropolis-template'
    const writeAlias = 'kafka-necropolis'
    const initialIndex = 'kafka-necropolis-000001'

    const mappings: MappingTypeMapping = {
      dynamic: 'strict',
      properties: {
        schemaVersion: { type: 'byte' },

        failureId: { type: 'keyword' },
        fingerprint: { type: 'keyword' },

        service: {
          properties: {
            name: { type: 'keyword' },
            environment: { type: 'keyword' },
            instanceId: { type: 'keyword' },
            groupId: { type: 'keyword' },
          },
        },

        occurredAt: { type: 'date' },
        firstFailureAt: { type: 'date' },
        attempt: { type: 'integer' },
        failureType: { type: 'keyword' },

        correlation: {
          properties: {
            traceId: { type: 'keyword' },
            correlationId: { type: 'keyword' },
            requestId: { type: 'keyword' },
          },
        },

        kafka: {
          properties: {
            original: {
              properties: {
                topic: { type: 'keyword' },
                partition: { type: 'integer' },
                offset: { type: 'keyword' },
                timestamp: { type: 'date' },
                key: { type: 'keyword' },
                headers: { type: 'flattened' },
              },
            },
            dlq: { properties: { topic: { type: 'keyword' } } },
          },
        },

        payload: {
          properties: {
            parsedMessage: { type: 'flattened' },
            raw: {
              properties: {
                valueBase64: { type: 'keyword', index: false, doc_values: false },
                keyBase64: { type: 'keyword', index: false, doc_values: false },
              },
            },
          },
        },

        error: {
          properties: {
            name: { type: 'keyword' },
            code: { type: 'keyword' },
            message: {
              type: 'text',
              fields: { keyword: { type: 'keyword', ignore_above: 1024 } },
            },
            stack: { type: 'text' },
            cause: { type: 'flattened' },
          },
        },
      },
    }

    try {
      await es.ilm.putLifecycle({
        name: ilmPolicyName,
        policy: {
          phases: {
            hot: { actions: { rollover: { max_size: '30gb' } } },
            delete: { min_age: '90d', actions: { delete: {} } },
          },
        },
      })

      this.logger.log(`✅ ILM policy "${ilmPolicyName}" upserted.`)

      await es.indices.putIndexTemplate({
        name: indexTemplateName,
        index_patterns: ['kafka-necropolis-*'],
        priority: 500,
        template: {
          settings: {
            number_of_shards: 12,
            number_of_replicas: 0,
            codec: 'best_compression',
            mapping: { total_fields: { limit: 2000 } },
            'index.lifecycle.name': ilmPolicyName,
            'index.lifecycle.rollover_alias': writeAlias,
          },
          mappings,
        },
      })

      this.logger.log(`✅ Index template "${indexTemplateName}" upserted.`)

      // 3) Create initial index with write alias if it doesn't exist
      const initialExists = await es.indices.exists({ index: initialIndex })
      if (!initialExists) {
        await es.indices.create({
          index: initialIndex,
          aliases: {
            [writeAlias]: { is_write_index: true },
          },
        })
        this.logger.log(`✅ Initial index "${initialIndex}" created with write alias "${writeAlias}".`)
      } else {
        this.logger.log(`ℹ️ Initial index "${initialIndex}" already exists. Skipping creation.`)
      }

      return {
        ok: true,
        ilmPolicyName,
        indexTemplateName,
        writeAlias,
        initialIndex,
      }
    } catch (err: any) {
      this.logger.error(
        { ilmPolicyName, indexTemplateName, writeAlias, initialIndex },
        err?.stack,
        '❌ Failed to provision kafka-necropolis with ILM',
      )
      throw err
    }
  }

  /**
   * @description
   * Upserts a DLQ envelope into the "kafka-necropolis" write alias.
   *
   * Guarantees:
   * - Deterministic _id (idempotent)
   * - Upsert (create if missing, update if exists)
   * - Preserves firstFailureAt once set (optional)
   * - Adds timeout + retry_on_conflict (defensive)
   */
  async sendToNecropolis(
    failureDocument: DlqEnvelope,
    options?: {
      /** When true, requests a refresh (expensive). Default: false */
      refresh?: boolean
      /** Request timeout in ms. Default: 3000 */
      timeoutMs?: number
      /** ES optimistic concurrency retries. Default: 3 */
      retryOnConflict?: number
      /** If true, preserve firstFailureAt once set. Default: true */
      preserveFirstFailureAt?: boolean
    },
  ): Promise<{
    indexed: boolean
    id: string
    result?: string
    index?: string
  }> {
    const es = this.elasticsearchClientService.getInstance()

    const timeoutMs = options?.timeoutMs ?? 3_000
    const refresh = options?.refresh ?? false
    const retryOnConflict = options?.retryOnConflict ?? 3
    const preserveFirstFailureAt = options?.preserveFirstFailureAt ?? true

    const id =
      failureDocument.failureId ||
      `${failureDocument.fingerprint}:${failureDocument.kafka?.original?.topic}:${failureDocument.kafka?.original?.partition}:${failureDocument.kafka?.original?.offset}`

    const routing = failureDocument.fingerprint || failureDocument.failureId

    try {
      // Option A (recommended): script-based update to preserve firstFailureAt
      if (preserveFirstFailureAt) {
        const resp = await es.update({
          index: 'kafka-necropolis',
          id,
          routing,
          refresh: refresh ? 'wait_for' : false,
          timeout: `${timeoutMs}ms`,
          retry_on_conflict: retryOnConflict,
          script: {
            lang: 'painless',
            source: `
            // Always replace with latest doc
            ctx._source = params.doc;

            // Preserve firstFailureAt once set (or initialize from occurredAt)
            if (ctx._source.firstFailureAt == null) {
              if (params.firstFailureAt != null) {
                ctx._source.firstFailureAt = params.firstFailureAt;
              } else if (ctx._source.occurredAt != null) {
                ctx._source.firstFailureAt = ctx._source.occurredAt;
              }
            }
          `,
            params: {
              doc: failureDocument,
              firstFailureAt: (failureDocument as any).firstFailureAt ?? null,
            },
          },
          upsert: failureDocument as unknown as Record<string, unknown>,
        })

        return {
          indexed: true,
          id,
          result: (resp as any).result,
          index: (resp as any)._index,
        }
      }

      // Option B: plain upsert (simpler, overwrites everything including firstFailureAt)
      const resp = await es.update({
        index: 'kafka-necropolis',
        id,
        routing,
        refresh: refresh ? 'wait_for' : false,
        timeout: `${timeoutMs}ms`,
        retry_on_conflict: retryOnConflict,
        doc: failureDocument as unknown as Record<string, unknown>,
        doc_as_upsert: true,
      })

      return {
        indexed: true,
        id,
        result: (resp as any).result,
        index: (resp as any)._index,
      }
    } catch (err: any) {
      const statusCode = err?.meta?.statusCode as number | undefined
      const errorType = err?.meta?.body?.error?.type as string | undefined

      this.logger.error(
        {
          id,
          failureId: failureDocument.failureId,
          fingerprint: failureDocument.fingerprint,
          topic: failureDocument.kafka?.original?.topic,
          partition: failureDocument.kafka?.original?.partition,
          offset: failureDocument.kafka?.original?.offset,
          statusCode,
          errorType,
          esError: err?.meta?.body?.error,
        },
        err?.stack,
        'Failed to upsert DLQ document into kafka-necropolis',
      )

      throw err
    }
  }

  /**
   * @description
   * Removes a DLQ envelope document from the "kafka-necropolis" write alias by deterministic _id.
   *
   * Guarantees:
   * - Idempotent: deleting a missing doc returns deleted=false (does not throw)
   * - Uses the same routing strategy as sendToNecropolis()
   */
  async removeFromNecropolis(
    failureDocument: Pick<DlqEnvelope, 'failureId' | 'fingerprint' | 'kafka'>,
    options?: {
      /** When true, requests a refresh (expensive). Default: false */
      refresh?: boolean
      /** Request timeout in ms. Default: 3000 */
      timeoutMs?: number
    },
  ): Promise<{
    deleted: boolean
    id: string
    result?: string
    index?: string
  }> {
    const es = this.elasticsearchClientService.getInstance()

    const timeoutMs = options?.timeoutMs ?? 3_000
    const refresh = options?.refresh ?? false

    const id = this.computeNecropolisId(failureDocument as DlqEnvelope)
    const routing = failureDocument.fingerprint || failureDocument.failureId

    try {
      const resp = await es.delete({
        index: 'kafka-necropolis',
        id,
        routing,
        refresh: refresh ? 'wait_for' : false,
        timeout: `${timeoutMs}ms`,
      })

      return {
        deleted: true,
        id,
        result: (resp as any).result,
        index: (resp as any)._index,
      }
    } catch (err: any) {
      const statusCode = err?.meta?.statusCode as number | undefined
      const errorType = err?.meta?.body?.error?.type as string | undefined

      // Not found => idempotent delete
      if (statusCode === 404 || errorType === 'document_missing_exception') {
        return { deleted: false, id }
      }

      this.logger.error(
        {
          id,
          failureId: (failureDocument as any).failureId,
          fingerprint: (failureDocument as any).fingerprint,
          topic: (failureDocument as any).kafka?.original?.topic,
          partition: (failureDocument as any).kafka?.original?.partition,
          offset: (failureDocument as any).kafka?.original?.offset,
          statusCode,
          errorType,
          esError: err?.meta?.body?.error,
        },
        err?.stack,
        'Failed to delete DLQ document from kafka-necropolis',
      )

      throw err
    }
  }

  /**
   * @description
   * Computes deterministic _id for kafka-necropolis documents.
   * Must stay consistent across sendToNecropolis/removeFromNecropolis.
   */
  private computeNecropolisId(failureDocument: DlqEnvelope): string {
    return (
      failureDocument.failureId ||
      `${failureDocument.fingerprint}:${failureDocument.kafka?.original?.topic}:${failureDocument.kafka?.original?.partition}:${failureDocument.kafka?.original?.offset}`
    )
  }
}
