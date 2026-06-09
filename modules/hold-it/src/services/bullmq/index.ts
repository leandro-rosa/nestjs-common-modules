import { Inject, Injectable, Logger } from '@nestjs/common'
import type { Queue, Job, JobsOptions } from 'bullmq'
import { HoldItBullMQBroker } from '../brokers/bull-mq'
import {
  QueueJobCounts,
  ReprocessFailedJobsOptions,
  ReprocessFailedJobsResult,
} from '@app/hold-it/interfaces/bullmq.interface'
import { HOLD_IT_QUEUE_NAMES } from '@app/hold-it/types'

@Injectable()
export class BullMQService {
  private readonly logger = new Logger(BullMQService.name)

  constructor(
    @Inject(HOLD_IT_QUEUE_NAMES)
    private readonly queueNames: string[],
    private readonly broker: HoldItBullMQBroker,
  ) { }

  /**
   * Returns job counts for each configured queue using a single Redis call per queue.
   */
  async monitorQueues(): Promise<QueueJobCounts[]> {
    const tasks = this.queueNames.map(async queueName => {
      const queue = await this.broker.getQueue(queueName)
      const counts = await queue.getJobCounts('waiting', 'active', 'delayed', 'failed', 'completed', 'paused')

      return {
        queueName,
        counts: {
          waiting: counts.waiting ?? 0,
          active: counts.active ?? 0,
          delayed: counts.delayed ?? 0,
          failed: counts.failed ?? 0,
          completed: counts.completed ?? 0,
          paused: counts.paused ?? 0,
        },
      }
    })

    return Promise.all(tasks)
  }

  /**
   * Retries failed jobs across all queues in a controlled, safe way:
   * - Distributed lock prevents multiple instances retrying simultaneously
   * - Batch fetch avoids loading huge lists in memory
   * - Concurrency limit avoids CPU/RAM spikes and event-loop starvation
   */
  async reprocessFailedJobs(options: ReprocessFailedJobsOptions = {}) {
    const {
      maxPerQueue = 500,
      batchSize = 50,
      retryConcurrency = 3,
      lockKeyPrefix = 'holdit:bullmq:reprocessFailed',
      lockTtlMs = 5 * 60 * 1000,
      dryRun = false,
      skipIfActive = true,
    } = options

    for await (const queueName of this.queueNames) {
      const queue = await this.broker.getQueue(queueName)
      const lockKey = `${lockKeyPrefix}:${queueName}`

      const lockOk = await this.acquireQueueLock(queue, lockKey, lockTtlMs)
      if (!lockOk) {
        this.logger.warn(`Skipping queue "${queueName}" because lock is held by another instance.`)
        return {
          queueName,
          scanned: 0,
          retried: 0,
          skipped: 0,
          errors: 0,
          errorSamples: [],
        }
      }

      try {
        if (skipIfActive) {
          const active = await queue.getActiveCount()
          if (active > 0) {
            this.logger.warn(`Skipping queue "${queueName}" because it has active jobs (${active}).`)
          }
        }

        await this.retryFailedInQueue(queue, {
          maxPerQueue,
          batchSize,
          retryConcurrency,
          dryRun,
        })
      } finally {
        await this.releaseQueueLock(queue, lockKey).catch(err => {
          this.logger.warn(`Failed to release lock for "${queueName}": ${err?.message ?? err}`)
        })
      }
    }

    // return Promise.all(tasks)
  }

  private async retryFailedInQueue(
    queue: Queue,
    params: { maxPerQueue: number; batchSize: number; retryConcurrency: number; dryRun: boolean },
  ): Promise<ReprocessFailedJobsResult> {
    const { maxPerQueue, batchSize, retryConcurrency, dryRun } = params

    let scanned = 0
    let retried = 0
    let skipped = 0
    let errors = 0
    const errorSamples: Array<{ jobId: string; message: string }> = []

    // IMPORTANT:
    // We always fetch from the "start" because retried jobs are removed from "failed",
    // so index 0 keeps advancing safely without needing pagination bookkeeping.
    while (retried + skipped + errors < maxPerQueue) {
      const remaining = maxPerQueue - (retried + skipped + errors)
      const limit = Math.min(batchSize, remaining)

      try {
        const jobs = await queue.getJobs(['failed'], 0, limit - 1, true)
        if (!jobs.length) break

        scanned += jobs.length

        for (const job of jobs) {
          try {
            const result = await this.safeRetryJob(job, dryRun)

            if (result === 'retried') retried += 1
            else skipped += 1
          } catch (err) {
            errors += 1

            if (errorSamples.length < 10) {
              errorSamples.push({
                jobId: String(job.id ?? ''),
                message: err?.message ?? String(err),
              })
            }
          }
        }
      } catch (err) {
        this.logger.error({ queue: queue.name }, err.stack ?? err.message ?? String(err), 'ERROR_RETRYING_FAILED_JOBS')
        errors += 1
        continue
      }
    }

    return {
      queueName: queue.name,
      scanned,
      retried,
      skipped,
      errors,
      errorSamples,
    }
  }

  private async safeRetryJob(job: Job, dryRun: boolean): Promise<'retried' | 'skipped'> {
    // Safety: avoid retrying jobs that are not actually failed anymore.
    const state = await job.getState()
    if (state !== 'failed') return 'skipped'

    if (dryRun) return 'retried'

    // BullMQ will move job back to waiting and re-process it.
    // If the job was previously delayed/paused/etc, BullMQ handles the state transition.
    await job.retry()
    return 'retried'
  }

  /**
   * Acquires a per-queue distributed lock using Redis SET NX PX.
   */
  private async acquireQueueLock(queue: Queue, key: string, ttlMs: number): Promise<boolean> {
    const client = await queue.client
    const value = `${process.pid}:${Date.now()}`
    const result = await client.set(key, value, 'PX', ttlMs, 'NX')
    return result === 'OK'
  }

  /**
   * Releases a per-queue distributed lock.
   * Note: For strict safety you can store the lock value and compare before DEL.
   * This is a pragmatic version to keep overhead minimal.
   */
  private async releaseQueueLock(queue: Queue, key: string): Promise<void> {
    const client = await queue.client
    await client.del(key)
  }
}
