export interface QueueJobCounts {
  queueName: string
  counts: {
    waiting: number
    active: number
    delayed: number
    failed: number
    completed: number
    paused: number
  }
}

export interface ReprocessFailedJobsOptions {
  /**
   * Maximum number of failed jobs to retry per queue.
   * Use to cap blast radius and avoid CPU/RAM spikes.
   */
  maxPerQueue?: number

  /**
   * Batch size used to fetch failed jobs.
   * Smaller batches are safer under tight memory/CPU constraints.
   */
  batchSize?: number

  /**
   * Max concurrent retries per queue.
   * Keep this small (1-5) for heavy jobs.
   */
  retryConcurrency?: number

  /**
   * Global lock key prefix to ensure only one instance runs retries at a time.
   */
  lockKeyPrefix?: string

  /**
   * Lock TTL in milliseconds.
   */
  lockTtlMs?: number

  /**
   * If true, only counts what would be retried (no mutation).
   */
  dryRun?: boolean

  /**
   * If true, skip queues that currently have any active jobs.
   * This prevents contention and reduces risk of stalled/OOM.
   */
  skipIfActive?: boolean
}

export interface ReprocessFailedJobsResult {
  queueName: string
  scanned: number
  retried: number
  skipped: number
  errors: number
  errorSamples: Array<{ jobId: string; message: string }>
}
