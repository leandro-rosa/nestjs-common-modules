import { Job, Queue } from 'bullmq'
import { Injectable, Inject, ConflictException } from '@nestjs/common'
import { ModuleRef } from '@nestjs/core'
import { HoldItALotOfJobDTO, HoldItJobDTO, HoldItSimpleJobDataDTO } from '../../../dto/hold-it-message'
import { HoldItBrokerInterface } from '../../../interfaces/broker.interface'

/**
 * Implementation of HoldItBroker interface using BullMQ as the underlying message broker.
 *
 * BullMQ is a powerful and reliable message queueing library for Node.js, built on top of Redis.
 * It provides high performance, scalability, and built-in support for advanced features like job scheduling,
 * retries, and priorities.
 *
 * @see {@link https://docs.bullmq.io/} BullMQ Documentation
 */
@Injectable()
export class HoldItBullMQBroker implements HoldItBrokerInterface {
  private queueMap = new Map<string, Queue>()

  /**
   * Initializes a new queue handler with specified configurations.
   *
   * @param moduleRef The NestJS module reference.
   */
  constructor(@Inject(ModuleRef) private moduleRef: ModuleRef) {}

  /**
   * Sends a message using the configured message broker.
   * Reliably sends your payload with a promise so sure, it's practically guaranteed.
   *
   * @param topicName
   * @param message
   * @param options
   * @returns Returns a promise that will return the job instance associated with the jobId parameter.
   * If the specified job cannot be located
   * the promise callback parameter will be set to null.
   */
  async holdIt<T = HoldItSimpleJobDataDTO>({ queueName, message, options }: HoldItJobDTO<T>): Promise<Job<T>> {
    if (!queueName) {
      throw new Error('queueName is required')
    }

    const queue = await this.getQueue<T>(queueName)

    const job = await queue.add(queueName as any, message as unknown as any, options)

    return job as Job<T>
  }

  /**
   * Sends a lot pf message using the configured message broker.
   * Reliably sends your payloads with a list of promise so sure, they're practically guaranteed.
   *
   * @param queueName
   * @param options
   * @param messages
   */
  async holdItALot<T = HoldItSimpleJobDataDTO>({ queueName, messages }: HoldItALotOfJobDTO<T>): Promise<Job<T>[]> {
    if (!queueName) {
      throw new Error('queueName is required')
    }

    const queue = await this.getQueue<T>(queueName)

    const jobs = messages.map(data => ({
      name: queueName,
      data,
      opts: data.jobOptions,
    }))

    return queue.addBulk(jobs as any[]) as Promise<Job<T>[]>
  }

  /**
   * Gracefully shuts down the message handler.
   * Like making a clean exit from a party—no fuss, no mess.
   *
   * @returns Just a promise that resolves once the handler has shut down cleanly.
   */
  async shutdown(): Promise<void> {
    for (const queue of this.queueMap.values()) {
      queue.removeAllListeners()
      await queue.close()
    }
  }

  async getQueue<T = any>(queueName?: string): Promise<Queue<T>> {
    if (!queueName) {
      throw new Error(`Queue is required`)
    }

    if (!this.queueMap.has(queueName)) {
      const queue = this.moduleRef.get<Queue<T>>(`BullQueue_${queueName}`, {
        strict: false,
      })

      if (!queue) {
        throw new Error(`Queue not found for name: ${queueName}`)
      }

      this.queueMap.set(queueName, queue)
    }

    const resolvedQueue = this.queueMap.get(queueName)
    if (!resolvedQueue) {
      throw new Error(`Queue was expected to be set but is missing for: ${queueName}`)
    }

    return resolvedQueue as Queue<T>
  }

  /**
   * Health check for BullMQ queues.
   * Verifies that the queues are operational, connected to Redis,
   * and checks for stalled jobs.
   *
   * @returns {Promise<{status: boolean, details?: object}>} Returns an object indicating whether the queues are healthy.
   */
  async isHealthy(): Promise<{ status: boolean; details?: object }> {
    const queues = this.queueMap.values()
    const details: Record<string, string> = {}

    for (const queue of queues) {
      try {
        const queueClient = (await queue.client) as unknown as { ping(): Promise<unknown> }
        const pong = await queueClient.ping()

        console.log({ pong })
      } catch (err) {
        details[queue.name] =
          `Queue ${queue.name} is not connected to Redis. Error: ${err instanceof Error ? err.message : 'Unknown error'}`
        return { status: false, details }
      }
    }

    return { status: true }
  }

  async checkQueueStatus(queueName: string): Promise<void> {
    const queue = await this.getQueue(queueName)

    const [waitingCount, activeCount, delayedCount] = await Promise.all([
      queue.getWaitingCount(),
      queue.getActiveCount(),
      queue.getDelayedCount(),
    ])

    if (waitingCount > 0 || activeCount > 0 || delayedCount > 0) {
      throw new ConflictException(
        `A integração já está em andamento. Jobs em andamento: 
        Em espera: ${waitingCount}, 
        Processando: ${activeCount}, 
        Atrasados: ${delayedCount}`,
      )
    }
  }
}
