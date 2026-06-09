import { Injectable, OnModuleInit, OnModuleDestroy, Logger, Type } from '@nestjs/common'

import * as avro from 'avsc'

import { ConfigService } from '@nestjs/config'
import { DiscoveryService, Reflector } from '@nestjs/core'
import {
  CompressionTypes,
  Consumer,
  Producer,
  Kafka,
  KafkaMessage,
  logLevel,
  Message,
  Partitioners,
  RecordMetadata,
  RetryOptions,
  Admin,
} from 'kafkajs'
import { HoldItALotOfJobDTO, HoldItJobDTO, HoldItSimpleJobDataDTO, KafkaJobOptions } from '../../../dto/hold-it-message'
import { KAFKA_CONSUMER_METADATA } from '../../../decorators/kafka-consumer'
import { HoldItBrokerInterface } from '../../../interfaces/broker.interface'
import { COMPATIBILITY, SchemaRegistry } from '@kafkajs/confluent-schema-registry'
import { AvroConfluentSchema, RawAvroSchema } from '@kafkajs/confluent-schema-registry/dist/@types'
import { randomUUID } from 'crypto'
import { KafkaDlqService } from './dlq'
import { DlqEnvelope } from '@app/hold-it/interfaces/kafka-dlq.interface'
import { setTimeout } from 'timers/promises'
import { HoldItElasticsearchService } from '../../elasticsearch'

/**
 * Implementation of HoldItBroker interface using Kafka as the underlying message broker.
 *
 * This implementation uses KafkaJS to produce messages to Kafka topics, where each topic
 * represents a queue. It uses NestJS lifecycle hooks to initialize and gracefully shut down
 * Kafka producers.
 *
 * Note:
 * - Kafka does not provide native support for job status or delayed messages like BullMQ.
 * - If you require features such as retries, delays, or dead-letter queues, you'll need to
 *   implement additional layers (e.g., separate retry topics or external state management).
 */
@Injectable()
export class HoldItKafkaBroker implements HoldItBrokerInterface, OnModuleInit, OnModuleDestroy {
  private sufixGroupId!: string
  private kafka!: Kafka
  private producer!: Producer
  private admin!: Admin
  private schemaRegistry!: SchemaRegistry

  private schemaIds: Map<string, number> = new Map()
  private readonly avroTypeCache = new Map<number, avro.Type>()
  private readonly logger = new Logger(HoldItKafkaBroker.name)
  private readonly consumers: Map<
    string,
    {
      descriptor: PropertyDescriptor
      instance: any
      metadadata: any
      concurrency?: number
      topic: string
      schemaRegistry?: RawAvroSchema
      kafkaConsumer?: Consumer[]
      isDlq?: boolean
    }
  > = new Map()

  private readonly retryPolicy: RetryOptions = {
    initialRetryTime: 300,
    multiplier: 2,
    factor: 0.2,
    retries: 10,
    maxRetryTime: 120_000,

    restartOnFailure: async (error: Error) => {
      const message = error.message.toUpperCase()

      this.logger.error(
        {
          name: error.name,
          message: error.message,
        },
        error.stack,
        'ERROR_KAFKA_RETRY_POLICY',
      )

      const isAuthorizationError =
        message.includes('AUTHORIZATION_FAILED') ||
        message.includes('NOT_AUTHORIZED') ||
        message.includes('SASL_AUTHENTICATION_FAILED')

      return !isAuthorizationError
    },
  }

  constructor(
    private readonly configService: ConfigService,
    private readonly discoveryService: DiscoveryService,
    private readonly reflector: Reflector,
    private readonly kafkaDlqService: KafkaDlqService,
    private readonly holdItElasticsearchService: HoldItElasticsearchService,
  ) {}

  /**
   * @description
   * OnModuleInit is called once the module has been initialized.
   * Use this hook to perform any necessary initialization.
   */
  async onModuleInit() {
    if (!this.configService.get<string>('KAFKA_BROKERS')) {
      this.logger.warn(
        { message: 'KAFKA_BROKERS is not defined. Kafka Broker will not start.' },
        'KAFKA_BROKERS_NOT_DEFINED',
      )
      return
    }

    await this.startKafkaProducers()

    this.startKafkaConsumers()
      .then(() => {
        this.logger.log({ message: 'Kafka Consumers has been connect successfully.' }, 'KAFKA_CONSUMERS_CONNECTED')
      })
      .catch(error => {
        this.logger.error(
          { message: 'Error connecting Kafka Consumers' },
          error.stack,
          'ERROR_CONNECTING_KAFKA_CONSUMERS',
        )
        throw error
      })
  }

  /**
   * @description
   * Starts Kafka producers and admin client.
   */
  private async startKafkaProducers() {
    const brokers = this.configService.get<string>(
      'KAFKA_BROKERS',
      'kafka-broker:29092,kafka-broker-2:29092,kafka-broker-3:29092',
    )

    this.sufixGroupId = `-${this.configService.get<string>('KAFKA_GROUP_ID_SUFIX', randomUUID({ disableEntropyCache: true }))}`
    // this.logger.debug({ brokers, sufixGroupId: this.sufixGroupId }, 'Starting Kafka Producers')

    switch (this.configService.get('KAFKA_SSL_ENABLED', false)) {
      case '1':
      case 1:
      case true:
        this.kafka = new Kafka({
          logLevel: logLevel.NOTHING,
          authenticationTimeout: 4200,
          connectionTimeout: 4200,
          enforceRequestTimeout: false,
          requestTimeout: 4200,
          brokers: brokers.split(',').map(broker => broker.trim()),
          retry: this.retryPolicy,
          clientId: 'digitalorchestrator',
          ssl: this.configService.get<boolean>('KAFKA_SSL_ENABLED', false),
          sasl: {
            mechanism: 'plain', // scram-sha-256 or scram-sha-512
            username: this.configService.get<string>('KAFKA_SASL_USERNAME', ''),
            password: this.configService.get<string>('KAFKA_SASL_PASSWORD', ''),
          },
          logCreator: () => {
            return ({ level, log, namespace }) => {
              const message = `[${namespace}] ${log.message}`
              const extra = { ...log, namespace }
              switch (level) {
                case logLevel.ERROR:
                  this.logger.error({ message, extra })
                  break
                case logLevel.WARN:
                  this.logger.warn({ message, extra })
                  break
                case logLevel.INFO:
                  this.logger.log({ message, extra })
                  break
                case logLevel.DEBUG:
                  this.logger.log({ message, extra })
                  break
                default:
                  this.logger.verbose({ message, extra })
              }
            }
          },
        })
        break
      default:
        this.kafka = new Kafka({
          logLevel: logLevel.NOTHING,
          authenticationTimeout: 4200,
          connectionTimeout: 4200,
          enforceRequestTimeout: false,
          requestTimeout: 4200,
          brokers: brokers.split(',').map(broker => broker.trim()),
          retry: this.retryPolicy,
          clientId: 'digitalorchestrator',
          logCreator: () => {
            return ({ level, log, namespace }) => {
              const message = `[${namespace}] ${log.message}`
              const extra = { ...log, namespace }
              switch (level) {
                case logLevel.ERROR:
                  this.logger.error(message, extra)
                  break
                case logLevel.WARN:
                  this.logger.warn(message, extra)
                  break
                case logLevel.INFO:
                  this.logger.log(message, extra)
                  break
                case logLevel.DEBUG:
                  // this.logger.debug(message, extra)
                  break
                default:
                  this.logger.verbose(message, extra)
              }
            }
          },
        })
    }

    this.producer = this.kafka.producer({
      allowAutoTopicCreation: true,
      createPartitioner: Partitioners.DefaultPartitioner,
      idempotent: false,
      transactionTimeout: 4200,
      retry: this.retryPolicy,
    })

    this.admin = this.kafka.admin({ retry: this.retryPolicy })
    await this.producer.connect()
    await this.admin.connect()

    if (!this.schemaRegistry) {
      this.schemaRegistry = new SchemaRegistry({
        host: this.configService.get<string>('SCHEMA_REGISTRY_HOST', 'http://digital-schema-registry:8081'),
        retry: this.retryPolicy,
      })
    }

    //// this.logger.debug({ brokers, sufixGroupId: this.sufixGroupId }, 'Kafka Producers started successfully')
  }

  /**
   * Starts Kafka consumers.
   */
  private async startKafkaConsumers() {
    const providers = this.discoveryService.getProviders()
    for (const provider of providers) {
      if (!provider.instance) continue
      const instance = provider.instance
      const prototype = Object.getPrototypeOf(instance)
      for (const methodKey of Object.getOwnPropertyNames(prototype)) {
        if (methodKey === 'constructor') continue
        const descriptor = Object.getOwnPropertyDescriptor(prototype, methodKey)

        if (!descriptor || typeof descriptor.value !== 'function') continue
        const metadadata = this.reflector.get<{ topic: string; concurrency?: number; schemaRegistry?: RawAvroSchema }>(
          KAFKA_CONSUMER_METADATA,
          descriptor?.value,
        )

        if (!metadadata?.topic) continue

        this.consumers.set(metadadata.topic, {
          descriptor,
          instance,
          metadadata,
          concurrency: metadadata.concurrency,
          topic: `${metadadata.topic}`,
          isDlq: false,
          kafkaConsumer: [],
        })

        if (this.configService.get<string>('MDM_ENVIRONMENT') === 'production') {
          this.consumers.set(`${metadadata.topic}_dlq`, {
            descriptor,
            instance,
            metadadata,
            concurrency: 1,
            topic: `${metadadata.topic}_dlq`,
            isDlq: true,
            kafkaConsumer: [],
          })
        }
      }
    }

    // this.logger.debug(
    //   {
    //     consumers: Array.from(this.consumers?.values() || []).map(({ concurrency, topic }) => ({ concurrency, topic })),
    //   },
    //   `Starting kafka consumers`,
    // )

    for await (const consumer of this.consumers.values()) {
      // this.logger.debug(
      //   { consumer: { topic: consumer.topic, concurrency: consumer.concurrency } },
      //   `Registering Kafka consumer for topic`,
      // )

      if (consumer.metadadata?.schemaRegistry) {
        // this.logger.debug(
        //   { consumer: { topic: consumer.topic, concurrency: consumer.concurrency } },
        //   `Registering schema for topic`,
        // )
        await this.registerSchemas({
          schema: consumer.metadadata.schemaRegistry,
          topic: consumer.topic,
        })
        // this.logger.debug(
        //   { consumer: { topic: consumer.topic, concurrency: consumer.concurrency } },
        //   `Schema registered for topic`,
        // )
      }

      const consumers = Array(consumer.concurrency || 1)
        .fill(0)
        .map(() =>
          this.createConsumer({
            topic: consumer.topic,
            concurrency: 1,
            onMessage: consumer.descriptor.value.bind(consumer.instance),
            isDlq: consumer.isDlq || false,
          }),
        )

      await Promise.all(consumers)
    }
  }

  /**
   * @description
   * Creates a Kafka consumer and subscribes it to a topic.
   * The consumer processes messages in batches, invoking the provided onMessage handler for each message.
   * If the topic does not exist, a warning is logged and the consumer is not created.
   *
   * @param params.topic - The Kafka topic to subscribe to.
   * @param params.onMessage - The message handler function.
   * @param params.concurrency - The number of partitions to consume concurrently.
   * @param params.isDlq - Flag indicating if the consumer is for a Dead Letter Queue (DLQ).
   */
  private async createConsumer({
    topic,
    onMessage,
    concurrency = 1,
    isDlq = false,
  }: {
    topic: string
    onMessage: (message: any) => Promise<void>
    concurrency?: number
    isDlq?: boolean
  }) {
    const topics = await this.admin.listTopics()
    // await setTimeout(12 * 1000) // Aguarda 12 segundos antes de iniciar os consumidores para garantir que o Kafka esteja pronto
    if (!topics.includes(topic)) {
      this.logger.warn({ topic }, 'TOPIC_DOES_NOT_EXIST')
      return
    }
    try {
      const consumerGroup = topic.replace(/_/g, '-') + this.sufixGroupId
      const consumerOptions = {
        groupId: `${consumerGroup.replace('-dlq', '')}${isDlq ? '-sending-to-dlq' : ''}`,
        readUncommitted: false,
        allowAutoTopicCreation: false,
        retry: this.retryPolicy,
        maxInFlightRequests: 1,
        maxWaitTimeInMs: 900,
        sessionTimeout: 60000,
        rebalanceTimeout: 60000,
        heartbeatInterval: 10000,
        minBytes: 1,
      }
      const consumer = this.kafka.consumer(consumerOptions)
      await consumer.connect()
      await consumer.subscribe({ topic: topic, fromBeginning: true })
      await consumer.run({
        autoCommit: false,
        autoCommitInterval: null,
        autoCommitThreshold: null,
        eachBatchAutoResolve: false,
        partitionsConsumedConcurrently: concurrency,
        eachBatch: async ({ batch, resolveOffset, heartbeat, isRunning, isStale }) => {
          if (!isRunning() || isStale()) return

          //// this.logger.debug(
          //   {
          //     topic: batch.topic,
          //     partition: batch.partition,
          //     size: batch.messages.length,
          //     firstOffset: batch.firstOffset(),
          //     lastOffset: batch.lastOffset(),
          //     isDlq,
          //   },
          //   `Batch received`,
          // )

          for await (const message of batch.messages) {
            if (!isDlq) {
              await this.handleMessage({
                topic: batch.topic,
                message,
                partition: batch.partition,
                onMessage,
                consumer,
                resolveOffset,
              })
            } else {
              await this.handleDLQMessage({
                topic: batch.topic,
                message,
                partition: batch.partition,
                onMessage,
                consumer,
                resolveOffset,
              })
            }

            await heartbeat()
          }
        },
        // eachMessage: async ({ message, partition, topic, heartbeat }) => {
        //   const parsedMessage = await this.parseMessage(message)
        //   try {
        //     await onMessage(parsedMessage)
        //   } catch (err) {
        //     const data = {
        //       parsedMessage,
        //       error: err,
        //       stack: err.stack,
        //       partition: partition,
        //       topic: topic,
        //       timestamp: message.timestamp,
        //       offset: message.offset,
        //       headers: message.headers,
        //       key: message.key,
        //     }
        //     this.logger.error(`Failed to process message on topic ${topic}`, data)
        //     const queueName = topic.replace(`${this.configService.get<string>('KAFKA_GROUP_ID')}-`, '')
        //     this.holdIt({
        //       queueName: `${queueName}_dlq`,
        //       message: data,
        //     })
        //       .then(() => {
        //         this.logger.warn(`Message sent to DLQ for topic ${topic}`, { queueName: `${queueName}_dlq` })
        //       })
        //       .catch(err2 => {
        //         this.logger.error(`Failed to send message to DLQ for topic ${topic}`, {
        //           error: err2,
        //           stack: err2.stack,
        //         })
        //       })
        //   } finally {
        //     await heartbeat()
        //     await consumer.commitOffsets([
        //       {
        //         topic: topic,
        //         partition: partition,
        //         offset: (parseInt(message.offset) + 1).toString(),
        //       },
        //     ])
        //   }
        // },
      })

      this.consumers.get(topic)!.kafkaConsumer?.push(consumer)
      this.logger.log({ topic }, `KAFKA_CONSUMER_STARTED`)
    } catch (err) {
      this.logger.error(
        { concurrency, topic },
        err instanceof Error ? err.stack : undefined,
        'ERROR_CREATING_KAFKA_CONSUMER',
      )
      throw err
    }
  }

  /**
   * @description
   * Handles messages from the main topic.
   * Attempts to process the message using the provided onMessage handler.
   * If processing fails, the message is sent to the Dead Letter Queue (DLQ).
   *
   * @param params.topic - The Kafka topic.
   * @param params.message - The Kafka message.
   * @param params.partition - The partition number.
   * @param params.onMessage - The message handler function.
   * @param params.consumer - The Kafka consumer instance.
   * @param params.resolveOffset - Function to resolve the message offset.
   */
  private async handleMessage({
    topic,
    message,
    partition,
    onMessage,
    consumer,
    resolveOffset,
  }: {
    topic: string
    partition: number
    message: KafkaMessage
    onMessage: (message: any) => Promise<void>
    consumer: Consumer
    resolveOffset: (offset: string) => void
  }) {
    //// this.logger.debug(
    //   {
    //     topic,
    //     partition,
    //     offset: message.offset,
    //     key: message.key?.toString('utf8'),
    //     timestamp: message.timestamp,
    //   },
    //   `Received message`,
    // )
    let failedEvenDlq = false

    const parsedMessage = await this.parseMessage(message)

    try {
      await onMessage(parsedMessage)
    } catch (err) {
      const queueName = topic.replace(`${this.configService.get<string>('KAFKA_GROUP_ID')}-`, '')
      const dlqTopic = `${queueName}_dlq`

      const envelope = this.kafkaDlqService.buildDlqEnvelope({
        parsedMessage,
        err,
        topic,
        partition,
        message,
        dlqTopic,
        serviceName: 'authoparts',
        environment: this.configService.get<string>('MDM_ENVIRONMENT', 'production'),
        groupId: this.configService.get<string>('KAFKA_GROUP_ID'),
        instanceId: process.env.HOSTNAME,
        attempt: 1,
      })

      try {
        await this.holdIt({
          queueName: dlqTopic,
          message: envelope,
          options: {
            key: message.key ? message.key.toString('utf8') : undefined,
            headers: message.headers,
            partition: partition,
          },
        })
      } catch (errorDlq) {
        failedEvenDlq = true
        this.logger.error(
          {
            error: this.kafkaDlqService.toDlqErrorInfo(errorDlq),
            dlqTopic,
            originalTopic: topic,
            errorMessage: errorDlq instanceof Error ? errorDlq.message : undefined,
          },
          errorDlq instanceof Error ? errorDlq.stack : undefined,
          'ERROR_SENDING_MESSAGE_TO_DLQ',
        )
      }
    } finally {
      if (!failedEvenDlq) {
        try {
          resolveOffset(message.offset)
          await consumer.commitOffsets([{ topic, partition, offset: (Number(message.offset) + 1).toString() }])
        } catch (commitError) {
          this.logger.error(
            {
              topic,
              offset: message.offset,
              partition,
            },
            commitError instanceof Error ? commitError.stack : undefined,
            'ERROR_RESOLVING_OFFSET',
          )
        }
      }
    }
  }

  /**
   * @description
   * Handles messages from the Dead Letter Queue (DLQ).
   *
   * Flow:
   * 1) Parse DLQ envelope
   * 2) Best-effort upsert into Necropolis (so we always have an audit trail)
   * 3) Backoff + try reprocess
   * 4) On success: remove from Necropolis and commit
   * 5) On failure:
   *    - If exceeded max attempts: ensure it is in Necropolis; if ES fails, DO NOT commit
   *    - Else: requeue with incremented attempt
   * 6) Commit offsets only if we are safe to do so
   */
  private async handleDLQMessage({
    topic,
    message,
    partition,
    onMessage,
    consumer,
    resolveOffset,
  }: {
    topic: string
    partition: number
    message: KafkaMessage
    onMessage: (message: any) => Promise<void>
    consumer: Consumer
    resolveOffset: (offset: string) => void
  }) {
    const ctx = this.buildDlqContext({ topic, partition, message })
    const dlqMessage = this.parseDlqEnvelope(message)

    this.logDlqReceived(ctx, dlqMessage)

    if (!dlqMessage) {
      // Nothing to process; commit to avoid poison loop
      await this.safeCommit({ topic, partition, message, consumer, resolveOffset })
      return
    }

    let shouldCommit = true
    const necropolisUpserted = await this.tryUpsertNecropolis(dlqMessage)

    try {
      await this.applyDlqBackoff(dlqMessage.attempt)
      await onMessage(dlqMessage.payload.parsedMessage)

      await this.tryRemoveNecropolis(dlqMessage)
      return
    } catch (err) {
      this.logDlqProcessingError(ctx, err)

      // If max attempts exceeded, we must ensure it is stored in Necropolis.
      if (this.isMaxAttemptsExceeded(dlqMessage.attempt)) {
        const ensured = necropolisUpserted ? true : await this.ensureNecropolisOrBlockCommit(dlqMessage)
        if (!ensured) shouldCommit = false // ES failed, do not commit offset
        return
      }

      // Not max attempts: requeue
      const requeued = await this.tryRequeueDlq({ topic, partition, message, dlqMessage })
      if (!requeued) shouldCommit = false
    } finally {
      if (shouldCommit) {
        await this.safeCommit({ topic, partition, message, consumer, resolveOffset })
      }
    }
  }

  private buildDlqContext({ topic, partition, message }: { topic: string; partition: number; message: KafkaMessage }) {
    return {
      topic,
      partition,
      offset: message.offset,
      key: message.key?.toString('utf8'),
      timestamp: message.timestamp,
    }
  }

  private parseDlqEnvelope(message: KafkaMessage): DlqEnvelope | null {
    if (!message.value) return null
    try {
      return JSON.parse(message.value.toString('utf8')) as DlqEnvelope
    } catch {
      return null
    }
  }

  private logDlqReceived(ctx: any, dlqMessage: DlqEnvelope | null) {
    //// this.logger.debug({ ...ctx, dlqMessage }, 'Received DLQ message')
  }

  private async applyDlqBackoff(attempt: number) {
    await setTimeout(this.computeDlqBackoffMs(attempt))
  }

  private isMaxAttemptsExceeded(attempt: number, maxAttempts = 12): boolean {
    return (attempt || 0) > maxAttempts
  }

  private async tryUpsertNecropolis(dlqMessage: DlqEnvelope): Promise<boolean> {
    try {
      await this.holdItElasticsearchService.sendToNecropolis(dlqMessage, { preserveFirstFailureAt: false })

      return true
    } catch (esErr) {
      this.logger.error(
        {
          failureId: dlqMessage.failureId,
          fingerprint: dlqMessage.fingerprint,
          attempt: dlqMessage.attempt,
          esError: this.kafkaDlqService.toDlqErrorInfo(esErr),
        },
        (esErr as any)?.stack,
        'ERROR_UPSERTING_DLQ_MESSAGE_IN_NECROPOLIS',
      )
      return false
    }
  }

  private async tryRemoveNecropolis(dlqMessage: DlqEnvelope): Promise<void> {
    try {
      await this.holdItElasticsearchService.removeFromNecropolis(dlqMessage)
    } catch (esErr) {
      // Remoção é best-effort. Não deve impedir commit do Kafka.
      this.logger.error(
        {
          failureId: dlqMessage.failureId,
          fingerprint: dlqMessage.fingerprint,
          esError: this.kafkaDlqService.toDlqErrorInfo(esErr),
        },
        (esErr as any)?.stack,
        'ERROR_REMOVING_DLQ_MESSAGE_FROM_NECROPOLIS',
      )
    }
  }

  private logDlqProcessingError(ctx: any, error: any) {
    this.logger.error(
      {
        ...ctx,
        error: this.kafkaDlqService.toDlqErrorInfo(error),
      },
      error?.stack,
      `ERROR_PROCESSING_DLQ_MESSAGE`,
    )
  }

  /**
   * @description
   * Ensures Necropolis contains the message. If ES fails, signals caller not to commit offset.
   *
   * @returns true if ensured; false if ES failed (block commit)
   */
  private async ensureNecropolisOrBlockCommit(dlqMessage: DlqEnvelope): Promise<boolean> {
    try {
      const necropolisResult = await this.holdItElasticsearchService.sendToNecropolis(dlqMessage, {
        preserveFirstFailureAt: false,
      })

      this.logger.warn(
        {
          failureId: dlqMessage.failureId,
          fingerprint: dlqMessage.fingerprint,
          attempt: dlqMessage.attempt,
          necropolis: necropolisResult,
        },
        'ERROR_DLQ_MESSAGE_EXCEEDED_MAX_ATTEMPTS',
      )

      return true
    } catch (esErr) {
      // Não pode perder: se não conseguiu salvar no ES, não comita.
      this.logger.error(
        {
          failureId: dlqMessage.failureId,
          fingerprint: dlqMessage.fingerprint,
          attempt: dlqMessage.attempt,
          esError: this.kafkaDlqService.toDlqErrorInfo(esErr),
        },
        (esErr as any)?.stack,
        'ERROR_STORING_DLQ_MESSAGE_IN_NECROPOLIS',
      )
      return false
    }
  }

  private async tryRequeueDlq({
    topic,
    partition,
    message,
    dlqMessage,
  }: {
    topic: string
    partition: number
    message: KafkaMessage
    dlqMessage: DlqEnvelope
  }): Promise<boolean> {
    try {
      await this.holdIt({
        queueName: topic,
        message: {
          ...dlqMessage,
          attempt: (dlqMessage.attempt || 1) + 1,
          occurredAt: new Date().toISOString(),
        },
        options: {
          key: message.key ? message.key.toString('utf8') : undefined,
          headers: message.headers,
          partition,
        } as KafkaJobOptions,
      })
      return true
    } catch (errorDlq) {
      this.logger.error(
        {
          error: this.kafkaDlqService.toDlqErrorInfo(errorDlq),
          dlqTopic: topic,
          errorMessage: errorDlq instanceof Error ? errorDlq.message : undefined,
        },
        errorDlq instanceof Error ? errorDlq.stack : undefined,
        `ERROR_REQUEUING_DLQ_MESSAGE`,
      )
      return false
    }
  }

  private async safeCommit({
    topic,
    partition,
    message,
    consumer,
    resolveOffset,
  }: {
    topic: string
    partition: number
    message: KafkaMessage
    consumer: Consumer
    resolveOffset: (offset: string) => void
  }) {
    try {
      resolveOffset(message.offset)
      await consumer.commitOffsets([{ topic, partition, offset: (Number(message.offset) + 1).toString() }])
    } catch (commitError) {
      this.logger.error(
        { offset: message.offset, partition, topic },
        (commitError as any)?.stack,
        `ERROR_COMMITTING_OFFSET`,
      )
    }
  }

  /**
   * @description
   * Computes exponential backoff for DLQ retries starting at 1 minute.
   *
   * attempt = 1  -> 1 min
   * attempt = 2  -> 2 min
   * attempt = 3  -> 4 min
   * ...
   * capped at maxBackoffMs
   */
  private computeDlqBackoffMs(attempt: number): number {
    const baseMs = 60_000 // 1 minute
    const maxBackoffMs = 60 * 60_000 // 1 hour

    const exp = Math.min(10, Math.max(0, attempt - 1))
    return Math.min(maxBackoffMs, baseMs * 2 ** exp)
  }

  /**
   * Registers Avro schemas with the Schema Registry.
   *
   * @param params.schema - The Avro schema to register.
   * @param params.topic - The Kafka topic associated with the schema.
   * @returns The schema ID assigned by the Schema Registry.
   */
  private async registerSchemas({ schema, topic }: { schema: RawAvroSchema; topic: string }) {
    try {
      const { id } = await this.schemaRegistry.register(
        {
          type: 'AVRO',
          schema: JSON.stringify(schema),
        } as AvroConfluentSchema,
        { subject: `${topic}-value`, compatibility: COMPATIBILITY.NONE },
      )
      this.schemaIds.set(topic, id)

      // this.logger.debug({ topic, schemaId: id }, `Registered default schema`)

      return id
    } catch (err) {
      this.logger.error(
        {
          topic,
          message: err instanceof Error ? err.message : undefined,
        },
        err instanceof Error ? err.stack : undefined,
        'ERROR_REGISTERING_SCHEMAS',
      )
      throw err
    }
  }

  /**
   * Parses a Kafka message using Avro schema.
   *
   * @param message - The Kafka message to parse.
   * @returns The parsed message value.
   */
  private async parseMessage(message: KafkaMessage) {
    const buf = message.value as Buffer
    if (!buf) return null

    const schemaId = buf.readInt32BE(1)
    let type = this.avroTypeCache.get(schemaId)

    if (!type) {
      const schema = await this.schemaRegistry.getSchema(schemaId)
      type = avro.Type.forSchema(schema as avro.Schema)
      this.avroTypeCache.set(schemaId, type)
    }

    const payload = buf.slice(5)
    const { value } = type.decode(payload)
    return value
  }

  /**
   * Sends a single message to the specified Kafka topic.
   *
   * @param params.queueName - The topic (queue) to send the message.
   * @param params.message - The message payload.
   * @param params.options - Additional options such as key and headers.
   * @returns A promise that resolves to an array of RecordMetadata.
   */
  async holdIt<T = HoldItSimpleJobDataDTO>({
    queueName,
    message,
    options,
  }: HoldItJobDTO<T>): Promise<RecordMetadata[]> {
    const topic = queueName || ''

    try {
      const schemaRegistryId = this.schemaIds.has(topic)
        ? this.schemaIds.get(topic)
        : await this.schemaRegistry.getLatestSchemaId(`${topic}-value`)

      if (!schemaRegistryId) {
        const kafkaMessage: Message = {
          value: JSON.stringify(message),
          key: (options as any)?.key || undefined,
          headers: (options as any)?.headers || undefined,
          partition: (options as any)?.partition || undefined,
        }
        return this.producer.send({
          topic,
          messages: [kafkaMessage],
          compression: CompressionTypes.GZIP,
          acks: 1,
        })
      }

      const encodedMessage = await this.schemaRegistry.encode(schemaRegistryId, message)
      const kafkaMessage: Message = {
        value: encodedMessage,
        key: (options as any)?.key || undefined,
        headers: (options as any)?.headers || undefined,
        partition: (options as any)?.partition || undefined,
      }

      return this.producer.send({
        topic,
        messages: [kafkaMessage],
        compression: CompressionTypes.GZIP,
        acks: 1,
      })
    } catch (error) {
      try {
        const kafkaMessage: Message = {
          value: JSON.stringify(message),
          key: (options as any)?.key || undefined,
          headers: (options as any)?.headers || undefined,
          partition: (options as any)?.partition || undefined,
        }
        return this.producer.send({
          topic,
          messages: [kafkaMessage],
          compression: CompressionTypes.GZIP,
          acks: 1,
        })
      } catch (error) {
        this.logger.error(
          { topic, message },
          error instanceof Error ? error.stack : undefined,
          'ERROR_SENDING_MESSAGE_TO_KAFKA',
        )
        throw error
      }
    }
  }

  /**
   * Sends multiple messages to the specified Kafka topic.
   *
   * @param params.queueName - The Kafka topic.
   * @param params.messages - An array of messages to send.
   * @returns A promise that resolves to an array of RecordMetadata for the sent batch.
   */
  async holdItALot<T = HoldItSimpleJobDataDTO>({
    queueName,
    messages,
  }: HoldItALotOfJobDTO<T>): Promise<RecordMetadata[]> {
    const topic = queueName || ''
    try {
      const schemaRegistryId = this.schemaIds.has(topic)
        ? this.schemaIds.get(topic)
        : await this.schemaRegistry.getLatestSchemaId(`${topic}-value`)

      const kafkaMessages: Message[] = []

      if (!schemaRegistryId) {
        for await (const message of messages) {
          kafkaMessages.push({
            value: JSON.stringify(message),
            key: (message.jobOptions as any)?.key || undefined,
            headers: (message.jobOptions as any)?.headers || undefined,
            partition: (message.jobOptions as any)?.partition || undefined,
          })
        }
      }

      try {
        return this.producer.send({
          topic,
          messages: kafkaMessages,
          compression: CompressionTypes.GZIP,
          acks: 1,
        })
      } catch (error) {
        let firstMessage = messages[0]
        if (Array.isArray(firstMessage)) {
          firstMessage = firstMessage[0]
          this.logger.warn({ firstMessage }, 'BATCH_MESSAGE_IS_ARRAY')
        }

        this.logger.error(
          { topic, examplePayload: firstMessage },
          error instanceof Error ? error.stack : undefined,
          'ERROR_SENDING_BATCH_MESSAGES_TO_KAFKA',
        )
        throw error
      }
    } catch (error) {
      try {
        const kafkaMessages: Message[] = messages.map(msg => ({
          value: JSON.stringify(msg),
          key: (msg.jobOptions as any)?.key || undefined,
          headers: (msg.jobOptions as any)?.headers || undefined,
          partition: (msg.jobOptions as any)?.partition || undefined,
        }))

        return this.producer.send({
          topic,
          messages: kafkaMessages,
          compression: CompressionTypes.GZIP,
          acks: 1,
        })
      } catch (error) {
        this.logger.error(
          { topic, firstMessage: messages[0] },
          error instanceof Error ? error.stack : undefined,
          'ERROR_SENDING_BATCH_MESSAGES_TO_KAFKA',
        )
        throw error
      }
    }
  }

  /**
   * Sends multiple messages to the specified Kafka topic using sendBatch for better performance.
   *
   * @param params.queueName - The Kafka topic.
   * @param params.messages - An array of messages to send.
   * @returns A promise that resolves to an empty array for compatibility with the interface.
   */
  async holdItBatch<T = HoldItSimpleJobDataDTO>({ queueName, messages }: HoldItALotOfJobDTO<T>): Promise<any[]> {
    const kafkaMessages: Message[] = messages.map(msg => ({
      value: JSON.stringify(msg),
      key: (msg.jobOptions as any)?.key || undefined,
      headers: (msg.jobOptions as any)?.headers || undefined,
      partition: (msg.jobOptions as any)?.partition || undefined,
    }))

    return this.producer.sendBatch({
      topicMessages: [
        {
          topic: queueName || '',
          messages: kafkaMessages,
        },
      ],
    })
  }

  /**
   * OnModuleDestroy is called once the module is about to be destroyed.
   * This hook ensures that all Kafka producers are disconnected gracefully.
   */
  async onModuleDestroy() {
    await this.shutdown()
  }

  /**
   * Gracefully shuts down all Kafka producers.
   *
   * Closes active connections to Kafka and clears the producer map.
   *
   * @returns A promise that resolves once all consumers and producers are disconnected.
   */
  async shutdown(): Promise<void> {
    await Promise.all([
      this.producer?.disconnect(),
      this.admin?.disconnect(),
      ...Array.from(this.consumers.values()).flatMap(
        ({ kafkaConsumer }) => kafkaConsumer?.map(consumer => consumer.disconnect()) || [],
      ),
    ])
  }

  /**
   * @returns Kafka client instance
   */
  getClient() {
    return this.kafka
  }

  /**
   * @returns Kafka producer
   */
  getProducer() {
    return this.producer
  }

  /**
   * @returns Kafka consumers
   */
  getConsumers() {
    return this.consumers
  }

  getSchemaRegistry(): SchemaRegistry {
    if (!this.schemaRegistry) {
      this.schemaRegistry = new SchemaRegistry({
        host: this.configService.get<string>('SCHEMA_REGISTRY_HOST', 'http://digital-schema-registry:8081'),
        retry: this.retryPolicy,
      })
    }

    return this.schemaRegistry
  }
}
