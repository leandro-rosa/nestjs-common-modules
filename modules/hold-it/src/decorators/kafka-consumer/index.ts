import { RawAvroSchema } from '@kafkajs/confluent-schema-registry/dist/@types'
import { SetMetadata } from '@nestjs/common'

export const KAFKA_CONSUMER_METADATA = 'KAFKA_CONSUMER_METADATA'

export const KafkaTopics = (topicOptions: { topic: string; concurrency?: number; schemaRegistry?: RawAvroSchema }) =>
  SetMetadata(KAFKA_CONSUMER_METADATA, topicOptions)
