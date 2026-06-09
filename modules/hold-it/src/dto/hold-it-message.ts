import { BulkJobOptions } from 'bullmq/dist/esm/interfaces'
import { JobsOptions } from 'bullmq/dist/esm/types'

export class HoldItJobDTO<T = HoldItSimpleJobDataDTO> {
  queueName: string | undefined

  options?: JobsOptions | KafkaJobOptions | any

  message: T

  onComplete?: (job: any, result: any) => Promise<void>
}

export class KafkaJobOptions<H = any> {
  key: string
  headers: Record<string, H>
}

export class HoldItALotOfJobDTO<T = HoldItSimpleJobDataDTO> {
  queueName: string
  messages: Array<T & { jobOptions?: BulkJobOptions }>
}


export class HoldItSimpleJobDataDTO {
  page?: number
}
