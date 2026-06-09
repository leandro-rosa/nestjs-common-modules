import { Controller, Get, Logger, Query } from '@nestjs/common'
import { BullMQService } from '../services/bullmq'

@Controller('holdit/bullmq')
export class BullMQController {
  private readonly logger = new Logger(BullMQController.name)
  constructor(private readonly bullMQService: BullMQService) {}

  @Get('monitor')
  async monitorQueues() {
    return this.bullMQService.monitorQueues()
  }

  @Get('reprocess-failed')
  async reprocessFailedJobs(@Query() body: { maxPerQueue?: number; batchSize?: number; retryConcurrency?: number }) {
    return this.bullMQService.reprocessFailedJobs({
      maxPerQueue: body.maxPerQueue ?? 50,
      batchSize: body.batchSize ?? 10,
      retryConcurrency: body.retryConcurrency ?? 1,
      dryRun: false,
      skipIfActive: false,
      lockKeyPrefix: 'holdit:api:reprocessFailed',
      lockTtlMs: 60 * 1000,
    })
  }
}
