import { Controller, Logger, Post } from '@nestjs/common'
import { HoldItElasticsearchService } from '../services/elasticsearch'

/**
 * @description
 * Administrative controller responsible for provisioning the Kafka DLQ "necropolis" index family
 * with an ILM policy that retains data for ~3 months (90 days).
 *
 * Strategy:
 * - ILM policy: rollover + delete after 90d
 * - Index template: applies mappings/settings + policy to kafka-necropolis-*
 * - Write alias: kafka-necropolis -> points to current write index
 */
@Controller('holdit/elasticsearch')
export class ElasticsearchController {
  private readonly logger = new Logger(ElasticsearchController.name)

  constructor(private readonly holdItElasticsearchService: HoldItElasticsearchService) {}

  /**
   * @description
   * Creates or updates the ILM policy, index template, and initial write index for kafka-necropolis.
   * This endpoint is safe to run multiple times (idempotent), except the index creation step which
   * only runs if the initial index does not exist.
   */
  @Post('kafka-necropolis-provision')
  async provisionKafkaNecropolis() {
    return this.holdItElasticsearchService.provisionKafkaNecropolis()
  }
}
