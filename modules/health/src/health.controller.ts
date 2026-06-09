import { Controller, Get } from '@nestjs/common'
import { RedisOptions, Transport } from '@nestjs/microservices'

import { HealthCheck, HealthCheckService, MicroserviceHealthIndicator } from '@nestjs/terminus'
import { PostgresHealthIndicator } from './custom/postgres.health'

@Controller('health')
export class HealthController {
  constructor(
    private readonly health: HealthCheckService,
    private readonly postgresHealthIndicator: PostgresHealthIndicator,
    private readonly microservice: MicroserviceHealthIndicator,
  ) {}

  @Get()
  @HealthCheck()
  async check() {
    try {
      return this.health.check([
        () => this.postgresHealthIndicator.isHealthy('POSTGRES'),
        // () =>
        //   this.microservice.pingCheck<RedisOptions>('REDIS', {
        //     transport: Transport.REDIS,
        //     options: {
        //       host: process.env.QUEUE_HOST,
        //       port: parseInt(String(process.env.QUEUE_PORT), 10),
        //       enableReadyCheck: true,
        //     },
        //   }),
      ])
    } catch (error) {
      return {
        status: 'error',
        error: error,
      }
    }
  }
}
