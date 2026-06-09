import { Module } from '@nestjs/common'
import { TerminusModule } from '@nestjs/terminus'
import { PostgresHealthIndicator } from './custom/postgres.health'
import { HealthController } from './health.controller'

//@TODO Refactor this module
@Module({
  imports: [TerminusModule],
  providers: [PostgresHealthIndicator],
  controllers: [HealthController],
})
export class HealthModule {}
