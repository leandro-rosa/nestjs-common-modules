import { Injectable } from '@nestjs/common';
import { HealthIndicatorResult } from '@nestjs/terminus';

@Injectable()
export class PostgresHealthIndicator {
  constructor() {}

  async isHealthy(key: string): Promise<HealthIndicatorResult> {
    try {
      const resp =  {current_database: 'unknown - service not implemented'}//await this.prisma.$queryRawUnsafe('SELECT current_database();') as Array<{ current_database: string }>;
      return {
        [key]: {
          status: 'up',
          database: resp[0]?.current_database,
        },
      };
    } catch (error) {
      return {
        [key]: {
          status: 'down',
          message: (error as Error).message,
        },
      };
    }
  }
}
