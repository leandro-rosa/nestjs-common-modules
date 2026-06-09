import { Module } from '@nestjs/common';
import { LoggerService } from './logger.service';
import { createLogger } from './factory/create-winston';
import { Logger } from 'winston';

@Module({
  providers: [
    {
      provide: LoggerService,
      useFactory: (): LoggerService => {
        const serviceName = process.env.APP_NAME || 'app';
        const winstonLogger: Logger = createLogger(serviceName);
        return new LoggerService(winstonLogger, serviceName);
      },
    },
  ],
  exports: [LoggerService],
})
export class LoggerModule {}
