import { Injectable } from '@nestjs/common';
import { Logger } from 'winston';
import { createLogger } from './factory/create-winston';

@Injectable()
export class LoggerService {
  private readonly logger: Logger;
  private readonly serviceName: string;
  private readonly loggers = new Map<string, Logger>();

  constructor(loggerInstance: Logger, serviceName: string) {
    this.logger = loggerInstance;
    this.serviceName = serviceName;
  }

  info(message: string, meta?: Record<string, unknown>) {
    this.logger.info(message, meta);
  }

  error(message: string, meta?: Record<string, unknown>) {
    this.logger.error(message, meta);
  }

  warn(message: string, meta?: Record<string, unknown>) {
    this.logger.warn(message, meta);
  }

  getRawLogger(serviceName = this.serviceName): Logger {
    if (!this.loggers.has(serviceName)) {
      this.loggers.set(serviceName, createLogger(serviceName));
    }
    return this.loggers.get(serviceName)!;
  }
}
