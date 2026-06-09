import { Module, Global } from '@nestjs/common';
import { CacheModule as NestCacheModule } from '@nestjs/cache-manager';
import * as redisStore from 'cache-manager-ioredis';
import { CacheService } from './cache.service';

@Global()
@Module({
  imports: [
    NestCacheModule.register({
      store: redisStore,
      host: process.env.REDIS_HOST,
      port: process.env.REDIS_PORT,
      ttl: 60,
    }),
  ],
  providers: [CacheService],
  exports: [CacheService],
})
export class CacheModule {}
