import { Global, Module } from '@nestjs/common'
import { ElasticsearchClientService } from './services/client'

@Global()
@Module({
  providers: [ElasticsearchClientService],
  exports: [ElasticsearchClientService],
  controllers: [],
})
export class ElasticsearchModule {}
