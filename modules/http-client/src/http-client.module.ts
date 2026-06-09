import { forwardRef, Global, Module } from '@nestjs/common'
import { HttpModule } from '@nestjs/axios'
import { AxiosHttpClient } from './services/client/axios'
import { GraphQLClientService } from './services/client/graphql'

@Global()
@Module({
  imports: [forwardRef(() => HttpModule)],
  providers: [AxiosHttpClient, GraphQLClientService],
  exports: [AxiosHttpClient, GraphQLClientService],
})
export class HttpClientModule {}
