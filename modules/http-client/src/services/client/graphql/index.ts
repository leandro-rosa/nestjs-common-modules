import { GraphQLRequestDTO } from '../../../dto/graphql'
import { Injectable } from '@nestjs/common'
import { AxiosHttpClient } from '../axios'
import { ResponseDTO } from '../axios/types'

@Injectable()
export class GraphQLClientService {
  constructor(private readonly httpRequest: AxiosHttpClient) {}

  async executeQuery<T, V = any>({
    url,
    query,
    variables,
    headers,
    logEntityId,
    logEntityType,
  }: GraphQLRequestDTO<V>): Promise<ResponseDTO<Partial<T>>> {
    return this.httpRequest.send<T>({
      headers,
      url,
      http_method: 'POST',
      payload: { query, variables },
      entity_id: logEntityId,
      entity_type: logEntityType,
    })
  }
}
