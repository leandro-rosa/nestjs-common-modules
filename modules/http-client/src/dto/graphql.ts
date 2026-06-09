import { IsOptional, IsString } from 'class-validator'

export class GraphQLRequestDTO<V> {
  url: string

  query: string

  variables?: V

  headers?: Record<string, string>

  logEntityType?: string

  logEntityId?: number | string
}
