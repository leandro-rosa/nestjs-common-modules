import { Injectable, Logger, OnModuleInit, Optional } from '@nestjs/common'
import { ConfigService } from '@nestjs/config'
import { Client } from '@elastic/elasticsearch'

type GuardQueueBroker = {
  holdIt(params: { message: unknown; queueName: string }): Promise<unknown>
}

@Injectable()
export class ElasticsearchClientService implements OnModuleInit {
  private readonly logger: Logger = new Logger(ElasticsearchClientService.name)

  protected INSTANCE?: Client

  constructor(
    private readonly configService: ConfigService,
    @Optional() private readonly bullBroker?: GuardQueueBroker,
  ) {}

  onModuleInit() {
    if (this.INSTANCE) {
      return
    }
    this.createInstance()
  }

  private async createInstance() {
    try {
      this.initializeClient()
      // this.logger.debug({ instance: this.INSTANCE }, 'ELASTICSEARCH_CLIENT_INITIALIZED')
    } catch (error) {
      this.logger.error(
        { message: error instanceof Error ? error.message : 'Unknown error' },
        error instanceof Error ? error.stack : undefined,
        'ELASTICSEARCH_CLIENT_INITIALIZATION_FAILED',
      )
      throw error
    }
  }

  private initializeClient() {
    this.INSTANCE = new Client({
      name: 'autho-elasticsearch-client',
      node: this.configService.get<string>('ELASTIC_SEARCH_URI') || 'http://localhost:9200',
      requestTimeout: 60 * 1000,
      compression: true,
      maxRetries: 12,
      sniffOnStart: false,

      auth: {
        username: this.configService.get<string>('ELASTIC_SEARCH_USER') || '',
        password: this.configService.get<string>('ELASTIC_SEARCH_PASSWORD') || '',
        apiKey: this.configService.get<string>('ELASTIC_SEARCH_API_KEY') || undefined,
      },
    })
  }

  getInstance(): Client {
    if (!this.INSTANCE) {
      this.createInstance()
    }
    if (!this.INSTANCE) {
      throw new Error('Failed to initialize Elasticsearch client instance')
    }
    return this.INSTANCE
  }

  public async fetchPage<T>({
    pitId,
    sortAttribute,
    pageSize,
    lastId,
    keepAlive,
    query,
    source,
    collapseAttribute,
    guardOptions = {},
  }: {
    pitId: string
    sortAttribute: string
    pageSize?: number
    lastId?: any[]
    keepAlive?: string
    query?: any
    source?: string[]
    collapseAttribute?: string
    guardOptions?: Record<string, unknown>
  }): Promise<{ items: T[]; lastSort?: any[] }> {
    if (!this.INSTANCE) {
      throw new Error('Elasticsearch client instance is not initialized')
    }

    const payload: any = {
      size: pageSize,
      sort: [
        {
          [sortAttribute]: { order: 'asc' },
        },
      ],
      pit: {
        id: pitId,
        keep_alive: keepAlive,
      },
      _source: source,
      query: query
        ? query
        : {
            match_all: {},
          },
    }

    if (collapseAttribute) {
      payload.collapse = { field: collapseAttribute }
    }

    if (lastId?.length) {
      payload.search_after = lastId
    }

    try {
      const response = await this.INSTANCE.search<T>({ ...payload })

      const hits = response?.hits?.hits || []
      const items = hits?.map((h: any) => h._source as T)
      const lastSort = hits.length ? hits[hits.length - 1]?.sort : []

      if (this.bullBroker && Object.keys(guardOptions).length > 0) {
        await this.bullBroker.holdIt({
          message: { payload, response, guardOptions },
          queueName: 'elasticsearch-guard',
        })
      }
      // this.logger.debug({ pitId, pageSize, lastId, itemsFetched: items.length }, 'ELASTICSEARCH_FETCH_PAGE_SUCCESS')
      return { items, lastSort }
    } catch (error) {
      this.logger.error(
        { payload, error },
        error instanceof Error ? error.stack : undefined,
        'ELASTICSEARCH_FETCH_PAGE_FAILED',
      )
      throw error
    }
  }

  public async openPIT(index: string, keepAlive: string): Promise<string> {
    if (!this.INSTANCE) {
      throw new Error('Elasticsearch client instance is not initialized')
    }
    try {
      const result = await this.INSTANCE.openPointInTime({ index, keep_alive: keepAlive })

      if (!result?.id) {
        this.logger.error({ index, result }, 'ELASTICSEARCH_OPEN_PIT_FAILED')
        throw new Error(`Failed to open PIT on "${index}": ${JSON.stringify(result)}`)
      }

      const pitId = result.id
      // this.logger.debug({ index, pitId }, 'ELASTICSEARCH_PIT_OPENED')
      return pitId
    } catch (error) {
      this.logger.error({ index, error }, 'ELASTICSEARCH_OPEN_PIT_EXCEPTION')
      throw error
    }
  }

  public async closePIT(pitId: string) {
    if (!this.INSTANCE) {
      throw new Error('Elasticsearch client instance is not initialized')
    }
    try {
      await this.INSTANCE.closePointInTime({ id: pitId })
      // this.logger.debug({ pitId }, 'ELASTICSEARCH_PIT_CLOSED')
    } catch (error) {
      this.logger.error({ pitId, error }, 'ELASTICSEARCH_CLOSE_PIT_FAILED')
      throw error
    }
  }
}
