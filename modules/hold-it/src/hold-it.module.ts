import { DiscoveryModule, ModuleRef } from '@nestjs/core'
import { DynamicModule, Global, Injectable, Module, Provider, Inject } from '@nestjs/common'
import { HoldItBullMQBroker } from './services/brokers/bull-mq'
import { BullModule, getQueueToken } from '@nestjs/bullmq'
import { ConfigModule, ConfigService } from '@nestjs/config'
import { FastifyAdapter as BullFastifyAdapter } from '@bull-board/fastify'
import { BullMQAdapter } from '@bull-board/api/bullMQAdapter'
import { createBullBoard } from '@bull-board/api'
import { HoldItKafkaBroker } from './services/brokers/kafka'
import { KafkaDlqService } from './services/brokers/kafka/dlq'
import { Queue } from 'bullmq'
import { BullMQController } from './controllers/bullmq'
import { BullMQService } from './services/bullmq'
import { HOLD_IT_QUEUE_NAMES } from './types'

// export const queueNames = [
//   // 'vtex_start_product_integration',
//   // 'vtex_import_product_by_sku',
//   // 'algolia_create_product_by_vtex_sku',
//   // 'algolia_remove_product_by_vtex_sku',
//   // 'algolia_create_synonym_by_product_group_and_category_tree',
//   // 'listAndUpdateSellerQueue',
//   // 'registerSellerQueue',
//   // 'createOrderDBQueue',
//   // 'createOrderItemDB',
//   // 'pullVtexOrderQueue',
//   // 'createOrderTrackingDB',
//   // 'syncProductQueue',
//   // 'registerProductQueue',
//   // 'vtex_category',
//   // 'vtex_create_category_integration',
//   // 'vtex_product_enrichment',
//   // 'update-order-history',
//   // 'vtex-order-webhook',

//   'category_process_by_sheet',
//   'intelliauto_start_import_products',
//   'intelliauto_start_import_product_groups',
//   'intelliauto_import_product',
//   'intelliauto_import_product_by_distributor_part_number',
//   'intelliauto_import_product_group',
//   'intelliauto_import_product_attribute',
//   'intelliauto_import_vehicle',
//   'intelliauto_start_import_vehicles',
//   'intelliauto_enriching_vehicle_by_source_sheet',
//   'sheeter_process_by_queue',
//   'associate_category_by_sheet',
//   'product_process_by_sheet',
//   'start_product_xls_integration',
//   'start_enriching_vehicle_data_by_sheets',
//   'tecdoc_map_table_by_sheet',
//   'import_product_by_source',
//   'import_product_by_source_sheet',
//   'enriching_vehicle_by_source_sheet',
//   'enriching_vehicle_by_source',
//   'import_source_vehicle',
//   'product_reindex',
//   'product_reindex_by_info',
//   'product_reindex_by_product',
//   'product_reindex_by_stock',
//   'pullProductsFromVtexQueue',
//   'remove_intelliauto_product_gallery_not_found',
//   'product-report-interviewee-182-compare',
//   'start-product-report-interviewee-182-compare',
//   'next-product-reindex',
//   'create_product',
//   'send_product_to_algolia',
//   'send_product_to_elasticsearch',
//   'create_brand_mapped_suggestion',
// ]
// 
// export const getQueuesNames = (): string[] => queueNames


// const queueMap = BullModule.registerQueue(...queueNames.map(name => ({ name, forceDisconnectOnShutdown: true })))

const queueMap = (queues: string[]) => BullModule.registerQueue(...queues.map(name => ({ name })))

export const getProviderMap = ({ withKafkaBrokers = true }: { withKafkaBrokers?: boolean }): Provider[] => {
  const providers: Provider[] = [HoldItBullMQBroker]

  if (withKafkaBrokers) {
    providers.push(HoldItKafkaBroker)
    providers.push(KafkaDlqService)
  }

  return providers
}
// const providerMap: Provider[] = [HoldItBullMQBroker, HoldItKafkaBroker, KafkaDlqService]

@Global()
@Module({})
export class HoldItModule {
  static register(queues?: string[]): DynamicModule {
    const selectedQueues = queues ?? [];
    const queuesMap = queueMap(selectedQueues)
    const queueNamesProvider: Provider = {
      provide: HOLD_IT_QUEUE_NAMES,
      useValue: selectedQueues,
    }
    return {
      module: HoldItModule,
      global: true,
      imports: [
        DiscoveryModule,
        BullModule.forRootAsync({
          imports: [ConfigModule],
          useFactory: async (configService: ConfigService) => ({
            connection: {
              noDelay: false,
              maxLoadingRetryTime: 6 * 60 * 1000, // 12 minutes
              lazyConnect: true,
              commandTimeout: 60 * 1000, // 1 minute
              sentinelCommandTimeout: 60 * 1000, // 1 minute
              host: configService.get<string>('REDIS_QUEUE_HOST'),
              port: configService.get<number>('REDIS_QUEUE_PORT'),
              sentinelMaxConnections: 4,
              retryDelayOnClusterDown: 60 * 1000, // 1 minute
              retryDelayOnFailover: 60 * 1000, // 1 minute
              retryDelayOnReconnect: 60 * 1000, // 1 minute
              retryDelayOnTryAgain: 60 * 1000, // 1 minute
              retryDelayOnMoved: 60 * 1000, // 1 minute

              // password: configService.get<string>('QUEUE_PASSWORD'),
            },
            defaultJobOptions: {
              attempts: 0,
              backoff: { type: 'fixed', delay: 1200 },
              timeout: 120 * 1000, // 120 seconds
              removeOnComplete: { age: 7200, count: 1 },
              removeOnFail: false,
              stackTraceLimit: 4,
            },
          }),
          inject: [ConfigService],
        }),
        queuesMap,
      ],
      providers: [
        ...getProviderMap({
          withKafkaBrokers: Boolean(process.env.WITH_KAFKA_BROKERS) ? process.env.WITH_KAFKA_BROKERS === 'true' : true,
        }),
        queueNamesProvider,
        HoldItQueueBoardService,
        BullMQService,
      ],
      exports: [
        ...getProviderMap({
          withKafkaBrokers: Boolean(process.env.WITH_KAFKA_BROKERS) ? process.env.WITH_KAFKA_BROKERS === 'true' : true,
        }),
        queueNamesProvider,
        queuesMap,
        HoldItQueueBoardService,
      ],
      // providers: [...providerMap, HoldItQueueBoardService, HoldItElasticsearchService, BullMQService],
      // exports: [...providerMap, queueMap, HoldItQueueBoardService],
      controllers: [
        BullMQController
      ],
    }
  }
}

@Injectable()
export class HoldItQueueBoardService {
  private queuesAdapters = new Map<string, BullMQAdapter>()

  constructor(
    private readonly moduleRef: ModuleRef,
    @Inject(HOLD_IT_QUEUE_NAMES) private readonly queueNames: string[]
  ) { }

  getQueuesAdapters(): BullMQAdapter[] {
    for (const name of this.queueNames) {
      if (!this.queuesAdapters.has(name)) {
        const adapter = new BullMQAdapter(this.getQueue(name), { readOnlyMode: false, allowRetries: true })
        this.queuesAdapters.set(name, adapter)
      }
    }
    return Array.from(this.queuesAdapters.values())
  }

  private getQueue(name: string): Queue {
    return this.moduleRef.get<Queue>(getQueueToken(name), { strict: false })
  }

  getServerAdapter(): BullFastifyAdapter {
    const serverAdapter = new BullFastifyAdapter()
    serverAdapter.setBasePath('/queue-manager')
    return serverAdapter
  }

  setupBoard() {
    const serverAdapter = this.getServerAdapter()
    const queuesAdapters = this.getQueuesAdapters()

    createBullBoard({
      queues: queuesAdapters,
      serverAdapter: serverAdapter as any,
      options: {
        uiConfig: {
          boardTitle: 'Digital Bull Queue',
          pollingInterval: { showSetting: true, forceInterval: 5 },
        },
      },
    })

    return serverAdapter.registerPlugin()
  }
}
