import { Global, Module, DynamicModule, Provider } from '@nestjs/common'

import { ConfigService } from '@nestjs/config'
import { PrismaPg } from '@prisma/adapter-pg'

import {
  PRISMA_CLIENT,
  PrismaClientConstructor,
  PrismaClientLifecycle,
  PrismaClientOptions,
  PrismaClientService,
} from './services/client'

export interface PrismaDbClientModuleOptions<TClient extends PrismaClientLifecycle = PrismaClientLifecycle> {
  prismaClient: PrismaClientConstructor<TClient>
  repositories?: Provider[]
}

@Global()
@Module({})
export class PrismaDbClientModule {
  static forRoot<TClient extends PrismaClientLifecycle = PrismaClientLifecycle>({
    prismaClient,
    repositories = [],
  }: PrismaDbClientModuleOptions<TClient>): DynamicModule {
    return {
      module: PrismaDbClientModule,
      providers: [
        {
          provide: PrismaPg,
          useFactory: async (configService: ConfigService) => {
            const connectionString = configService.get<string>('DATABASE_URL')
            return new PrismaPg({ connectionString })
          },
          inject: [ConfigService],
        },
        {
          provide: PRISMA_CLIENT,
          useFactory: (prismaPg: PrismaPg): TClient => {
            const ConfiguredPrismaClient = prismaClient as unknown as new (options: PrismaClientOptions) => TClient

            return new ConfiguredPrismaClient({ adapter: prismaPg, log: ['warn', 'error'] })
          },
          inject: [PrismaPg],
        },
        PrismaClientService,
        ...repositories,
      ],
      exports: [PRISMA_CLIENT, PrismaClientService, ...repositories],
    }
  }
}
