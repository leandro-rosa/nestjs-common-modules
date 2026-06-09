import { Inject, Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common'

export const PRISMA_CLIENT = Symbol('PRISMA_CLIENT')

export interface PrismaClientLifecycle {
  $connect(): Promise<void>
  $disconnect(): Promise<void>
}

export interface PrismaClientOptions {
  adapter: unknown
  log: Array<'warn' | 'error'>
}

export type PrismaClientConstructor<TClient extends PrismaClientLifecycle = PrismaClientLifecycle> = new (
  ...args: never[]
) => TClient

@Injectable()
export class PrismaClientService<TClient extends PrismaClientLifecycle = PrismaClientLifecycle>
  implements OnModuleInit, OnModuleDestroy
{
  constructor(@Inject(PRISMA_CLIENT) private readonly prismaClient: TClient) {}

  getClient(): TClient {
    return this.prismaClient
  }

  async onModuleInit() {
    await this.prismaClient.$connect()
  }

  async onModuleDestroy() {
    await this.prismaClient.$disconnect()
  }
}
