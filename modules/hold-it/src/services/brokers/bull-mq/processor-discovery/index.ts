import { Injectable, OnModuleInit } from '@nestjs/common'
import { DiscoveryService } from '@nestjs/core'

// Obs: a chave exata é interna da lib; abaixo uso o valor mais comum referenciado como PROCESSOR_METADATA.
const PROCESSOR_METADATA = 'processor_metadata'

type ProcessorMetadata = {
  name?: string // queue name
  // em algumas versões pode ter outras props (config, scope, etc.)
}

@Injectable()
export class BullmqProcessorDiscoveryService implements OnModuleInit {
  private readonly queueNames = new Set<string>()

  constructor(private readonly discoveryService: DiscoveryService) {}

  onModuleInit() {
    this.queueNames.clear()

    const providers = this.discoveryService.getProviders()

    for (const wrapper of providers) {
      const target = wrapper.metatype
      if (!target) continue

      const meta = Reflect.getMetadata(PROCESSOR_METADATA, target) as ProcessorMetadata | undefined
      const queueName = meta?.name

      if (queueName) this.queueNames.add(queueName)
    }
  }

  getDiscoveredQueueNames(): string[] {
    return Array.from(this.queueNames).sort()
  }
}
