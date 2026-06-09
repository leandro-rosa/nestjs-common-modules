import { SetMetadata } from '@nestjs/common'

export function JobInit() {
  return function (_target: any, _propertyKey: string, descriptor: PropertyDescriptor) {
    descriptor.value = async function (...args: any[]) {
      const job = args[0]
      SetMetadata('current_job', job)
    }

    return descriptor
  }
}
