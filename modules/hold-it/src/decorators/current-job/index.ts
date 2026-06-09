import { createParamDecorator, ExecutionContext, Inject } from '@nestjs/common'
import { Reflector } from '@nestjs/core'
import { Job } from 'bullmq'

export const CurrentJob = createParamDecorator((data: unknown, ctx: ExecutionContext): Job | undefined => {
  const handler = ctx.getHandler()
  const reflector = new Reflector()
  return reflector.get<Job>('current_job', handler)
})
