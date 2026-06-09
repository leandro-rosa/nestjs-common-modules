export const HOLD_IT_QUEUE_NAMES = Symbol('HOLD_IT_QUEUE_NAMES')

export interface HoldItModuleOptions {
    queues: string[]
}