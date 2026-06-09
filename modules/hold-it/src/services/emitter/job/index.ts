import { Injectable } from '@nestjs/common'
import { EventEmitter2 } from '@nestjs/event-emitter'
import { HoldItJobEventsDTO } from '../../../dto/events'
import { HoldItSimpleJobDataDTO } from '../../../dto/hold-it-message'

@Injectable()
export class HoldItJobEmitterService {
  public static JOB_EVENT_CODE = 'holdItJobEvent'

  constructor(private eventEmitter: EventEmitter2) {}

  /**
   * Emits an event indicating that a specific job has a job event was updated
   *
   * @param jobEvent Job event
   */
  emitHoldItJobEvent<T = HoldItSimpleJobDataDTO>(jobEvent: HoldItJobEventsDTO<T>): void {
    this.eventEmitter.emit(HoldItJobEmitterService.JOB_EVENT_CODE, jobEvent)
  }

  /**
   * Registers a callback to be executed when a job in a queue has been processed.
   * @param callback A function to be executed when a job is processed. Receives the name of the processed queue and the ID of the processed job.
   */
  onHoldItJobEvent<T = HoldItSimpleJobDataDTO>(callback: (jobEvent: HoldItJobEventsDTO<T>) => void): void {
    this.eventEmitter.on(HoldItJobEmitterService.JOB_EVENT_CODE, callback)
  }
}
