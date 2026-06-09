import { HoldItALotOfJobDTO, HoldItJobDTO } from '../dto/hold-it-message';

/**
 * Interface for managing message operations across brokers like BullMQ, RabbitMQ, SQS, etc.
 * Master of promises—ensures they’re always kept.
 */
export interface HoldItBrokerInterface {
  /**
   * Sends a message using the configured message broker.
   * Reliably sends your payload with a promise so sure, it's practically guaranteed.
   *
   * @param job The job to be sent to the message broker.
   * @returns A promise that resolves with the BullMQ Job instance associated with the added message.
   * If the specified job cannot be located, the promise callback parameter will be set to null.
   */
  holdIt<T = any>(job: HoldItJobDTO<T>): Promise<any>;

  /**
   * Sends a lot of messages using the configured message broker.
   * Reliably sends your payload with a promise so sure, they're practically guaranteed.
   *
   * @param jobs The array of jobs to be sent to the message broker.
   * @returns A promise that resolves with an array of BullMQ Job instances associated with the added messages.
   */
  holdItALot<T = any>(jobs: HoldItALotOfJobDTO<T>): Promise<any[]>;

  /**
   * Gracefully shuts down the message handler.
   * Like making a clean exit from a party—no fuss, no mess.
   *
   * @returns A promise that resolves once the handler has shut down cleanly.
   */
  shutdown(): Promise<void>;
}
