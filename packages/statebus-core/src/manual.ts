import { StateBus } from './api.js';

/**
 * Manual dispatch implementation of StateBus.
 * Events are queued but not automatically dispatched.
 * Call dispatchEvents() manually to process the queue.
 */
export class ManualStateBus extends StateBus {
  protected scheduleDispatch(): void {
    // No-op - user must call dispatchEvents() manually
  }
}
