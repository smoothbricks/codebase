import { StateBus } from './api.js';

/** Batch application commands in a microtask without waiting for a browser paint. */
export class MicrotaskStateBus extends StateBus {
  private dispatchScheduled = false;

  protected scheduleDispatch(): void {
    if (this.dispatchScheduled) return;
    this.dispatchScheduled = true;
    queueMicrotask(() => {
      this.dispatchScheduled = false;
      this.dispatchEvents();
    });
  }
}
