/** Shared by ambient and composed APIs. Queue storage is reused; published payloads never are. */
export abstract class DispatchQueue<E> {
  private queuedEventCount = 0;
  private eventQueue = new Array<E | undefined>(32).fill(undefined);
  private dispatchingEventQueue = new Array<E | undefined>(32).fill(undefined);
  private dispatching = false;
  private stopped = false;

  protected abstract scheduleDispatch(): void;
  protected abstract dispatchWave(events: readonly (E | undefined)[], count: number): void;
  protected clearWave(): void {}

  dispatchEvents(): void {
    if (this.dispatching || this.stopped) return;
    this.dispatching = true;
    try {
      while (this.queuedEventCount > 0 && !this.stopped) {
        const events = this.eventQueue;
        const count = this.queuedEventCount;
        this.queuedEventCount = 0;
        this.eventQueue = this.dispatchingEventQueue;
        this.dispatchingEventQueue = events;
        try {
          this.dispatchWave(events, count);
        } finally {
          for (let index = 0; index < count; index += 1) events[index] = undefined;
          this.clearWave();
        }
      }
    } finally {
      this.dispatching = false;
      if (this.queuedEventCount > 0 && !this.stopped) this.scheduleDispatch();
    }
  }

  protected queue(event: E): number {
    if (this.stopped) return 0;
    const length = ++this.queuedEventCount;
    this.eventQueue[length - 1] = event;
    if (!this.dispatching) this.scheduleDispatch();
    return length;
  }

  /** Releases queued references; an already scheduled callback becomes harmless. */
  protected stopDispatch(): void {
    this.stopped = true;
    for (let index = 0; index < this.queuedEventCount; index += 1) this.eventQueue[index] = undefined;
    this.queuedEventCount = 0;
  }
}
