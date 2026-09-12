/** The shared, capacity-retaining dispatcher used by both public StateBus APIs. */
export class DispatchQueue<Event> {
  private count = 0;
  private pending = new Array<Event | undefined>(32).fill(undefined);
  private working = new Array<Event | undefined>(32).fill(undefined);
  private active = false;
  private disposed = false;

  constructor(
    private readonly wave: (events: readonly (Event | undefined)[], count: number) => void,
    private readonly schedule: () => void,
  ) {}

  get idle(): boolean {
    return !this.active && this.count === 0;
  }

  publish(event: Event): number {
    if (this.disposed) throw new Error('Cannot publish to a disposed StateBus.');
    const length = ++this.count;
    this.pending[length - 1] = event;
    if (!this.active) this.schedule();
    return length;
  }

  flush(): void {
    if (this.active || this.disposed) return;
    this.active = true;
    try {
      while (this.count > 0 && !this.disposed) {
        const events = this.pending;
        const count = this.count;
        this.count = 0;
        this.pending = this.working;
        this.working = events;
        try {
          this.wave(events, count);
        } finally {
          // Retain capacity, never payloads. A failed wave is never replayed.
          for (let index = 0; index < count; index++) events[index] = undefined;
        }
      }
    } finally {
      this.active = false;
      if (this.count > 0 && !this.disposed) this.schedule();
    }
  }

  dispose(): void {
    if (this.disposed) return;
    this.disposed = true;
    for (let index = 0; index < this.count; index++) this.pending[index] = undefined;
    this.count = 0;
  }
}

export interface DispatchScheduler {
  schedule(callback: () => void): void;
  cancel(callback: () => void): void;
}

/** Scheduling policy only: this object owns no bus or application state. */
export const microtaskScheduler: DispatchScheduler = Object.freeze({
  schedule: (callback: () => void) => queueMicrotask(callback),
  // Queued microtasks cannot be removed; the runtime's stable callback checks disposal.
  cancel: (_callback: () => void) => {},
});

export class ManualScheduler implements DispatchScheduler {
  private readonly callbacks = new Set<() => void>();
  schedule(callback: () => void): void {
    this.callbacks.add(callback);
  }
  cancel(callback: () => void): void {
    this.callbacks.delete(callback);
  }
  get pending(): number {
    return this.callbacks.size;
  }
  flush(): void {
    while (this.callbacks.size > 0) {
      const callback = this.callbacks.values().next().value;
      if (callback === undefined) return;
      this.callbacks.delete(callback);
      callback();
    }
  }
}
