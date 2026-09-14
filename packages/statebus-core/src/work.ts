/** Aggregate execution admission failed before any queued job, observer or operation was created. */
export class RuntimeCapacityError extends Error {
  readonly code = 'runtime-capacity';
  constructor(readonly limit: number) {
    super(`StateBus runtime work capacity ${limit} exhausted.`);
  }
}

/**
 * Execution-boundary port, shared by every interpreter in one runtime.
 * A successful acquisition MUST be released exactly once after all owned work settles.
 * Cancellation is not settlement. Do not pair release with a screen/binding unmount.
 */
export interface WorkAdmission {
  readonly limit: number;
  readonly pending: number;
  readonly capacityError: RuntimeCapacityError;
  tryAcquire(): boolean;
  release(): void;
}

const SETTLED = Promise.resolve();

/** @internal One counter per runtime; no permit objects, job registry or per-acquisition callbacks. */
export class WorkBudget implements WorkAdmission {
  readonly capacityError: RuntimeCapacityError;
  private count = 0;
  private closed = false;
  private completion: Promise<void> | undefined;
  private resolveCompletion: (() => void) | undefined;

  constructor(readonly limit: number) {
    if (!Number.isSafeInteger(limit) || limit < 1)
      throw new RangeError('maxPendingWork must be a positive safe integer.');
    this.capacityError = Object.freeze(new RuntimeCapacityError(limit));
  }

  get pending(): number {
    return this.count;
  }

  tryAcquire(): boolean {
    if (this.closed || this.count === this.limit) return false;
    this.count++;
    return true;
  }

  release(): void {
    if (this.count === 0) throw new Error('StateBus work released without admission.');
    this.count--;
    if (this.count !== 0 || !this.resolveCompletion) return;
    const resolve = this.resolveCompletion;
    this.resolveCompletion = undefined;
    this.completion = undefined;
    resolve();
  }

  drain(): Promise<void> {
    if (this.count === 0) return SETTLED;
    this.completion ??= new Promise<void>((resolve) => {
      this.resolveCompletion = resolve;
    });
    return this.completion;
  }

  close(): void {
    // Keep outstanding reservations: late physical completion still owns their release.
    this.closed = true;
  }
}
