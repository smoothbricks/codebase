import {
  type EventHandle, type LibraryEffect, type LibraryScope, type LibraryState, type RequiredBinding,
  type ResourceId, type SavedValue, type StateBusCodec,
} from './composition.js';

export interface EffectExecutionContext { readonly signal: AbortSignal }
/** Inject a closure that invokes the application's existing Op through its existing SpanContext. */
export type EffectOperation<Plan, Outcome> = (plan: Plan, context: EffectExecutionContext) => Outcome | Promise<Outcome> | AsyncIterable<Outcome>;
export interface CapturedOutcome { readonly key: string; readonly plan: SavedValue; readonly outcome: SavedValue }
export interface PlannedEffectOptions<Command, Plan, Outcome, Result> {
  readonly operation: RequiredBinding<EffectOperation<Plan, Outcome>>;
  readonly result: EventHandle<Result>;
  readonly plan: (state: LibraryState, command: Command) => Plan | undefined;
  readonly requestId: (plan: Plan) => ResourceId;
  readonly decode: (plan: Plan, outcome: Outcome) => Result;
  /** Classify only unexpected thrown/rejected operation failures. Expected failures are typed outcomes. */
  readonly failure: (plan: Plan, cause: unknown) => Outcome;
  readonly capture?: { readonly plan: StateBusCodec<Plan>; readonly outcome: StateBusCodec<Outcome> };
}
interface RunningOperation { readonly abort: AbortController; closeIterator?: () => void }

export class PlannedEffect<Command, Plan, Outcome, Result> implements LibraryEffect {
  readonly requires;
  readonly results;
  private readonly keys = new WeakMap<LibraryScope, string>();
  private readonly cancellations = new WeakMap<LibraryScope, (id: ResourceId) => void>();
  constructor(readonly name: string, readonly event: EventHandle<Command>, readonly options: PlannedEffectOptions<Command, Plan, Outcome, Result>) {
    this.requires = Object.freeze([options.operation]);
    this.results = Object.freeze([options.result]);
    Object.freeze(options);
    if (options.capture) Object.freeze(options.capture);
  }
  bind(scope: LibraryScope, key: string): void { this.keys.set(scope, key); }
  decodeCaptured(scope: LibraryScope, captured: CapturedOutcome): Result {
    const codecs = this.options.capture;
    if (!codecs || this.keys.get(scope) !== captured.key || captured.plan.schema !== codecs.plan.schema || captured.outcome.schema !== codecs.outcome.schema) throw new Error('Incompatible captured effect outcome.');
    return this.options.decode(codecs.plan.decode(captured.plan.value, captured.plan.version), codecs.outcome.decode(captured.outcome.value, captured.outcome.version));
  }
  /** Detaches client work; it does not undo or certify cancellation of a server mutation. */
  cancel(scope: LibraryScope, requestId: ResourceId): void { this.cancellations.get(scope)?.(requestId); }
  install(scope: LibraryScope): () => void {
    const running = new Map<ResourceId, RunningOperation>();
    let disposed = false;
    const operation = this.options.operation.value(scope);
    this.cancellations.set(scope, (id) => {
      const job = running.get(id);
      if (job) { job.abort.abort(); job.closeIterator?.(); running.delete(id); }
    });
    const publish = scope.publisher(this.options.result);
    const emit = (plan: Plan, outcome: Outcome, job: RunningOperation) => {
      if (disposed || job.abort.signal.aborted) return;
      const key = this.keys.get(scope);
      if (key === undefined) throw new Error('Unbound planned effect.');
      const capture = this.options.capture;
      if (capture) scope.runtime.captureOutcome(key, plan, outcome, capture.plan, capture.outcome);
      // Decoder bugs are programming errors, not transport/domain failures.
      scope.runtime.guard(() => publish(this.options.decode(plan, outcome)));
    };
    const execute = async (plan: Plan, id: ResourceId, job: RunningOperation): Promise<void> => {
      try {
        const result = operation(plan, { signal: job.abort.signal });
        if (isAsyncIterable<Outcome>(result)) {
          const iterator = result[Symbol.asyncIterator]();
          let closed = false;
          job.closeIterator = () => {
            if (closed) return;
            closed = true;
            if (iterator.return) void Promise.resolve().then(() => iterator.return?.()).catch((error: unknown) => scope.runtime.reportError(error));
          };
          while (!disposed && !job.abort.signal.aborted) {
            const next = await iterator.next();
            if (next.done) { closed = true; break; }
            emit(plan, next.value, job);
          }
        } else emit(plan, await result, job);
      } catch (cause) {
        if (!disposed && !job.abort.signal.aborted) {
          try { emit(plan, this.options.failure(plan, cause), job); }
          catch (error) { scope.runtime.reportError(error); }
        }
      } finally {
        job.closeIterator?.();
        if (running.get(id) === job) running.delete(id);
      }
    };
    const stop = scope.subscribe(this.event, (command, admitted) => {
      if (!admitted || disposed) return;
      const plan = this.options.plan(scope, command);
      if (plan === undefined) return;
      const id = this.options.requestId(plan);
      if (running.has(id)) return;
      const job: RunningOperation = { abort: new AbortController() };
      running.set(id, job);
      void execute(plan, id, job);
    });
    return () => {
      if (disposed) return;
      disposed = true; stop();
      for (const job of running.values()) { job.abort.abort(); job.closeIterator?.(); }
      running.clear(); this.cancellations.delete(scope);
    };
  }
}
function isAsyncIterable<T>(value: T | Promise<T> | AsyncIterable<T>): value is AsyncIterable<T> {
  return value !== null && typeof value === 'object' && Symbol.asyncIterator in value;
}
export function plannedEffect<Command, Plan, Outcome, Result>(name: string, event: EventHandle<Command>, options: PlannedEffectOptions<Command, Plan, Outcome, Result>): PlannedEffect<Command, Plan, Outcome, Result> {
  return new PlannedEffect(name, event, options);
}
