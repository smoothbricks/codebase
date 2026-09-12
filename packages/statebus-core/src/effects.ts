import type { ComposedRuntime, EventHandle, StateReader, ValueCodec } from './composition.js';

export interface EffectPlan {
  readonly requestId: string | number;
}
export interface EffectExecution {
  readonly signal: AbortSignal;
}
export interface EffectOutcome<Plan, Outcome> {
  readonly plan: Plan;
  readonly outcome: Outcome;
}
export interface EffectSpecification<Command, Plan extends EffectPlan, Outcome, ResultEvent> {
  readonly command: EventHandle<Command>;
  readonly result: EventHandle<ResultEvent>;
  /** Undefined means no operation. Reads see the complete, successfully reduced wave. */
  readonly plan: (state: StateReader, command: Command) => Plan | undefined;
  /** An existing LMAO Result may flow through Outcome without an adapter or a second result hierarchy. */
  readonly decode: (plan: Plan, outcome: Outcome) => ResultEvent | undefined;
  readonly codec?: ValueCodec<EffectOutcome<Plan, Outcome>>;
}
export interface EffectDefinition<C, P extends EffectPlan, O, R> extends EffectSpecification<C, P, O, R> {
  readonly metadata: {
    readonly key: string;
    readonly owner: string;
    readonly schema?: string;
    readonly version?: number;
  };
}
export function defineEffect<Command, Plan extends EffectPlan, Outcome, ResultEvent>(
  definition: EffectSpecification<Command, Plan, Outcome, ResultEvent>,
): EffectDefinition<Command, Plan, Outcome, ResultEvent> {
  if (definition.command.ownerToken !== definition.result.ownerToken)
    throw new Error('An effect command and result must have the same owner.');
  return Object.freeze({
    ...definition,
    metadata: Object.freeze({
      key: `${definition.command.metadata.key}/${definition.result.metadata.key}`,
      owner: definition.command.metadata.owner,
      schema: definition.codec?.schema,
      version: definition.codec?.version,
    }),
  });
}
export interface EffectOperations<Plan, Outcome> {
  /** Inject the existing Op invocation, with its caller-owned tracing context already bound. */
  readonly execute: (plan: Plan, context: EffectExecution) => Promise<Outcome> | AsyncIterable<Outcome>;
  /** Unexpected rejections become the application's typed outcome, not unhandled promises. */
  readonly failure: (cause: unknown, plan: Plan) => Outcome;
  readonly capture?: (outcome: EffectOutcome<Plan, Outcome>) => void;
}
export interface EffectBinding<ID extends string | number = string | number> {
  /** Cancels client observation/work, NEVER promises to undo a server mutation. */
  cancel(requestId: ID): boolean;
  dispose(): void;
}
interface Job {
  readonly controller: AbortController;
  closeIterator?: () => Promise<void>;
  closed: boolean;
}
const bound = new WeakMap<ComposedRuntime, Map<string, object>>();
function isStream<T>(source: Promise<T> | AsyncIterable<T>): source is AsyncIterable<T> {
  return Symbol.asyncIterator in source;
}

export function publishEffectOutcome<C, P extends EffectPlan, O, R>(
  runtime: ComposedRuntime,
  definition: EffectDefinition<C, P, O, R>,
  captured: EffectOutcome<P, O>,
): void {
  const event = definition.decode(captured.plan, captured.outcome);
  if (event !== undefined && !runtime.disposed) runtime.publish(definition.result, event);
}

export function bindEffect<C, P extends EffectPlan, O, R>(
  runtime: ComposedRuntime,
  definition: EffectDefinition<C, P, O, R>,
  operations: EffectOperations<NoInfer<P>, NoInfer<O>>,
): EffectBinding<P['requestId']> {
  runtime.assertOwner(definition.command.ownerToken);
  runtime.assertOwner(definition.result.ownerToken);
  // No execution subscriptions, dependencies or iterators are instantiated during replay.
  if (runtime.mode === 'replay') return Object.freeze({ cancel: () => false, dispose: () => {} });
  let bindings = bound.get(runtime);
  if (!bindings) {
    bindings = new Map();
    bound.set(runtime, bindings);
  }
  if (bindings.has(definition.metadata.key)) throw new Error('An effect is already bound in this runtime.');
  bindings.set(definition.metadata.key, definition);
  const jobs = new Map<P['requestId'], Job>();
  let disposed = false;
  const current = (id: P['requestId'], job: Job) =>
    !disposed && !runtime.disposed && !job.closed && jobs.get(id) === job;
  function cancel(id: P['requestId']): boolean {
    const job = jobs.get(id);
    if (!job) return false;
    jobs.delete(id);
    job.closed = true;
    job.controller.abort();
    if (job.closeIterator) void job.closeIterator().catch((cause: unknown) => runtime.reportError(cause));
    return true;
  }
  function emit(plan: P, job: Job, outcome: O): void {
    if (!current(plan.requestId, job)) return;
    try {
      operations.capture?.({ plan, outcome });
    } catch (cause) {
      runtime.reportError(cause);
    }
    try {
      if (current(plan.requestId, job)) publishEffectOutcome(runtime, definition, { plan, outcome });
    } catch (cause) {
      runtime.reportError(cause);
    }
  }
  async function execute(plan: P, job: Job): Promise<void> {
    try {
      const source = operations.execute(plan, { signal: job.controller.signal });
      if (isStream(source)) {
        const iterator = source[Symbol.asyncIterator]();
        let returned = false;
        job.closeIterator = async () => {
          if (returned) return;
          returned = true;
          await iterator.return?.();
        };
        try {
          while (current(plan.requestId, job)) {
            const next = await iterator.next();
            if (next.done) break;
            emit(plan, job, next.value);
          }
        } finally {
          await job.closeIterator();
        }
      } else emit(plan, job, await source);
    } catch (cause) {
      if (current(plan.requestId, job)) {
        try {
          emit(plan, job, operations.failure(cause, plan));
        } catch (failure) {
          runtime.reportError(failure);
        }
      }
    } finally {
      if (jobs.get(plan.requestId) === job) jobs.delete(plan.requestId);
      job.closed = true;
    }
  }
  const stop = runtime.listen(definition.command, (command, admitted) => {
    if (!admitted || disposed) return;
    const plan = definition.plan(runtime.reader, command);
    if (!plan || jobs.has(plan.requestId)) return;
    const job: Job = { controller: new AbortController(), closed: false };
    jobs.set(plan.requestId, job);
    // execute handles both a synchronous throw and asynchronous/iterator rejection.
    void execute(plan, job);
  });
  const dispose = runtime.manage(() => {
    if (disposed) return;
    disposed = true;
    stop();
    for (const id of jobs.keys()) cancel(id);
    bindings.delete(definition.metadata.key);
  });
  return Object.freeze({ cancel, dispose });
}
