import { CaptureError, captureValue } from './capture.js';
import { decodeCodec, supportsCodec } from './codec.js';
import type { ComposedRuntime, EventHandle, StateReader, SupportClassification, ValueCodec } from './composition.js';
import type { RecordedEffectCapture, RecordedEffectOutcome } from './recording.js';
import type { SupportDecision, SupportPolicy } from './support-policy.js';

export interface EffectPlan {
  readonly requestId: string | number;
  readonly operationKey?: string | number;
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
  readonly instructionCodec?: ValueCodec<Plan>;
  readonly classifyInstruction?: (plan: Plan) => SupportClassification;
  readonly supportInstruction?: SupportPolicy<Plan>;
  readonly policy?: EffectPolicy;
  readonly cancelled?: (plan: Plan, reason: EffectCancellation) => ResultEvent | undefined;
  readonly codec?: ValueCodec<EffectOutcome<Plan, Outcome>>;
  readonly classify?: (value: EffectOutcome<Plan, Outcome>) => SupportClassification;
  readonly support?: SupportPolicy<EffectOutcome<Plan, Outcome>>;
}
export interface EffectCodecDescriptor {
  readonly ownerToken: object;
  readonly metadata: {
    readonly key: string;
    readonly owner: string;
    readonly schema?: string;
    readonly version?: number;
    readonly instructionSchema?: string;
    readonly instructionVersion?: number;
  };
  acceptsCodec(kind: 'instruction' | 'outcome', schema: string | undefined, version: number | undefined): boolean;
  migrateCapture(capture: RecordedEffectCapture): RecordedEffectCapture;
  supportCapture(capture: RecordedEffectCapture): SupportDecision | undefined;
}
export interface EffectDefinition<C, P extends EffectPlan, O, R>
  extends EffectSpecification<C, P, O, R>,
    EffectCodecDescriptor {
  readonly metadata: {
    readonly key: string;
    readonly owner: string;
    readonly schema?: string;
    readonly version?: number;
    readonly instructionSchema?: string;
    readonly instructionVersion?: number;
  };
}
export function defineEffect<Command, Plan extends EffectPlan, Outcome, ResultEvent>(
  definition: EffectSpecification<Command, Plan, Outcome, ResultEvent>,
): EffectDefinition<Command, Plan, Outcome, ResultEvent> {
  if (definition.command.ownerToken !== definition.result.ownerToken)
    throw new Error('An effect command and result must have the same owner.');
  const ownerToken = definition.command.ownerToken;
  const key = `${definition.command.metadata.key}/${definition.result.metadata.key}`;
  function decoded(outcome: RecordedEffectOutcome): EffectOutcome<Plan, Outcome> {
    try {
      if (outcome.effect !== key) throw new Error('Foreign effect codec.');
      return decodeCodec(definition.codec, outcome);
    } catch (cause) {
      throw new CaptureError(
        {
          code: 'schema',
          boundary: 'effect outcome',
          owner: definition.command.metadata.owner,
          declaration: definition.command.metadata.name,
          schema: outcome.schema,
          fromVersion: outcome.version,
          toVersion: definition.codec?.version,
        },
        { cause },
      );
    }
  }
  return Object.freeze({
    ...definition,
    ownerToken,
    acceptsCodec: (kind: 'instruction' | 'outcome', schema: string | undefined, version: number | undefined) =>
      kind === 'instruction'
        ? supportsCodec(definition.instructionCodec, { schema, version })
        : supportsCodec(definition.codec, { schema, version }),
    migrateCapture(outcome: RecordedEffectCapture): RecordedEffectCapture {
      if (outcome.kind === 'instruction') {
        const codec = definition.instructionCodec;
        if (!codec || outcome.effect !== key)
          throw new CaptureError({
            code: 'schema',
            boundary: 'instruction codec',
            owner: definition.command.metadata.owner,
            declaration: key,
          });
        try {
          const plan = decodeCodec(codec, outcome);
          return Object.freeze({
            ...outcome,
            schema: codec.schema,
            version: codec.version,
            value: captureValue(codec.encode(plan)).value,
            classification: definition.classifyInstruction?.(plan) ?? 'unclassified',
          });
        } catch (cause) {
          throw new CaptureError(
            {
              code: 'schema',
              boundary: 'instruction codec',
              owner: definition.command.metadata.owner,
              declaration: key,
              schema: outcome.schema,
              fromVersion: outcome.version,
              toVersion: codec.version,
            },
            { cause },
          );
        }
      }
      const value = decoded(outcome);
      const codec = definition.codec;
      if (!codec) throw new Error('Missing effect codec.');
      return Object.freeze({
        kind: 'outcome',
        effect: key,
        schema: codec.schema,
        version: codec.version,
        value: captureValue(codec.encode(value)).value,
        classification: definition.classify?.(value) ?? 'unclassified',
      });
    },
    supportCapture: (outcome: RecordedEffectCapture) => {
      if (outcome.kind === 'instruction') {
        if (outcome.effect !== key) throw new Error('Foreign instruction.');
        return definition.supportInstruction?.(decodeCodec(definition.instructionCodec, outcome));
      }
      return definition.support?.(decoded(outcome));
    },
    metadata: Object.freeze({
      key,
      owner: definition.command.metadata.owner,
      schema: definition.codec?.schema,
      version: definition.codec?.version,
      instructionSchema: definition.instructionCodec?.schema,
      instructionVersion: definition.instructionCodec?.version,
    }),
  });
}
export type EffectPolicy = 'parallel' | 'serialize' | 'latest-wins' | 'drop-duplicate';
export type EffectCancellation = 'cancelled' | 'superseded' | 'duplicate';
export type EffectSource<T> = T | Promise<T>;
export type EffectStream<T> = Iterable<T> | AsyncIterable<T>;
interface EffectHooks<Plan, Outcome> {
  readonly failure: (cause: unknown, plan: Plan) => Outcome;
  readonly capture?: (outcome: EffectOutcome<Plan, Outcome>) => void;
  readonly captureInstruction?: (plan: Plan) => void;
}
/** Explicit value/stream operations avoid mistaking an array-shaped Outcome for a stream of outcomes. */
export type EffectOperations<Plan, Outcome> = EffectHooks<Plan, Outcome> &
  (
    | { readonly execute: (plan: Plan, context: EffectExecution) => EffectSource<Outcome> }
    | { readonly stream: (plan: Plan, context: EffectExecution) => EffectStream<Outcome> }
  );
export interface EffectBinding<ID extends string | number = string | number, Key extends string | number = ID> {
  /** Stops client observation/work, not an already committed server mutation. */
  cancel(requestId: ID): boolean;
  cancelKey(key: Key): number;
  drain(): Promise<void>;
  dispose(): void;
  disposeAsync(): Promise<void>;
}
interface Job<P extends EffectPlan> {
  readonly plan: P;
  readonly key: string | number;
  readonly controller: AbortController;
  readonly group: Group<P>;
  closeIterator: (() => Promise<void>) | undefined;
  closed: boolean;
  started: boolean;
  queueIndex: number;
}
interface Group<P extends EffectPlan> {
  active: number;
  next: number;
  queue: (Job<P> | undefined)[];
}
const bound = new WeakMap<ComposedRuntime, Map<string, object>>();
function asyncValues<T>(source: EffectStream<T>): source is AsyncIterable<T> {
  return source !== null && typeof source === 'object' && Symbol.asyncIterator in source;
}

export function publishEffectOutcome<C, P extends EffectPlan, O, R>(
  runtime: ComposedRuntime,
  definition: EffectDefinition<C, P, O, R>,
  captured: EffectOutcome<P, O>,
): void {
  const event = definition.decode(captured.plan, captured.outcome);
  if (event !== undefined && !runtime.disposed) runtime.publish(definition.result, event);
}

type OperationKey<P extends EffectPlan> = 'operationKey' extends keyof P
  ? Exclude<P['operationKey'], undefined> | (undefined extends P['operationKey'] ? P['requestId'] : never)
  : P['requestId'];

export function bindEffect<C, P extends EffectPlan, O, R>(
  runtime: ComposedRuntime,
  definition: EffectDefinition<C, P, O, R>,
  operations: EffectOperations<NoInfer<P>, NoInfer<O>>,
): EffectBinding<P['requestId'], OperationKey<P>> {
  runtime.assertOwner(definition.command.ownerToken);
  runtime.assertOwner(definition.result.ownerToken);
  // No interpreter subscription, request, promise, or iterator is installed during replay.
  if (runtime.mode === 'replay')
    return Object.freeze({
      cancel: () => false,
      cancelKey: () => 0,
      drain: async () => {},
      dispose: () => {},
      disposeAsync: async () => {},
    });
  let bindings = bound.get(runtime);
  if (!bindings) {
    bindings = new Map();
    bound.set(runtime, bindings);
  }
  if (bindings.has(definition.metadata.key)) throw new Error('An effect is already bound in this runtime.');
  bindings.set(definition.metadata.key, definition);
  const releaseRequirement = runtime.provideEffect(definition);
  const policy = definition.policy ?? 'parallel';
  const jobs = new Map<P['requestId'], Job<P>>();
  const groups = new Map<string | number, Group<P>>();
  const parallelGroup: Group<P> = { active: 0, next: 0, queue: [] };
  const pending = new Set<Promise<void>>();
  const seen = new Set<P['requestId']>();
  let seenWave = -1;
  let disposed = false;
  const current = (job: Job<P>) =>
    !disposed && !runtime.disposed && !job.closed && jobs.get(job.plan.requestId) === job;

  function cancellation(plan: P, reason: EffectCancellation): void {
    if (disposed || runtime.disposed || !definition.cancelled) return;
    try {
      const event = definition.cancelled(plan, reason);
      if (event !== undefined) runtime.publish(definition.result, event);
    } catch (cause) {
      runtime.reportError(cause, 'decoder');
    }
  }
  function cancelJob(job: Job<P>, reason: EffectCancellation): void {
    if (job.closed) return;
    job.closed = true;
    if (!job.started && job.queueIndex >= 0) {
      job.group.queue[job.queueIndex] = undefined;
      job.queueIndex = -1;
      trim(job.group);
    }
    if (jobs.get(job.plan.requestId) === job) jobs.delete(job.plan.requestId);
    job.controller.abort();
    if (job.closeIterator) void job.closeIterator().catch((cause: unknown) => runtime.reportError(cause, 'cleanup'));
    cancellation(job.plan, reason);
  }
  function cancel(requestId: P['requestId']): boolean {
    const job = jobs.get(requestId);
    if (!job) return false;
    cancelJob(job, 'cancelled');
    return true;
  }
  function cancelKey(key: OperationKey<P>): number {
    let count = 0;
    for (const job of jobs.values())
      if (job.key === key) {
        cancelJob(job, 'cancelled');
        count++;
      }
    return count;
  }
  function emit(job: Job<P>, outcome: O): void {
    if (!current(job)) return;
    try {
      operations.capture?.({ plan: job.plan, outcome });
    } catch (cause) {
      runtime.reportError(cause);
    }
    try {
      if (current(job)) publishEffectOutcome(runtime, definition, { plan: job.plan, outcome });
    } catch (cause) {
      runtime.reportError(cause, 'decoder');
    }
  }
  function trim(group: Group<P>): void {
    while (group.next < group.queue.length && group.queue[group.next] === undefined) group.next++;
    if (group.next === group.queue.length) {
      group.queue.length = 0;
      group.next = 0;
    } else if (group.next >= 64 && group.next * 2 >= group.queue.length) {
      group.queue = group.queue.slice(group.next);
      group.next = 0;
      for (let index = 0; index < group.queue.length; index++) {
        const job = group.queue[index];
        if (job) job.queueIndex = index;
      }
    }
  }
  function pump(key: string | number, group: Group<P>): void {
    if (disposed || runtime.disposed) return;
    while (group.active === 0 && group.next < group.queue.length) {
      const job = group.queue[group.next];
      group.queue[group.next++] = undefined;
      if (job) job.queueIndex = -1;
      if (job && !job.closed) start(job);
    }
    trim(group);
    if (group.active === 0 && group.queue.length === 0 && groups.get(key) === group) groups.delete(key);
  }
  async function execute(job: Job<P>): Promise<void> {
    try {
      if ('stream' in operations) {
        const source = operations.stream(job.plan, { signal: job.controller.signal });
        const iterator = asyncValues(source) ? source[Symbol.asyncIterator]() : source[Symbol.iterator]();
        let closed: Promise<void> | undefined;
        job.closeIterator = () => {
          if (!closed)
            closed = (async () => {
              await iterator.return?.();
            })();
          return closed;
        };
        try {
          while (current(job)) {
            const next = await iterator.next();
            if (next.done) break;
            emit(job, next.value);
          }
        } finally {
          await job.closeIterator();
        }
      } else emit(job, await operations.execute(job.plan, { signal: job.controller.signal }));
    } catch (cause) {
      if (current(job)) {
        try {
          emit(job, operations.failure(cause, job.plan));
        } catch (failure) {
          runtime.reportError(failure);
        }
      }
    } finally {
      // Reserve the entire notification wave even for direct outcomes and synchronous throws.
      await Promise.resolve();
      if (jobs.get(job.plan.requestId) === job) jobs.delete(job.plan.requestId);
      job.closed = true;
      job.group.active--;
      pump(job.key, job.group);
    }
  }
  function start(job: Job<P>): void {
    if (!current(job)) return;
    job.started = true;
    job.group.active++;
    const task = execute(job);
    pending.add(task);
    runtime.trackExecution(task);
    void task.then(
      () => {
        pending.delete(task);
      },
      (cause: unknown) => {
        pending.delete(task);
        runtime.reportError(cause, 'cleanup');
      },
    );
  }
  const stop = runtime.listen(definition.command, (command, admitted) => {
    if (!admitted || disposed) return;
    let plan: P | undefined;
    try {
      plan = definition.plan(runtime.reader, command);
    } catch (cause) {
      runtime.reportError(cause, 'planner');
      return;
    }
    if (!plan) return;
    if (runtime.waveNumber !== seenWave) {
      seen.clear();
      seenWave = runtime.waveNumber;
    }
    if (seen.has(plan.requestId) || jobs.has(plan.requestId)) return;
    seen.add(plan.requestId);
    try {
      operations.captureInstruction?.(plan);
    } catch (cause) {
      runtime.reportError(cause);
    }
    const key = plan.operationKey ?? plan.requestId;
    let group = policy === 'parallel' ? parallelGroup : groups.get(key);
    if (!group) {
      group = { active: 0, next: 0, queue: [] };
      groups.set(key, group);
    }
    if (policy === 'drop-duplicate' && group.active > 0) {
      cancellation(plan, 'duplicate');
      return;
    }
    if (policy === 'latest-wins')
      for (const previous of jobs.values()) if (previous.key === key) cancelJob(previous, 'superseded');
    const job: Job<P> = {
      plan,
      key,
      group,
      controller: new AbortController(),
      closeIterator: undefined,
      closed: false,
      started: false,
      queueIndex: -1,
    };
    jobs.set(plan.requestId, job);
    if (policy === 'serialize' && group.active > 0) {
      job.queueIndex = group.queue.length;
      group.queue.push(job);
    } else start(job);
  });
  const dispose = runtime.manage(() => {
    if (disposed) return;
    disposed = true;
    stop();
    releaseRequirement();
    for (const job of jobs.values()) cancelJob(job, 'cancelled');
    for (const group of groups.values()) {
      group.queue.length = 0;
      group.next = 0;
    }
    jobs.clear();
    groups.clear();
    seen.clear();
    bindings.delete(definition.metadata.key);
  });
  async function drain(): Promise<void> {
    while (pending.size > 0) await Promise.allSettled([...pending]);
  }
  return Object.freeze({
    cancel,
    cancelKey,
    drain,
    dispose,
    disposeAsync: async () => {
      dispose();
      await drain();
    },
  });
}
