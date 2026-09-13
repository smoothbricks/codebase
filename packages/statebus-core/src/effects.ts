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
/** Explicit execution admission limit. The application's existing failure mapper owns the typed outcome. */
export class EffectCapacityError extends Error {
  readonly code = 'effect-capacity';
  constructor(readonly limit: number) {
    super(`Effect capacity ${limit} exhausted.`);
  }
}
interface EffectHooks<Plan, Outcome> {
  /** Active (including cancelled-but-unsettled) plus queued work. Defaults to 1024 per binding. */
  readonly maxPending?: number;
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
  controller: AbortController | undefined;
  readonly group: Group<P>;
  closeIterator: (() => Promise<void>) | undefined;
  closed: boolean;
  started: boolean;
  readonly sequence: number;
  previous: Job<P> | undefined;
  next: Job<P> | undefined;
}
interface Group<P extends EffectPlan> {
  active: number;
  first: Job<P> | undefined;
  last: Job<P> | undefined;
}
const bound = new WeakMap<ComposedRuntime, Map<string, object>>();
// Native abort() otherwise constructs an exception for every cancellation. The reason is
// immutable shared protocol data; request identity remains in the plan/result, not an Error.
const EFFECT_ABORTED = Object.freeze(new DOMException('StateBus operation cancelled.', 'AbortError'));
const SETTLED = Promise.resolve();
const INACTIVE_BINDING = Object.freeze({
  cancel: () => false,
  cancelKey: () => 0,
  drain: () => SETTLED,
  dispose: () => {},
  disposeAsync: () => SETTLED,
});
function asyncValues<T>(source: EffectStream<T>): source is AsyncIterable<T> {
  return source !== null && typeof source === 'object' && Symbol.asyncIterator in source;
}

export function publishEffectOutcome<C, P extends EffectPlan, O, R>(
  runtime: ComposedRuntime,
  definition: EffectDefinition<C, P, O, R>,
  captured: EffectOutcome<P, O>,
): void {
  publishDecoded(runtime, definition, captured.plan, captured.outcome);
}
function publishDecoded<C, P extends EffectPlan, O, R>(
  runtime: ComposedRuntime,
  definition: EffectDefinition<C, P, O, R>,
  plan: P,
  outcome: O,
): void {
  const event = definition.decode(plan, outcome);
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
  if (runtime.mode === 'replay') return INACTIVE_BINDING;
  const maxPending = operations.maxPending ?? 1024;
  if (!Number.isSafeInteger(maxPending) || maxPending < 1)
    throw new RangeError('maxPending must be a positive safe integer.');
  const capacityError = new EffectCapacityError(maxPending);
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
  // Intrusive per-key lists: no queue nodes, suffix copies, or scans of unrelated groups.
  let outstanding = 0;
  let running = 0;
  let sequence = 0;
  let drainPromise: Promise<void> | undefined;
  let resolveDrain: (() => void) | undefined;
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
  function unlink(job: Job<P>): void {
    const { group, previous, next } = job;
    if (previous) previous.next = next;
    else group.first = next;
    if (next) next.previous = previous;
    else group.last = previous;
    job.previous = undefined;
    job.next = undefined;
  }
  function releaseGroup(key: string | number, group: Group<P>): void {
    if (group.active === 0 && !group.first && groups.get(key) === group) groups.delete(key);
  }
  function cancelJob(job: Job<P>, reason: EffectCancellation): void {
    if (job.closed) return;
    job.closed = true;
    unlink(job);
    if (!job.started) outstanding--;
    if (jobs.get(job.plan.requestId) === job) jobs.delete(job.plan.requestId);
    releaseGroup(job.key, job.group);
    // Revoke membership before callbacks. A cancelled active task still owns its capacity
    // until it physically settles; abort cannot disguise unbounded non-cooperative work.
    job.controller?.abort(EFFECT_ABORTED);
    if (job.closeIterator) void job.closeIterator().catch((cause: unknown) => runtime.reportError(cause, 'cleanup'));
    cancellation(job.plan, reason);
  }
  function cancel(requestId: P['requestId']): boolean {
    const job = jobs.get(requestId);
    if (!job) return false;
    cancelJob(job, 'cancelled');
    return true;
  }
  function cancelGroup(group: Group<P>, through: number, reason: EffectCancellation): number {
    let count = 0;
    // Callbacks may cancel other jobs or append new work; never cancel beyond this call's frontier.
    while (group.first && group.first.sequence <= through) {
      cancelJob(group.first, reason);
      count++;
    }
    return count;
  }
  function cancelKey(key: OperationKey<P>): number {
    const group = groups.get(key);
    return group ? cancelGroup(group, sequence, 'cancelled') : 0;
  }
  function emit(job: Job<P>, outcome: O): void {
    if (!current(job)) return;
    try {
      operations.capture?.({ plan: job.plan, outcome });
    } catch (cause) {
      runtime.reportError(cause);
    }
    try {
      if (current(job)) publishDecoded(runtime, definition, job.plan, outcome);
    } catch (cause) {
      runtime.reportError(cause, 'decoder');
    }
  }
  function pump(key: string | number, group: Group<P>): void {
    if (disposed || runtime.disposed) return;
    if (policy === 'serialize' && group.active === 0 && group.first) start(group.first);
    releaseGroup(key, group);
  }
  function settled(): void {
    if (running !== 0 || !resolveDrain) return;
    const resolve = resolveDrain;
    resolveDrain = undefined;
    drainPromise = undefined;
    resolve();
  }
  async function execute(job: Job<P>, signal: AbortSignal): Promise<void> {
    try {
      if ('stream' in operations) {
        const source = operations.stream(job.plan, { signal });
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
      } else emit(job, await operations.execute(job.plan, { signal }));
    } catch (cause) {
      // An operation can throw before reaching its first await. Match Promise rejection
      // timing so failure outcomes never enter the caller's command flush synchronously.
      await SETTLED;
      if (current(job)) {
        try {
          emit(job, operations.failure(cause, job.plan));
        } catch (failure) {
          runtime.reportError(failure);
        }
      }
    } finally {
      if (!job.closed) {
        unlink(job);
        jobs.delete(job.plan.requestId);
        job.closed = true;
      }
      outstanding--;
      job.group.active--;
      running--;
      try {
        pump(job.key, job.group);
      } finally {
        settled();
      }
    }
  }
  function start(job: Job<P>): void {
    if (!current(job)) return;
    job.started = true;
    job.group.active++;
    running++;
    job.controller = new AbortController();
    runtime.trackExecution(execute(job, job.controller.signal));
  }
  function overloaded(plan: P): void {
    // Refusal is an operational outcome, not an executed job or an unhandled exception.
    try {
      const outcome = operations.failure(capacityError, plan);
      if (disposed || runtime.disposed) return;
      try {
        operations.capture?.({ plan, outcome });
      } catch (cause) {
        runtime.reportError(cause);
      }
      if (!disposed && !runtime.disposed) publishDecoded(runtime, definition, plan, outcome);
    } catch (cause) {
      runtime.reportError(cause);
    }
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
    if (!plan || disposed || runtime.disposed) return;
    if (runtime.waveNumber !== seenWave) {
      seen.clear();
      seenWave = runtime.waveNumber;
    }
    if (seen.has(plan.requestId) || jobs.has(plan.requestId)) return;
    seen.add(plan.requestId);
    const key = plan.operationKey ?? plan.requestId;
    let group = groups.get(key);
    if (policy === 'drop-duplicate' && group && group.active > 0) {
      cancellation(plan, 'duplicate');
      return;
    }
    if (outstanding === maxPending) {
      overloaded(plan);
      return;
    }
    if (!group) {
      group = { active: 0, first: undefined, last: undefined };
      groups.set(key, group);
    }
    const job: Job<P> = {
      plan,
      key,
      group,
      controller: undefined,
      closeIterator: undefined,
      closed: false,
      started: false,
      sequence: ++sequence,
      previous: group.last,
      next: undefined,
    };
    if (group.last) group.last.next = job;
    else group.first = job;
    group.last = job;
    jobs.set(plan.requestId, job);
    outstanding++;
    if (policy === 'latest-wins') cancelGroup(group, job.sequence - 1, 'superseded');
    // Capture is injected code and can dispose/cancel synchronously. Reserve the request
    // first, then recheck it before executing or retaining it in the serialized queue.
    if (!current(job)) return;
    try {
      operations.captureInstruction?.(plan);
    } catch (cause) {
      runtime.reportError(cause);
    }
    if (current(job) && (policy !== 'serialize' || group.active === 0)) start(job);
  });
  const dispose = runtime.manage(() => {
    if (disposed) return;
    disposed = true;
    stop();
    releaseRequirement();
    for (const job of jobs.values()) cancelJob(job, 'cancelled');
    jobs.clear();
    groups.clear();
    seen.clear();
    bindings.delete(definition.metadata.key);
  });
  function drain(): Promise<void> {
    if (running === 0) return SETTLED;
    // Only callers that actually wait allocate a completion promise. All waiters share it.
    drainPromise ??= new Promise<void>((resolve) => {
      resolveDrain = resolve;
    });
    return drainPromise;
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
