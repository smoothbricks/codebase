/// <reference types="node" />

import { createRequire } from 'node:module';
import typia from 'typia';
import type {
  NativeCoordinatorOperations,
  NativeEvents,
  NativeHostOperations,
  NativeJobAttachmentCapabilities,
  NativeJobHandleCapabilities,
  NativeJobHandleOperations,
  NativeProjectOperations,
  NativeSccacheHostOperations,
  NativeWorkspaceHandleOperations,
  NativeWorkspaceRefOperations,
} from './native.generated.js';
import { addonDirectory } from './platform.js';
import type { CoordinatorEndpoint } from './types.js';

// Every controller operation's method is declared in `native.generated.ts`, from the operation
// table the addon's adapters are generated from, and each handle extends exactly the operations
// its authority admits. What follows is only what no operation declares: each handle's identity,
// read from the call that minted it.

export interface NativeProjectHandle extends NativeProjectOperations {
  readonly repoId: string;
  readonly gitRoot: string;
}

export interface NativeWorkspaceRefHandle extends NativeWorkspaceRefOperations {
  readonly name: string;
  readonly mountPath: string;
  /** The reference as a land or rebase target, pinned to the incarnation it was resolved at. */
  readonly targetJson: string;
}

export type NativeCoordinatorHandle = NativeCoordinatorOperations;

/** A host connection's adapters and the close that ends it. */
export type NativeHostHandle = NativeHostOperations & { close(): Promise<void> };

/** A sccache host connection's adapters and the close that ends it. */
export type NativeSccacheHostHandle = NativeSccacheHostOperations & { close(): Promise<void> };

export interface NativeWorkspaceHandle extends NativeWorkspaceHandleOperations {
  readonly workspaceIncarnation: string;
  readonly name: string;
  readonly mountPath: string;
}

export interface NativeJobHandle extends NativeJobHandleOperations, NativeJobHandleCapabilities {
  readonly id: number;
}

/**
 * A stream-lane call's events as an async iterator. Unlike an async generator, whose `return`
 * waits behind a `next` in flight, `return` here closes the call at once.
 */
export interface EventIterator<T> {
  next(): Promise<IteratorResult<T, undefined>>;
  return(): Promise<IteratorResult<T, undefined>>;
  [Symbol.asyncIterator](): EventIterator<T>;
}

/**
 * One observation the iterator shell reports to its core: a consumer's `next`/`return`, the
 * open's result, one event (or its demand's failure), one parse result, or the close's result.
 * The core owns every decision those observations can change; the shell only performs the
 * effects it returns and reports every real I/O outcome back, failures included.
 */
export type EventIteratorInput<T> =
  | { readonly kind: 'next' }
  | { readonly kind: 'return' }
  | { readonly kind: 'open-ok' }
  | { readonly kind: 'open-error'; readonly error: unknown }
  | { readonly kind: 'event'; readonly event: string | null }
  | { readonly kind: 'event-error'; readonly error: unknown }
  | { readonly kind: 'parsed'; readonly value: T }
  | { readonly kind: 'parse-error'; readonly error: unknown }
  | { readonly kind: 'close-ok' }
  | { readonly kind: 'close-error'; readonly error: unknown };

/** One effect the core orders, performed by the shell in the order returned. */
export type EventIteratorEffect<T> =
  | { readonly kind: 'open' }
  | { readonly kind: 'demand' }
  | { readonly kind: 'parse'; readonly event: string }
  | { readonly kind: 'close' }
  | { readonly kind: 'resolve-next'; readonly result: IteratorResult<T, undefined> }
  | { readonly kind: 'reject-next'; readonly error: unknown }
  | { readonly kind: 'resolve-return' }
  | { readonly kind: 'reject-return'; readonly error: unknown };

type EventIteratorPhase = 'idle' | 'opening' | 'open' | 'awaiting-event' | 'parsing' | 'open-failed' | 'ended' | 'done';

/** The opened call's close lifecycle, orthogonal to the demand phase: exactly one close is sent.
 */
type EventIteratorClose = 'unopened' | 'available' | 'closing' | 'closed' | 'close-failed';

// The content-identical hot effects and the done result are frozen singletons: a step on the
// warmed path (next, demand, close, settle) allocates no effect object or array. Steps that
// carry a value or an error still allocate their one result.
const DONE: IteratorResult<never, undefined> = Object.freeze({ done: true, value: undefined });
const OPEN_EFFECT: { readonly kind: 'open' } = Object.freeze({ kind: 'open' });
const DEMAND_EFFECT: { readonly kind: 'demand' } = Object.freeze({ kind: 'demand' });
const CLOSE_EFFECT: { readonly kind: 'close' } = Object.freeze({ kind: 'close' });
const RESOLVE_RETURN_EFFECT: { readonly kind: 'resolve-return' } = Object.freeze({ kind: 'resolve-return' });
const RESOLVE_DONE_EFFECT: {
  readonly kind: 'resolve-next';
  readonly result: IteratorResult<never, undefined>;
} = Object.freeze({ kind: 'resolve-next', result: DONE });
const NO_EFFECTS: readonly never[] = Object.freeze([]);
const OPEN_EFFECTS: readonly [typeof OPEN_EFFECT] = Object.freeze([OPEN_EFFECT]);
const DEMAND_EFFECTS: readonly [typeof DEMAND_EFFECT] = Object.freeze([DEMAND_EFFECT]);
const CLOSE_EFFECTS: readonly [typeof CLOSE_EFFECT] = Object.freeze([CLOSE_EFFECT]);
const RESOLVE_DONE_EFFECTS: readonly [typeof RESOLVE_DONE_EFFECT] = Object.freeze([RESOLVE_DONE_EFFECT]);
const RESOLVE_RETURN_EFFECTS: readonly [typeof RESOLVE_RETURN_EFFECT] = Object.freeze([RESOLVE_RETURN_EFFECT]);

/**
 * The pure transition core of one `eventIterator`: lazy open on the first `next`, one demand
 * per `next`, `return` closing the call without waiting behind a pending demand (and opening
 * none that was never opened), exactly one close per opened call, and a parse failure closing
 * the call before its `next` rejects. A close settles only through its reported result: a
 * failed close rejects the pending `return` (and is what a `next` left rejecting by a parse
 * failure hears, as the awaited close's throw replaced that failure before). A demand failure
 * rejects only its own `next` and poisons nothing: the `NativeEvents` contract makes each
 * `next` its own demand, so a later explicit `next` demands again on the still-open call.
 * I/O and holds no promises: `step` consumes one input and returns the effects that
 * observation causes, so the whole matrix is table-testable without a native call.
 * `resolve-next`/`reject-next` settle the oldest pending `next`; `resolve-return`/
 * `reject-return` settle every pending `return`.
 */
export class EventIteratorCore<T> {
  private phase: EventIteratorPhase = 'idle';
  private closeState: EventIteratorClose = 'unopened';
  private returned = false;
  private nextsPending = 0;
  private returnPending = false;
  private openError: unknown = undefined;
  private pendingRejection: { readonly error: unknown } | undefined = undefined;
  private closeError: unknown = undefined;

  step(input: EventIteratorInput<T>): readonly EventIteratorEffect<T>[] {
    switch (input.kind) {
      case 'next': {
        if (this.returned || this.phase === 'ended' || this.phase === 'done') {
          return RESOLVE_DONE_EFFECTS;
        }
        if (this.phase === 'open-failed') {
          return [{ kind: 'reject-next', error: this.openError }];
        }
        this.nextsPending++;
        switch (this.phase) {
          case 'idle':
            this.phase = 'opening';
            return OPEN_EFFECTS;
          case 'open':
            this.phase = 'awaiting-event';
            return DEMAND_EFFECTS;
          default:
            // Opening, awaiting an event or parsing: this demand queues behind the work
            // already in flight and is served by its completion.
            return NO_EFFECTS;
        }
      }
      case 'return': {
        if (this.closeState === 'closed') {
          return RESOLVE_RETURN_EFFECTS;
        }
        if (this.closeState === 'close-failed') {
          return [{ kind: 'reject-return', error: this.closeError }];
        }
        this.returned = true;
        this.returnPending = true;
        if (this.closeState === 'closing') {
          // The one close is already in flight; its result settles this return too.
          return NO_EFFECTS;
        }
        switch (this.phase) {
          case 'idle':
          case 'open-failed':
          case 'done':
            // A call never opened, or one that failed to open, has no close to send.
            this.phase = 'done';
            this.returnPending = false;
            return RESOLVE_RETURN_EFFECTS;
          case 'opening':
          case 'parsing':
            // The result already in flight decides the close this return starts.
            return NO_EFFECTS;
          default:
            // Open, awaiting an event or ended: close at once, never behind a
            // waiting demand. The close's result settles the pending calls.
            return this.startClose();
        }
      }
      case 'open-ok': {
        if (this.phase !== 'opening') {
          return NO_EFFECTS;
        }
        this.closeState = 'available';
        if (this.returned) {
          this.phase = 'done';
          return this.startClose();
        }
        if (this.nextsPending > 0) {
          this.phase = 'awaiting-event';
          return DEMAND_EFFECTS;
        }
        this.phase = 'open';
        return NO_EFFECTS;
      }
      case 'open-error': {
        if (this.phase !== 'opening') {
          return NO_EFFECTS;
        }
        this.openError = input.error;
        const effects: EventIteratorEffect<T>[] = [];
        while (this.nextsPending > 0) {
          this.nextsPending--;
          effects.push({ kind: 'reject-next', error: input.error });
        }
        this.phase = this.returned ? 'done' : 'open-failed';
        if (this.returnPending) {
          this.returnPending = false;
          effects.push(RESOLVE_RETURN_EFFECT);
        }
        return effects;
      }
      case 'event': {
        if (this.phase !== 'awaiting-event') {
          return NO_EFFECTS;
        }
        if (input.event === null) {
          this.phase = this.returned || this.closeState === 'closing' ? 'done' : 'ended';
          return this.resolveAllNextsDone([]);
        }
        if (this.returned || this.closeState === 'closing') {
          this.phase = 'done';
          return this.resolveAllNextsDone([]);
        }
        this.phase = 'parsing';
        return [{ kind: 'parse', event: input.event }];
      }
      case 'event-error': {
        if (this.phase !== 'awaiting-event') {
          return NO_EFFECTS;
        }
        // Only this demand failed. Its `next` rejects; the call stays available, so a queued
        // or later explicit `next` sends its own demand, which the call may answer with an
        // event, the end, or its own failure. While a close is already in flight there is no
        // further demand: the queued calls settle done as on any return.
        this.nextsPending--;
        const effects: EventIteratorEffect<T>[] = [{ kind: 'reject-next', error: input.error }];
        if (this.returned || this.closeState === 'closing') {
          this.phase = 'done';
          return this.resolveAllNextsDone(effects);
        }
        if (this.nextsPending > 0) {
          effects.push(DEMAND_EFFECT);
        } else {
          this.phase = 'open';
        }
        return effects;
      }
      case 'parsed': {
        if (this.phase !== 'parsing') {
          return NO_EFFECTS;
        }
        if (this.returned) {
          this.phase = 'done';
          return this.startClose();
        }
        this.nextsPending--;
        const effects: EventIteratorEffect<T>[] = [
          { kind: 'resolve-next', result: { done: false, value: input.value } },
        ];
        if (this.nextsPending > 0) {
          this.phase = 'awaiting-event';
          effects.push(DEMAND_EFFECT);
        } else {
          this.phase = 'open';
        }
        return effects;
      }
      case 'parse-error': {
        if (this.phase !== 'parsing') {
          return NO_EFFECTS;
        }
        // The rejection waits for the close it starts: the call closes before its `next` rejects.
        this.pendingRejection = { error: input.error };
        this.phase = 'done';
        return this.startClose();
      }
      case 'close-ok': {
        if (this.closeState !== 'closing') {
          return NO_EFFECTS;
        }
        this.closeState = 'closed';
        this.phase = 'done';
        const effects: EventIteratorEffect<T>[] = [];
        if (this.pendingRejection !== undefined) {
          effects.push({ kind: 'reject-next', error: this.pendingRejection.error });
          this.pendingRejection = undefined;
          this.nextsPending--;
        }
        this.resolveAllNextsDone(effects);
        if (this.returnPending) {
          this.returnPending = false;
          effects.push(RESOLVE_RETURN_EFFECT);
        }
        return effects.length === 0 ? NO_EFFECTS : effects;
      }
      case 'close-error': {
        if (this.closeState !== 'closing') {
          return NO_EFFECTS;
        }
        this.closeState = 'close-failed';
        this.closeError = input.error;
        this.phase = 'done';
        const effects: EventIteratorEffect<T>[] = [];
        if (this.pendingRejection !== undefined) {
          // The close failure is what the rejecting `next` hears, replacing the parse failure
          // that prompted the close.
          effects.push({ kind: 'reject-next', error: input.error });
          this.pendingRejection = undefined;
          this.nextsPending--;
        }
        this.resolveAllNextsDone(effects);
        if (this.returnPending) {
          this.returnPending = false;
          effects.push({ kind: 'reject-return', error: input.error });
        }
        return effects.length === 0 ? NO_EFFECTS : effects;
      }
    }
  }

  private startClose(): readonly EventIteratorEffect<T>[] {
    this.closeState = 'closing';
    return CLOSE_EFFECTS;
  }

  private resolveAllNextsDone(effects: EventIteratorEffect<T>[]): EventIteratorEffect<T>[] {
    while (this.nextsPending > 0) {
      this.nextsPending--;
      effects.push(RESOLVE_DONE_EFFECT);
    }
    return effects;
  }
}

/**
 * Iterates the call `open` starts, each event parsed by `parse`. The first `next` opens the
 * call and each `next` sends one demand; `return` closes the call -- even while a `next`
 * waits for its event, which then resolves done -- and opens none that was never opened. A
 * `next` whose event fails to parse closes the call before it rejects. Every one of those
 * decisions is `EventIteratorCore`'s; this shell only executes its effects against the real
 * call and reports their outcomes back as inputs, close failures included.
 */
export function eventIterator<T>(open: () => Promise<NativeEvents>, parse: (json: string) => T): EventIterator<T> {
  const core = new EventIteratorCore<T>();
  let opened: NativeEvents | undefined;
  const nextWaiters: {
    resolve: (result: IteratorResult<T, undefined>) => void;
    reject: (error: unknown) => void;
  }[] = [];
  const returnWaiters: {
    resolve: (result: IteratorResult<T, undefined>) => void;
    reject: (error: unknown) => void;
  }[] = [];

  async function run(effects: readonly EventIteratorEffect<T>[]): Promise<void> {
    for (const effect of effects) {
      switch (effect.kind) {
        case 'open':
          open().then(
            (events) => {
              opened = events;
              void run(core.step({ kind: 'open-ok' }));
            },
            (error: unknown) => {
              void run(core.step({ kind: 'open-error', error }));
            },
          );
          break;
        case 'demand': {
          const events = opened;
          if (events === undefined) {
            // invariant throw: the core demands only after an open-ok, and the shell records
            // the opened call before reporting it.
            throw new Error('event iterator demanded an event before its call opened');
          }
          events.next().then(
            (event) => {
              void run(core.step({ kind: 'event', event }));
            },
            (error: unknown) => {
              void run(core.step({ kind: 'event-error', error }));
            },
          );
          break;
        }
        case 'parse':
          try {
            await run(core.step({ kind: 'parsed', value: parse(effect.event) }));
          } catch (error) {
            await run(core.step({ kind: 'parse-error', error }));
          }
          break;
        case 'close': {
          const events = opened;
          if (events === undefined) {
            // invariant throw: the core closes only a call whose open it accepted, and the
            // shell records that call before reporting the open.
            throw new Error('event iterator closed a call that never opened');
          }
          events.close().then(
            () => {
              void run(core.step({ kind: 'close-ok' }));
            },
            (error: unknown) => {
              void run(core.step({ kind: 'close-error', error }));
            },
          );
          break;
        }
        case 'resolve-next':
          nextWaiters.shift()?.resolve(effect.result);
          break;
        case 'reject-next':
          nextWaiters.shift()?.reject(effect.error);
          break;
        case 'resolve-return':
          for (const waiter of returnWaiters.splice(0)) {
            waiter.resolve(DONE);
          }
          break;
        case 'reject-return':
          for (const waiter of returnWaiters.splice(0)) {
            waiter.reject(effect.error);
          }
          break;
      }
    }
  }

  const iterator: EventIterator<T> = {
    next: () =>
      new Promise<IteratorResult<T, undefined>>((resolve, reject) => {
        nextWaiters.push({ resolve, reject });
        void run(core.step({ kind: 'next' }));
      }),
    return: () =>
      new Promise<IteratorResult<T, undefined>>((resolve, reject) => {
        returnWaiters.push({ resolve, reject });
        void run(core.step({ kind: 'return' }));
      }),
    [Symbol.asyncIterator]: () => iterator,
  };
  return iterator;
}
export type NativeJobAttachmentHandle = NativeJobAttachmentCapabilities;

interface NativeModule {
  coordinatorEndpoint(descriptor: number): CoordinatorEndpoint;
  openProject(endpoint: CoordinatorEndpoint, path: string): Promise<NativeProjectHandle>;
  connectCoordinator(endpoint: CoordinatorEndpoint, path: string): Promise<NativeCoordinatorHandle>;
  openHost(launcher: string): Promise<NativeHostHandle>;
  openSccacheHost(launcher: string): Promise<NativeSccacheHostHandle>;
}

const assertNativeModule = typia.createAssert<NativeModule>();

interface NativeBinary {
  directory: string;
  fileName: string;
}

function nativeBinary(): NativeBinary {
  const directory = addonDirectory(process.platform, process.arch);
  if (directory === null) {
    throw new Error(`Unsupported Cowshed native target: ${process.platform}-${process.arch}`);
  }
  return { directory, fileName: `cowshed.${directory}.node` };
}

export function loadNativeModule(): NativeModule {
  const { directory, fileName } = nativeBinary();
  // napi-rs loaders honour NAPI_RS_NATIVE_LIBRARY_PATH first; this is that same hook, not a
  // bespoke cowshed name. The override stays — only the spelling is the ecosystem one.
  const override = process.env.NAPI_RS_NATIVE_LIBRARY_PATH;
  // NAPI_DEBUG_ADDON is set only by the inferred napi-test target: the test suite loads the
  // dev-profile addon from .cache/native-debug (never packaged; `files` ships dist/ wholesale)
  // instead of the release artifacts. Both URL depths cover running from src/ and dist/ts/.
  const debugCandidates =
    process.env.NAPI_DEBUG_ADDON === '1'
      ? [
          new URL(`../.cache/native-debug/${fileName}`, import.meta.url).pathname,
          new URL(`../../.cache/native-debug/${fileName}`, import.meta.url).pathname,
        ]
      : [];
  const candidates = [
    ...(override ? [override] : []),
    ...debugCandidates,
    new URL(`../dist/native/host/${fileName}`, import.meta.url).pathname,
    new URL(`../dist/native/${directory}/${fileName}`, import.meta.url).pathname,
    new URL(`../native/host/${fileName}`, import.meta.url).pathname,
    new URL(`../native/${directory}/${fileName}`, import.meta.url).pathname,
  ];
  const require = createRequire(import.meta.url);
  let lastError: unknown;

  for (const path of candidates) {
    try {
      return assertNativeModule(require(path));
    } catch (error) {
      lastError = error;
    }
  }

  throw new Error(`Could not load ${fileName}. Run \`nx build cowshed\` for this platform.`, {
    cause: lastError,
  });
}
