# Execution capacity, lifetime and performance audit

This extends the existing composed runtime and navigation adapters. It does not replace composition,
React subscriptions, the exact-interest registry or QueryClient. See [REPLAY.md](./REPLAY.md) for the
recording and migration contracts; those capture bounds are not runtime-wide memory bounds.

## Admitted effect capacity

`bindEffect(runtime, definition, operations)` accepts `operations.maxPending`, a positive safe integer
(default **1024**). This bounds queued requests plus actual running operations **within that binding**.
An aborted operation still occupies its slot until its Promise/iterator physically settles. Cancelling
a queued operation immediately frees its slot and never allocates an AbortController for that request.

When the limit is reached, execution is not started. The existing application `failure(cause, plan)`
receives `EffectCapacityError` (`code: 'effect-capacity'`, `limit`), and its typed outcome passes through
the existing decoder into a successor-wave result. This lets the application's reducer reconcile an
admitted command without stranding its pending state. Duplicate request IDs in the same wave are
still refused once; the overload path does not turn one submission into multiple outcomes.

```ts
import { bindEffect, EffectCapacityError } from '@smoothbricks/statebus-core';

const binding = bindEffect(runtime, model.effect, {
  maxPending: 128,
  execute: operations.execute,
  failure: (cause, plan) => cause instanceof EffectCapacityError
    ? operations.busy(plan) // Application-owned typed outcome; never a fictional server rollback.
    : operations.failure(cause, plan),
});
```

The local capacity applies to all operation keys in the binding. The runtime-wide `maxPendingWork`
budget must also admit the request; see [aggregate admission](#aggregate-runtime-admission).
At capacity, even `latest-wins` refuses a new request instead of cancelling an old operation and pretending that its resources were freed.
Ordinary direct/Promise execution outcomes remain asynchronous. Overload refusal can be decoded
while the command wave is notifying listeners, but its result still reduces in a successor wave.
That result does not represent an external operation, and no instruction-executed claim is made.

Per-key scheduling now uses group-local linked job membership. `cancelKey` and supersession no longer
scan unrelated keys; serialized cancellation unlinks a queued job without compacting a queue. A
sequence frontier prevents a reentrant abort observer from cancelling new same-key work created
inside that observer. Capture hooks are also treated as reentrant: a request is reserved before the
hook runs and rechecked for cancellation/disposal before execution.

Job objects initialize the same fields in the same order. Controllers are created only when work
starts. Cancellation shares an immutable `AbortError` reason; request identity remains in the plan,
not in an exception instance. Per-binding and per-runtime drain bookkeeping counts completion and
uses stable callbacks/lazy shared waiters instead of a second Promise Set and copied drain arrays.
Already returned records are never recycled to obtain these reductions.

`binding.drain()` waits for that binding's started operations and cooperative iterator cleanup.
`runtime.drain()` additionally flushes successor publications and waits for tracked loaders/navigation.
New work can keep a drain pending. Non-cooperative work can outlive synchronous disposal; neither
abort nor disposal is a rollback of a server mutation.

## Runaway dispatch is fail-stop, not silent dropping

`composition.createRuntime({ maxWavesPerFlush })` bounds the number of **complete waves** processed
by one synchronous flush (default **1024**). It does not split a reducer wave or limit its event count.
At the limit, `DispatchCycleError` (`code: 'dispatch-cycle'`, `limit`) is thrown. Automatic scheduling
reports that failure through the existing runtime error channel and stops scheduling the cascade.

The pending queue is retained and `runtime.dispatchPaused` becomes true. New publications can join
that queue but do not automatically restart it. Remove/fix the cyclic listener, then explicitly call
`runtime.flush()` to resume, or dispose the runtime to discard its work. Stale microtasks left over
from an earlier manual flush cannot silently resume a paused queue. The same capacity-retaining
queue engine is shared by the older runtime entry points.

This is a programmer-error recovery boundary, **not event backpressure**. Arbitrary pending event
storage, one enormous wave, application state, listener count, and the aggregate number of effect
bindings remain outside these limits. A runtime-wide publication capacity needs a coherent typed
refusal policy for commands, result events and interest leases; merely dropping a full queue would
break those contracts. No total-heap or maximum event-loop-time guarantee is claimed.

## Adjacent navigation and React corrections

`connectNavigation` and `connectBrowserNavigation` accept optional `onError` and `trackExecution`
hooks through the exported `NavigationConnectionOptions<Target, Location>` contract. A composition
owner can attach the existing runtime, rather than create a navigation store:

```ts
const stop = runtime.manage(connectBrowserNavigation({
  window,
  channel, // Application-owned port over declared navigation events/state.
  work: runtime.work,
  onError: cause => runtime.reportError(cause),
  trackExecution: task => runtime.trackExecution(task),
}));
```

Setup now unwinds acquired channel/native subscriptions if later installation or initial
publication fails. Disposal attempts every cleanup, even if one release throws. An abort callback
that disposes the connection or submits a newer request cannot make the interrupted older request
execute afterwards. Async driver
completion and observation publication failures reach `onError` instead of unhandled rejections.
Tracked completion represents actual asynchronous driver settlement, not merely an abort signal.
Synchronous browser-history writes still allocate no completion Promise.

The memory driver gives repeated subscriptions of the same callback independent leases. Pushing
following Back truncates the forward branch in place without allocating an unused removed-entry
array. Constant Back/Forward planner results are immutable and reused. Browser setup rollback
removes partially installed popstate/hashchange handlers; guard cleanup failure cannot strand the
beforeunload or location subscriptions.

Primitive React selection props now include booleans. Existing memoized subscriptions, publishers
and committed-binding semantics are retained. React's production declaration lib is ES2022, matching
the public `ErrorOptions` type already exposed by the core package; this does not enable ambient Node
types or weaken declaration checks.

## Measured synthetic workload

`benchmarks/execution.mjs` runs through the public built core export. It holds unrelated operation
keys pending, then measures superseding one hot key. Setup, warmup and teardown are outside the
measured interval. Every operation in this fixture ignores abort until a shared completion Promise
resolves; the candidate explicitly configures enough capacity to exercise scheduling rather than
measure refusal. This is an execution-path test, not a capture or transport throughput benchmark.

Example commands from an external directory containing the harness and installed built package:

```sh
STATEBUS_BENCH_MODE=timing node --expose-gc execution.mjs
STATEBUS_BENCH_MODE=allocation node --expose-gc execution.mjs
```

Optional `STATEBUS_BENCH_KEYS`, `STATEBUS_BENCH_ITERATIONS` and `STATEBUS_BENCH_COMMIT` select/report
inputs. Run baseline and candidate in fresh processes, alternating order. Output includes engine,
CPU and hashes of the actual built index/composition/effects/dispatch files, not just a branch name.

Local audit: Node **22.16.0**, V8 **12.4.254.21-node.26**, Intel Xeon Platinum 8573C. Three baseline /
candidate pairs, with the second pair reversed, 256 warmup submissions then 2,000 measured
submissions per key count. Timing and allocation sampling ran in separate processes. The table
reports the median of the three run-level measurements, not pooled production quantiles.

| Unrelated keys | Baseline p50 / p99 (microseconds) | Candidate p50 / p99 (microseconds) | Sampled bytes/submission: baseline / candidate |
| ---: | ---: | ---: | ---: |
| 64 | 13.320 / 114.444 | 7.370 / 50.707 | 5,521 / 3,592 |
| 4,096 | 27.252 / 138.637 | 5.093 / 47.873 | 4,011 / 3,089 |
| 16,384 | 105.848 / 419.332 | 3.005 / 23.891 | 3,823 / 2,853 |

The baseline's relevant execution/dispatch/React sources are unchanged between the original
verified package build at `52a95c3ba0f9d3892bc3cb6e19693e6b8b0695b2` and the audit base
`0b6896357480caa50547ca3caba860de8fda8981`. Both sides used extracted release-shaped public packages.
The candidate was compiled locally with TypeScript 6.0.3, strict declarations, and the same dependency
versions. The native repository compiler and full consumer matrix are separate CI gates.

Allocation numbers are **V8 sampling estimates** at a 4,096-byte interval, with objects collected by
both minor and major GC included. They include harness and engine work and are not exact heap
allocations. No GC pause occurred in the measured windows, so this run establishes **no GC-pause
improvement**. JIT tiering and shared-host noise explain some non-monotonic small-workload results.
This evidence supports removal of unrelated-key linear scans and reduced observed allocation work;
it does not certify browser/app latency, zero allocation, all V8 shapes, or a universal memory bound.

These measurements precede aggregate admission. They do not measure the new shared counter.
The harness now explicitly sets both runtime and binding headroom for this workload so capacity
refusal is not mistaken for a scheduling speedup.

## Regression gate and remaining scope

The existing real-package verifier additionally runs `runtime-edges.ts` and `navigation-edges.ts`:
22 scenarios including generated FIFO/cancellation traces, exact capacity, non-cooperative work,
reentrant capture/abort, failed reductions, listener-cycle recovery, browser partial setup/cleanup,
tracked navigation completion and real ReactDOM StrictMode with boolean selection props.
The verifier retains all previous consumer programs and both Node/Bun × isolated/hoisted installs.

This review covered the composed/legacy dispatch seam, effect ownership, runtime completion,
React binding identity, structural interest/byte-progress producer paths and navigation adapters.
No speculative rewrite was made to working interest or chunk-progress loops. QueryClient sharing,
retry and grace logic stays with the existing loader implementation. Scheduling domains remain
binding-local, capture migrations remain payload-only, and sanitized-support artifacts are not
replayable. Those limits are not silently presented as completion of the entire application brief.


## Aggregate runtime admission

`composition.createRuntime({ maxPendingWork: 4096 })` owns one execution budget for the entire
runtime, shared automatically by every `bindEffect` and `bindComposedQueryLoader`. The default
is 4,096; the limit must be a positive safe integer and is validated before state initialization.
`runtime.work.limit` and `runtime.work.pending` expose allocation-free counter reads. Separate
runtime instances never share this budget, even when they use the same composition and handles.

Effect jobs reserve before queuing, capture callbacks, supersession or operation invocation. Both
binding-local `operations.maxPending` and the aggregate limit must allow admission. A queued job
releases when cancelled before execution. A running job keeps its slot through actual completion,
including iterator finalization, even after cancellation, binding disposal or replacement. Rebinding
an interpreter cannot reset the runtime's count. At exhaustion, the existing failure mapper receives
`RuntimeCapacityError` (`code: 'runtime-capacity'`, `limit`) and the pure decoder publishes the typed
outcome in the successor wave. No operation starts and no replacement abort is triggered for that
refused job. Capacity errors are allocated once per runtime, not once per refused submission.

An admitted loader read reserves **before query configuration and observer setup**. It owns one
slot for its QueryClient waiter plus its actual transport. Shared query work still has one slot per
logical StateBus read: transport deduplication does not erase each consumer's waiter and lifecycle.
Retries reuse that read's slot and request ID; no extra slot is taken just to run its transport.
Cancelling a QueryClient waiter cannot release the slot while an abort-ignoring transport is still
running. Stored query functions invoked later by an external QueryClient refetch acquire fresh
transport admission instead of reusing the old request's released reservation.

Loader capacity refusal goes directly through its typed `failure`/`loadFailed` path, outside
QueryClient, so QueryClient retry configuration cannot accumulate automatic retries for refused
StateBus requests. Cached reads also require logical-read admission. Resource interest remains
accurate; application policy decides when to retry after capacity becomes available. No polling,
unbounded wait queue, silent dropping of admitted results, or automatic mutation retry is added.

The low-level loader's optional `work` port is the exported `WorkAdmission` interface. The composed
binding supplies `runtime.work` automatically; callers cannot replace it through composed options.
Other application-owned execution adapters can participate using that same port. It is a trusted
execution-boundary protocol: each successful `tryAcquire()` must be paired with exactly one
`release()` after all owned work settles, normally in `finally`. Do not release on unmount/abort,
share a reservation between independent operations, or release another adapter's work. Calling
release with no outstanding admission throws an invariant error. Admission itself returns a boolean,
not a per-operation lease/wrapper object or another dependency-injection container.

`runtime.drain()` observes outstanding reservations as well as registered completion tasks. This
covers calls made reentrantly from a capture callback after reservation but before a task Promise
exists. Disposal closes admission before invoking cleanup/abort listeners and does **not** reset
pending counts. Non-cooperative work can therefore outlive synchronous disposal or keep
`disposeAsync()` pending; actual settlement eventually releases its slot exactly once.

`connectNavigation` and `connectBrowserNavigation` accept `work: runtime.work` alongside the
existing `trackExecution` hook. They refuse before invoking a native/custom driver, release
synchronous history writes immediately, and retain asynchronous work until genuine settlement.
Refusal publishes `navigationFailed` with the typed error code `capacity`. Their small structural
port keeps platform-neutral navigation independent of the StateBus implementation.

The pre-existing `trackExecution(task)` observes a Promise that has already started; it cannot
retroactively admit or prevent arbitrary external work. Direct clients and custom interpreters
must acquire `runtime.work` before starting work to participate in this bound.
The budget covers registered interpreters, not JavaScript executed outside these boundaries.
Pending event storage, application state, payload bytes and execution time are separate limits.
The dispatch queue's existing whole-wave cycle guard remains unchanged in this aggregate-work change.

The aggregate tests use the normal packed-consumer gate. They cover same-wave requests across
libraries, independent runtimes, queued cancellation, disposed/rebound interpreters, reentrant
capture/abort/drain, iterator finalization, mixed effect/loader pressure, shared queries, retry
identity, paused reads, reused query callbacks, setup failures, navigation/history refusal and replayed overload results.
Generated multi-binding cancel/settle traces compare the count with the outstanding operations.
These assert capacity and ownership invariants, not a new allocation/GC or tail-latency measurement.
