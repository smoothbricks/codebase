# Retained replay, migration and execution lifecycle

These contracts extend the existing composed StateBus runtime. They do not add another application
store, loader, operation system or React provider. For composition and connector setup, start with
[CONSUMER.md](./CONSUMER.md).

## Finite recordings and rolling sessions

`recordScenario` remains a finite recorder: evicting an event wave makes its history incomplete,
and replay refuses it. Use `recordRollingScenario` when a long-running session needs a replayable
retained suffix.

```ts
import {
  bindEffect,
  createCaptureEnvelope,
  recordRollingScenario,
} from '@smoothbricks/statebus-core';

const journal = recordRollingScenario(runtime, {
  maxEvents: 512,
  maxEventBytes: 1024 * 1024,
  maxEffects: 128,
  maxEffectBytes: 512 * 1024,
});

// operations is the application's existing typed execute/failure implementation.
const binding = bindEffect(runtime, model.effect, {
  ...operations,
  capture: journal.outcomeSink(model.effect),
});

// At a quiescent event boundary:
const envelope = createCaptureEnvelope(composition, journal.snapshot(), {
  buildId: applicationBuildId,
  effects: [model.effect],
});
```

All state/event declarations need versioned codecs before recording begins. An outcome sink needs
the effect's `codec`. Instruction recording is separate: give the effect an `instructionCodec` and
pass `journal.instructionSink(effect)` as the binding's `captureInstruction` hook. No instruction
is inferred from an outcome or reconstructed from the current UI state.

A rolling recorder evicts only whole successful waves. It advances the checkpoint through those
waves using the existing execution-disabled reducer runtime, then retains the newer suffix.
Admission decisions are recorded and checked during replay. Failed reductions enter neither the
checkpoint nor the retained event stream. Checkpoint advancement encodes touched cells rather than
recapturing all application state on every publication.

The defaults are:

| Limit | Default | What it counts |
| --- | ---: | --- |
| `maxEvents` | 1,024 | Publications in retained complete waves |
| `maxEventBytes` | 2 MiB | Sum of encoded retained wave records |
| `maxEffects` | 256 | Retained instruction/outcome records |
| `maxEffectBytes` | 1 MiB | Sum of instruction/outcome records with sequence and wave framing |
| `maxCheckpointBytes` | 4 MiB | Encoded checkpoint, including declarations and retained cells |
| `maxCaptureBytes` | 8 MiB | Complete rolling capture, including outer framing |
| `maxEntryBytes` | 256 KiB | An encoded event, checkpoint entry or framed effect record |

All limits must be positive safe integers; queue capacities must also fit the JavaScript array
index range. Configuration owns startup memory costs: do not specify needlessly large capacities.
The two stream byte counters count their records, not outer array/envelope framing. The recorder
combines those exact section sizes and framing before materializing a cold capture, so an oversized
export does not first allocate checkpoint arrays or traverse all retained payloads again.
`createCaptureEnvelope` additionally checks its own `maxBytes`, including application/manifest metadata.

Wave and effect queues allocate their slots once at recorder setup and reuse them across eviction
and reset. Removed slots are cleared, not deleted. No suffix copying or per-record size wrapper is
needed; numeric byte sizes have separate fixed storage. Captured payloads remain detached and
immutable, never recycled while a caller may retain them. Checkpoint setup reuses already-owned
entries; existing cells are replaced without deleting/reinserting their Map keys. These are source
level work reductions, not measured bytes/op or latency improvements.

An indivisible wave or record that cannot fit is refused, not truncated into something labeled
replayable. The checkpoint's total bound is checked after the whole atomic wave: adding a new
address before removing the old one must not cause a false refusal when final state fits.
A growing checkpoint refusal disposes the interpreter and clears partial checkpoint/journal payloads.
The production wave has already committed: capture failure must not suppress admitted operations or
roll production state back. `stats().refusal` exposes the recorded failure, and `snapshot()` throws
`CaptureError` until an explicit successful `reset()`. A cold-export size refusal alone does not
stop recording. `stats().effects` reports retained instruction/outcome count alongside its byte total.

`reset()` starts from current state at a quiescent event queue and clears retained history.
`dispose()` removes recorder observation and the replay interpreter. Already returned snapshots
remain detached and frozen. The application owns copies it retains and must dispose its runtime
and operation bindings at their actual lifecycle boundaries.

Effect retention is independent of state replay. A late outcome keeps its original plan/request
identity even when its command has moved into the checkpoint. Evicting side-history records
increments `evictedEffects`; it does not remove the decoded result events already in the scenario.

## Portable values and application envelopes

`captureValue(input, maxBytes?)` creates detached, frozen portable values and reports their exact
JSON UTF-8 byte count. `captureBytes` checks/counts without allocating another payload tree or JSON
string. `canonicalCapture` produces deterministic JSON from that owned representation.

The transport accepts JSON primitives, arrays and plain objects. It refuses cycles, non-finite
numbers, class instances, symbol keys, accessors, sparse arrays and undefined array elements.
Undefined object properties are omitted. The walk does not call getters or `toJSON`. The default
portable-value bound is 8 MiB with nesting limited to 64.

Object keys have deterministic JavaScript JSON ordering: non-index keys are inserted in UTF-16
lexical order, while JSON retains its native integer-index ordering. Checkpoint resource IDs are
ordered numerically before strings; numeric `7` and string `"7"` remain different resources.
Array order remains meaningful. This package format is not an RFC 8785 certification, heap-size
measurement or guarantee about allocations inside application codecs.

`CaptureEnvelope` records the original application build/environment, library versions,
declaration/ID codec versions, effect/instruction codec versions, scenario and retained effect
positions. `environment` is an explicitly supplied application summary, not permission to capture
`process.env`, ports, request headers or credentials.

Validate data from storage against the real public envelope type before importing it, and enforce
a transport-size bound before parsing untrusted JSON. For example:

```ts
import {
  type CaptureEnvelope,
  canonicalCapture,
  replayCaptureEnvelope,
} from '@smoothbricks/statebus-core';
import typia from 'typia';

const parseEnvelope = typia.json.createAssertParse<CaptureEnvelope>();
const imported = parseEnvelope(canonicalCapture(envelope));
const replay = replayCaptureEnvelope(composition, imported, [model.effect]);
try {
  inspectReplay(replay);
} finally {
  replay.dispose();
}
```

The archive's semantic checks supplement the structural validator: library identity, declaration
coverage, codec headers and causal positions must match. Missing scalar checkpoint cells, duplicate
effect headers, unknown captured effects and corrupt sequence positions refuse import.

Replay uses the existing `mode: 'replay'` runtime. Effect and QueryClient bindings do not install
execution subscriptions there. Recorded reaction publications are consumed, not regenerated.
Retained instructions/outcomes are side history, never a second source of result publications.
Use `decodeEffectInstruction` and `decodeEffectOutcome` for typed inspection; direct scenario
replay does not interpret those side-history payloads as another operation stream.

`CaptureError.issue` carries a code and boundary, with declaration/version information where
applicable. Local errors can contain private names and causes; they are not themselves support
artifacts. Share only explicitly approved diagnostic fields or a successful support projection.

## Library-owned codec evolution

A library owns the conversion from its old payload types to its current ones. `evolveCodec` links
a current codec to a previous codec and a pure typed upgrade:

```ts
import { evolveCodec, type ValueCodec } from '@smoothbricks/statebus-core';
import typia from 'typia';

const balanceV1: ValueCodec<number> = {
  schema: 'account.balance',
  version: 1,
  encode: value => value,
  decode: typia.createAssert<number>(),
};

const balanceV2 = evolveCodec(
  {
    schema: 'account.balance',
    version: 2,
    encode: (value: { balance: number }) => ({ balance: value.balance }),
    decode: typia.createAssert<{ balance: number }>(),
  },
  balanceV1,
  balance => ({ balance }),
);
```

Attach the evolved codec to the same declaration in the next library definition. Set that
definition's `version` and explicitly list supported `previousVersions`. Library-version
permission alone is insufficient: every old declaration, ID, instruction and outcome codec must
have a compatible registered conversion. Chaining evolved codecs carries their previous source
versions forward. Duplicate migration sources are rejected.

`migrateScenario` upgrades the same owned declaration set. `migrateCaptureEnvelope` additionally
checks library/effect versions and upgrades retained instructions/outcomes:

```ts
const migrated = migrateCaptureEnvelope(nextComposition, imported, {
  buildId: nextApplicationBuildId,
  effects: nextEffects,
});
const replay = replayCaptureEnvelope(nextComposition, migrated, nextEffects);
```

The original `application.buildId` remains intact; `migratedForBuild` identifies the target.
Wave numbers, event order, admission decisions and side-history positions are preserved.
Conversions cannot silently move state into another library or collapse two resource IDs.

This is **payload evolution, not structural composition migration**. Adding, removing or renaming
declarations, changing owners or changing the effect declaration set is refused. Such a change
needs an explicitly designed new scenario contract. Callbacks must remain pure and validate their
historical domain input with the owning codec; an envelope never supplies executable migration
code. The application/library is responsible for the semantic correctness of each upgrade.

## Deny-default support projections

Lossless replay data is private. `exportSupportCapture` is a separate permission boundary, not a
serializer that makes arbitrary local captures safe to upload.

A declaration's `support` policy receives its decoded value. A keyed declaration additionally
needs `supportId`; an effect can independently provide `supportInstruction` and `support`.
Select named fields rather than forwarding a complete object:

```ts
const status = scope.scalar('status', initialStatus, {
  codec: statusCodec,
  support: publicSupport((value: Status) => ({ phase: value.phase })),
});

const rows = scope.keyed('rows', initialRow, {
  codec: rowCodec,
  idCodec: rowIdCodec,
  supportId: publicSupport((id: RowId) => approvedRowAlias(id)),
  support: publicSupport((value: Row) => ({ state: value.state })),
});
```

Without a policy, domain data is redacted even when its local classification is `public`.
`secret` and `excluded` values cannot be included by a broad policy or consent.
`sensitive` values and `consentSupport` decisions require `consent: true`.
A denied keyed ID also denies the associated value **before its value codec or projection runs**.

Build IDs, library/owner names, declaration/schema names and environment are denied by default.
The artifact uses capture-local numeric declaration/effect references. An explicit metadata
projection can approve a build identifier or map manifest indexes to application-owned aliases:

```ts
const artifact = exportSupportCapture(composition, envelope, {
  effects: [model.effect],
  metadata: publicSupport(({ application }) => ({
    buildId: application.buildId,
  })),
});
```

An environment projection is separately supplied with `environment`. Do not forward the full
metadata/application object: that deliberately grants broader access, including environment.
Neither numerical references nor application aliases are cryptographic anonymization. Wave
positions, admission flags, record counts and redaction markers remain visible; applications
whose support policy forbids those relationships must not share this artifact as-is.

Configured projections are trusted privacy code. `publicSupport(value => value)` deliberately
approves the whole decoded object and can expose newly added fields. It is not an automatic
field allowlist. The consumer regression demonstrates that an explicit `{ phase: value.phase }`
projection excludes an additional private field. Reserved credential field names and recognizable
credential strings are scrubbed as a last-line guard; arbitrary opaque secrets mislabeled as
public cannot be detected universally.

A thrown codec/projection refuses the whole export without returning a partial artifact or
falling back to raw data. Projection errors expose a fixed `CaptureError` boundary, not the private
exception message or nested cause. Input and output byte limits are enforced. The successful
artifact is frozen and has `kind: 'sanitized-support'` and `replayable: false`; it is not a
`CaptureEnvelope` and cannot legitimately be supplied to replay.

The existing `classifyScenario` helper remains available with its original whole-record filtering
contract. It is not a substitute for these explicit field/ID/metadata projections.

## Execution policies, reactions and cleanup

See [EXECUTION.md](./EXECUTION.md) for the current admission-capacity defaults, overload outcomes,
listener-cycle recovery, navigation lifetime contract and separately measured execution benchmark.
`operations.maxPending` bounds queued plus actual running work per effect binding (default 1024),
including aborted operations that have not settled. Capacity refusal uses `EffectCapacityError`
through the application's existing failure mapper and decoder; it never silently drops an admitted
command. This is separate from journal retention limits.

The existing effect boundary now accepts either `execute`, returning one value or Promise, or
`stream`, returning an iterable or async iterable. The distinction is explicit: an array-shaped
outcome from `execute` is one value, not a stream. Direct values and synchronous operation failures
do not publish outcomes synchronously inside the caller's command flush.

`EffectPlan.requestId` remains request identity. Optional `operationKey` identifies the group for
the definition's `policy`; without it, the key is the request ID.

| Policy | Behavior within one binding/key |
| --- | --- |
| `parallel` | Execute distinct admitted requests concurrently |
| `serialize` | Start the next queued request after the active request settles |
| `latest-wins` | Abort/supersede older requests and suppress their late publications |
| `drop-duplicate` | Do not execute another request while the key has active work |

`cancel(requestId)` and `cancelKey(operationKey)` preserve inferred ID/key types. Queued requests
can be cancelled without executing them. A definition's optional `cancelled` decoder maps
cancelled, superseded or duplicate plans to domain result events. Keys do not coordinate unrelated
effect definitions, mounts or runtimes; existing loader/QueryClient policy remains unchanged.

`binding.drain()` waits for that binding's actual work and cooperative iterator finalization.
`runtime.drain()` also flushes successor event waves and waits for effects they start.
`disposeAsync()` first disposes, then waits for settlement. Non-cooperative operations can keep
either wait pending, and client cancellation cannot roll back a server mutation.

For pure cross-library follow-up publications, use `scope.react(source, target, project)`.
The projector reads batch-final state and emits into a successor wave without inventing an I/O
operation. Both handles must belong to the composition; cross-library handles can be supplied
through typed capabilities. Publishing a public command does not grant permission to write its
owner's state directly. `maxReactionSteps` bounds total reaction fanout per causal root, not just
depth. A later asynchronous effect result starts a new causal root.

A library marks required interpreters with `scope.requireEffect(defineEffect(...))`.
After binding operations, call `runtime.assertReady()`. Composition-bound React providers do
this before descendants render; disposing a binding invalidates the cached readiness result.
Replay skips execution requirements. This is declared-effect preflight, not a general provider
discovery framework.

`bindRuntimeDiagnostics` converts runtime-boundary failures to typed phase/wave facts through a
declared event. It does not publish raw `Error` values, credentials or exception text.

## Acceptance and measurement boundaries

Run `nx lint statebus-core` before `nx test statebus-core`. The existing test dependency includes
`nx run statebus-core:verify-packages`; no alternate preview workflow is required.

That verifier uses real tarballs, strict public declarations and native Typia-generated validators
under Node and Bun with both isolated and hoisted consumer installations. Its ten composed
programs are `scenario`, `edge-cases`, `capture-scenarios`, `execution-scenarios`, `support-edges`,
`journal-edges`, `loader-edges`, `runtime-edges`, `navigation-edges` and `aggregate-edges`. They cover generated long
streams, byte pressure, rollback, immutable captures, typed migrations, operation policy/cleanup,
replay without I/O, support privacy, malformed-envelope rejection, atomic checkpoint relocation,
queue wrap/reset, pre-materialization refusal, execution overload/reentrancy and navigation cleanup.

Byte counters are canonical JSON UTF-8, not live heap, GC, latency or allocation measurements.
Changed-cell encoding and checkpoint materialization counters describe that work only. No
zero-allocation or engine-optimization claim follows from these tests.

The journal's capacity is **not** a runtime-wide memory guarantee. Effects and composed query
loaders now share `maxPendingWork` across bindings, in addition to each effect's local limit.
Composed runtime `maxWavesPerFlush` stops runaway imperative cascades at complete-wave boundaries
and retains the paused queue for explicit recovery. Application state, pending event storage, one
wave's size and elapsed execution time remain outside those bounds.
See EXECUTION.md for the measured synthetic allocation/timing comparison and its limits; journal
byte accounting alone does not establish those performance results.

A performance acceptance run must separate unrecorded composed dispatch, active effects, retained
recording/eviction and cold export. Report actual allocation/GC evidence and tail latency separately
from logical byte/count bounds. The existing exact-interest diagnostics do not measure all those
paths. Reusing journal slots does not make owned payload capture or JavaScript execution allocation-free.


Aggregate execution admission is configured with `RuntimeOptions.maxPendingWork` (default 4096).
All effect and composed QueryClient bindings in the same runtime share it; cancelled-but-unsettled
work still counts, and rebinding cannot bypass it. See [aggregate admission](./EXECUTION.md#aggregate-runtime-admission)
for exact read/retry semantics, `RuntimeCapacityError`, the `runtime.work` execution-boundary port,
and the distinction between admitted work and pending event storage.
