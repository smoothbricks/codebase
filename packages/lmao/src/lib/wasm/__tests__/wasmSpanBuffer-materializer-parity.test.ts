/**
 * Parity suite: WASM SpanBuffer classes generated under the compiled
 * (new Function) materializer must be observably identical to the
 * closure-composed (no-eval) materializer. Every cell of the matrix
 * message family x message physical layout x eager storage mode is built
 * twice on fresh allocators, driven through the same write script, and
 * compared with toStrictEqual.
 *
 * Compared per cell (root, child, chained overflow):
 * - instance own-key names in insertion order, and prototype/class member
 *   names with descriptor kinds (getter, setter, value type, flags)
 * - raw identity bytes, system/message/row-header/log-header lanes
 * - column pointer allocation state (pointers normalised to -1 / '>=0')
 * - column values and null bitmaps, read non-allocating first and through
 *   the lazy `_values`/`_nulls` getters afterwards (those allocate)
 * - stats after span-chain sealing, and the error text thrown by static-only
 *   row guards and released-buffer guards (generation numbers normalised)
 *
 * Not compared: span ids (global counter; span_id bytes are zeroed), raw
 * WASM pointers, and wall-clock span-start times (timestamp lanes are
 * overwritten with explicit values; span-start times are compared by type).
 */

import { afterEach, describe, expect, it } from 'bun:test';
import {
  S as ArrowS,
  activeMaterializerMode,
  type MaterializerMode,
  setMaterializerModeOverride,
} from '@smoothbricks/arrow-builder';
import { createTestOpMetadata, TEST_TRACER } from '../../__tests__/test-helpers.js';
import { defineOpContext } from '../../defineOpContext.js';
import { resolveEagerColumns } from '../../physicalLayoutPlan.js';
import {
  type MessageLayoutFamily,
  type MessagePhysicalLayout,
  RUNTIME_HINT_ANALYZED_VALID,
  RUNTIME_HINT_LOG,
  RUNTIME_HINT_RESULT,
  RUNTIME_HINT_TAG,
} from '../../runtimeHint.js';
import { S } from '../../schema/builder.js';
import { defineLogSchema } from '../../schema/defineLogSchema.js';
import type { LogSchema } from '../../schema/LogSchema.js';
import { EMPTY_SCOPE } from '../../spanBuffer.js';
import { createTraceId } from '../../traceId.js';
import { createWasmAllocator, type WasmAllocator } from '../wasmAllocator.js';
import {
  createWasmChildSpanBuffer,
  createWasmOverflowBuffer,
  createWasmSpanBuffer,
  getWasmSpanBufferClass,
  type WasmSpanBufferInstance,
} from '../wasmSpanBuffer.js';
import { WasmTraceRoot } from '../wasmTraceRoot.js';

afterEach(() => {
  setMaterializerModeOverride(undefined);
});

const CAPACITY = 64;
const WRITE_INDEX = 4;
const NO_ERROR = '<no error>';

const FAMILIES: readonly MessageLayoutFamily[] = ['mixed', 'static-only', 'dynamic-only'];
const PHYSICALS: readonly MessagePhysicalLayout[] = ['current', 'specialized', 'packed'];

/**
 * lazy: no eager declarations; columns allocate on first write.
 * schema-eager: `.eager()` on category/number (schema-eager lanes, no null bitmap).
 * preallocated: plan-proven eager columns from `defineOp({ eagerColumns })`.
 */
type Storage = 'lazy' | 'schema-eager' | 'preallocated';
const STORAGES: readonly Storage[] = ['lazy', 'schema-eager', 'preallocated'];

/** Every schema column, in declaration order, so each lane kind is observed. */
const COLUMN_NAMES: readonly string[] = ['userId', 'errorMessage', 'duration', 'requestBytes', 'operation', 'isAdmin'];

function withMode<T>(mode: MaterializerMode, fn: () => T): T {
  setMaterializerModeOverride(mode);
  try {
    return fn();
  } finally {
    setMaterializerModeOverride(undefined);
  }
}

function makeSchema(storage: Storage): LogSchema {
  if (storage === 'schema-eager') {
    return defineLogSchema({
      userId: ArrowS.category().eager(),
      errorMessage: S.text(),
      duration: ArrowS.number().eager(),
      requestBytes: ArrowS.bigUint64(),
      operation: S.enum(['SELECT', 'INSERT', 'UPDATE', 'DELETE']),
      isAdmin: S.boolean(),
    });
  }
  return defineLogSchema({
    userId: S.category(),
    errorMessage: S.text(),
    duration: S.number(),
    requestBytes: ArrowS.bigUint64(),
    operation: S.enum(['SELECT', 'INSERT', 'UPDATE', 'DELETE']),
    isAdmin: S.boolean(),
  });
}

/** Op metadata whose startup plan proves `userId` and `duration` eager (preallocated lanes). */
function preallocatedMetadata(schema: LogSchema) {
  const context = defineOpContext({ logSchema: schema });
  const op = context.defineOp('wasm-materializer-parity', (ctx) => ctx.ok(null), undefined, {
    runtimeHint: RUNTIME_HINT_ANALYZED_VALID | RUNTIME_HINT_LOG | RUNTIME_HINT_RESULT | RUNTIME_HINT_TAG | 8,
    eagerColumns: ['userId', 'duration'],
  });
  return op.metadata;
}

function callMethod(target: object, name: string, ...args: unknown[]): unknown {
  const method = Reflect.get(target, name);
  if (typeof method !== 'function') throw new Error(`Missing method: ${name}`);
  return Reflect.apply(method, target, args);
}

/** Writes one raw lane slot. Optional lanes exist only for some physical layouts. */
function setLane(buffer: object, name: string, index: number, value: number | bigint, optional: boolean): void {
  const lane: unknown = Reflect.get(buffer, name);
  if (typeof lane !== 'object' || lane === null) {
    if (optional) return;
    throw new Error(`Missing required lane: ${name}`);
  }
  Reflect.set(lane, index, value);
}

/**
 * Deterministic write script: messages, every lane family, schema columns
 * (including explicit nulls), and the write index. Timestamps are explicit so
 * wall-clock values stamped by the span lifecycle are overwritten.
 */
function fillBuffer<T extends LogSchema>(buffer: WasmSpanBufferInstance<T>, family: MessageLayoutFamily): void {
  callMethod(buffer, 'message', 0, 'span-name');
  callMethod(buffer, 'message', 1, 'terminal');
  if (family !== 'static-only') callMethod(buffer, 'message', 2, 'log-entry');
  setLane(buffer, 'timestamp', 0, 1000n, false);
  setLane(buffer, 'timestamp', 1, 1500n, false);
  setLane(buffer, 'timestamp', 2, 2000n, false);
  setLane(buffer, 'timestamp', 3, 2500n, false);
  setLane(buffer, 'entry_type', 2, 5, true);
  setLane(buffer, '_rowHeaders', 2, (7 << 8) | 5, true);
  setLane(buffer, '_messageIds', 2, 3, true);
  setLane(buffer, '_logHeaders', 2, 9, true);
  callMethod(buffer, 'userId', 2, 'alice');
  callMethod(buffer, 'userId', 3, 'bob');
  callMethod(buffer, 'errorMessage', 2, 'boom');
  callMethod(buffer, 'errorMessage', 3, null);
  callMethod(buffer, 'duration', 2, 12.5);
  callMethod(buffer, 'duration', 3, null);
  callMethod(buffer, 'operation', 2, 1);
  callMethod(buffer, 'isAdmin', 2, true);
  callMethod(buffer, 'isAdmin', 3, false);
  Reflect.set(buffer, '_writeIndex', WRITE_INDEX);
}

function snapshotValue(value: unknown): unknown {
  if (value === undefined || value === null) return value;
  if (typeof value === 'function') return 'function';
  if (typeof value !== 'object') return value;
  if (Array.isArray(value)) return value.map((item: unknown) => snapshotValue(item));
  if (ArrayBuffer.isView(value)) {
    return {
      ctor: value.constructor.name,
      bytes: Array.from(new Uint8Array(value.buffer, value.byteOffset, value.byteLength)),
    };
  }
  return Object.fromEntries(Object.entries(value).map(([key, item]): [string, unknown] => [key, snapshotValue(item)]));
}

/** Allocation shape only: -1 when unallocated, '>=0' when allocated (pointer values are not compared). */
function columnPtrShape<T extends LogSchema>(buffer: WasmSpanBufferInstance<T>): (number | string)[] {
  return Array.from(buffer._columnPtrs, (ptr) => (ptr === -1 ? -1 : '>=0'));
}

/**
 * Normalized observable state of one buffer. Non-allocating accessors run
 * first; the lazy `_values`/`_nulls` getters run last because they allocate.
 */
function snapshotBuffer<T extends LogSchema>(buffer: WasmSpanBufferInstance<T>): Record<string, unknown> {
  const identity = Array.from(buffer._identityView);
  for (let index = 4; index < 8; index++) identity[index] = 0; // span_id: global counter

  const columns: Record<string, unknown> = {};
  for (const name of COLUMN_NAMES) {
    columns[name] = {
      allocatedValues: snapshotValue(buffer.getColumnIfAllocated(name)),
      allocatedNulls: snapshotValue(buffer.getNullsIfAllocated(name)),
    };
  }

  const snapshot: Record<string, unknown> = {
    ownKeys: Reflect.ownKeys(buffer).map((key) => String(key)),
    capacity: Reflect.get(buffer, '_capacity'),
    writeIndex: Reflect.get(buffer, '_writeIndex'),
    overflowWriteIndex: buffer._overflowWriteIndex,
    statsSealed: Reflect.get(buffer, '_statsSealed'),
    statsReservedRows: Reflect.get(buffer, '_statsReservedRows'),
    identity,
    threadId: Reflect.get(buffer, 'thread_id'),
    parentThreadId: snapshotValue(Reflect.get(buffer, 'parent_thread_id')),
    traceId: snapshotValue(Reflect.get(buffer, 'trace_id')),
    hasParent: Reflect.get(buffer, '_hasParent'),
    spanStartTimeType: typeof Reflect.get(buffer, '_spanStartTime'),
    lastLoggedTimeType: typeof Reflect.get(buffer, '_lastLoggedTime'),
    spanName: Reflect.get(buffer, '_spanName'),
    terminalMessage: Reflect.get(buffer, '_terminalMessage'),
    scopeValues: snapshotValue(Reflect.get(buffer, '_scopeValues')),
    messageLayoutFamily: Reflect.get(buffer, '_messageLayoutFamily'),
    messagePhysicalLayout: Reflect.get(buffer, '_messagePhysicalLayout'),
    message: snapshotValue(buffer._message),
    messageValues: snapshotValue(Reflect.get(buffer, 'message_values')),
    timestamp: snapshotValue(Reflect.get(buffer, 'timestamp')),
    entryType: snapshotValue(Reflect.get(buffer, 'entry_type')),
    rowHeaders: snapshotValue(buffer._rowHeaders),
    messageIds: snapshotValue(buffer._messageIds),
    logHeaders: snapshotValue(buffer._logHeaders),
    columnPtrs: columnPtrShape(buffer),
    columns,
  };

  const getters: Record<string, unknown> = {};
  for (const name of COLUMN_NAMES) {
    getters[name] = {
      values: snapshotValue(Reflect.get(buffer, `${name}_values`)),
      nulls: snapshotValue(Reflect.get(buffer, `${name}_nulls`)),
    };
  }
  snapshot.getters = getters;
  snapshot.columnPtrsAfterGetters = columnPtrShape(buffer);
  return snapshot;
}

interface MemberShape {
  readonly key: string;
  readonly getter: boolean;
  readonly setter: boolean;
  readonly valueType: string;
  readonly enumerable: boolean | undefined;
  readonly writable: boolean | undefined;
  readonly configurable: boolean | undefined;
}

function byKey(left: MemberShape, right: MemberShape): number {
  if (left.key < right.key) return -1;
  return left.key > right.key ? 1 : 0;
}

/** Member names and descriptor kinds of an object (prototype or class), sorted by key. */
function snapshotMembers(target: object): MemberShape[] {
  return Reflect.ownKeys(target)
    .map((key): MemberShape => {
      const descriptor = Reflect.getOwnPropertyDescriptor(target, key);
      return {
        key: String(key),
        getter: typeof descriptor?.get === 'function',
        setter: typeof descriptor?.set === 'function',
        valueType: typeof descriptor?.value,
        enumerable: descriptor?.enumerable,
        writable: descriptor?.writable,
        configurable: descriptor?.configurable,
      };
    })
    .sort(byKey);
}

/** Runs fn and returns the thrown error as `Name: message`, with generation numbers normalised. */
function captureError(fn: () => unknown): string {
  try {
    fn();
    return NO_ERROR;
  } catch (error) {
    const text = error instanceof Error ? `${error.name}: ${error.message}` : String(error);
    return text.replace(/generation \d+/g, 'generation N');
  }
}

interface Guards {
  readonly rowTwoMessage: string;
  readonly messageAfterFree: string;
  readonly numericSetterAfterFree: string;
  readonly stringSetterAfterFree: string;
  readonly numericGetterAfterFree: string;
  readonly stringGetterAfterFree: string;
}

interface CellSnapshot {
  readonly root: Record<string, unknown>;
  readonly child: Record<string, unknown>;
  readonly overflow: Record<string, unknown>;
  readonly writeIndexes: readonly unknown[];
  readonly overflowSharesIdentity: boolean;
  readonly statics: Record<string, unknown>;
  readonly instanceMembers: readonly MemberShape[];
  readonly classMembers: readonly MemberShape[];
  readonly guards: Guards;
}

async function freshAllocator(): Promise<WasmAllocator> {
  const allocator = await createWasmAllocator({ capacity: CAPACITY });
  allocator.reset();
  allocator.setThreadId(0, 42); // thread_id = 42n
  return allocator;
}

/**
 * Builds root, child and chained overflow buffers for one matrix cell and
 * snapshots them. Schema, op metadata, class generation and construction all
 * happen inside `mode`, so each cell is generated by that materializer only.
 */
function buildCell(
  allocator: WasmAllocator,
  mode: MaterializerMode,
  family: MessageLayoutFamily,
  physical: MessagePhysicalLayout,
  storage: Storage,
): CellSnapshot {
  return withMode(mode, () => {
    const schema = makeSchema(storage);
    const metadata = storage === 'preallocated' ? preallocatedMetadata(schema) : createTestOpMetadata();
    const eagerColumns = metadata._physicalLayoutPlan?.eagerColumns ?? resolveEagerColumns(schema);
    const SpanBufferClass = getWasmSpanBufferClass(schema, family, physical, eagerColumns);
    SpanBufferClass.stats.totalWrites = 0;
    SpanBufferClass.stats.spansCreated = 0;

    const traceRoot = new WasmTraceRoot(allocator, createTraceId('wasm-materializer-parity'), TEST_TRACER);
    const options = { allocator, capacity: CAPACITY, messageLayoutFamily: family, messagePhysicalLayout: physical };
    const root = createWasmSpanBuffer(schema, options, traceRoot, EMPTY_SCOPE, metadata, metadata);
    fillBuffer(root, family);
    const child = createWasmChildSpanBuffer(root, options, traceRoot, EMPTY_SCOPE, metadata, metadata);
    fillBuffer(child, family);
    const overflow = createWasmOverflowBuffer(root, traceRoot, EMPTY_SCOPE, metadata, metadata);
    fillBuffer(overflow, family);
    callMethod(root, '_sealStatsChain');

    const proto = Reflect.getPrototypeOf(root);
    if (proto === null) throw new Error('WASM SpanBuffer instance has no prototype');

    const statics: Record<string, unknown> = {
      schemaIsInput: SpanBufferClass.schema === schema,
      messageLayoutFamily: SpanBufferClass.messageLayoutFamily,
      messagePhysicalLayout: SpanBufferClass.messagePhysicalLayout,
      eagerColumns: [...SpanBufferClass.eagerColumns.names],
      stats: {
        capacity: SpanBufferClass.stats.capacity,
        totalWrites: SpanBufferClass.stats.totalWrites,
        spansCreated: SpanBufferClass.stats.spansCreated,
      },
    };

    const rootSnapshot = snapshotBuffer(root);
    const childSnapshot = snapshotBuffer(child);
    const overflowSnapshot = snapshotBuffer(overflow);
    const writeIndexes = [
      Reflect.get(root, '_writeIndex'),
      Reflect.get(child, '_writeIndex'),
      Reflect.get(overflow, '_writeIndex'),
    ];
    const overflowSharesIdentity = overflow._identityPtr === root._identityPtr;
    const instanceMembers = snapshotMembers(proto);
    const classMembers = snapshotMembers(SpanBufferClass);

    // Static-only rejects row 2 before writing; other families accept it.
    const rowTwoMessage = captureError(() => callMethod(root, 'message', 2, 'guard-row'));
    root.free();

    const guards: Guards = {
      rowTwoMessage,
      messageAfterFree: captureError(() => callMethod(root, 'message', 0, 'after-free')),
      numericSetterAfterFree: captureError(() => callMethod(root, 'duration', 0, 1)),
      stringSetterAfterFree: captureError(() => callMethod(root, 'userId', 0, 'after-free')),
      numericGetterAfterFree: captureError(() => Reflect.get(root, 'duration_values')),
      stringGetterAfterFree: captureError(() => Reflect.get(root, 'userId_values')),
    };

    return {
      root: rootSnapshot,
      child: childSnapshot,
      overflow: overflowSnapshot,
      writeIndexes,
      overflowSharesIdentity,
      statics,
      instanceMembers,
      classMembers,
      guards,
    };
  });
}

describe('WASM SpanBuffer materializer parity (compiled vs closure-composed)', () => {
  for (const family of FAMILIES) {
    for (const physical of PHYSICALS) {
      for (const storage of STORAGES) {
        it(`root, child and chained overflow are observably identical: ${family} x ${physical} x ${storage}`, async () => {
          const compiled = buildCell(await freshAllocator(), 'compiled', family, physical, storage);
          const closure = buildCell(await freshAllocator(), 'closure', family, physical, storage);
          expect(closure).toStrictEqual(compiled);

          // Anti-vacuity: the compiled cell really ran the script and hit its guards.
          expect(compiled.writeIndexes).toEqual([WRITE_INDEX, WRITE_INDEX, WRITE_INDEX]);
          expect(compiled.overflowSharesIdentity).toBe(true);
          expect(compiled.guards.rowTwoMessage === NO_ERROR).toBe(family !== 'static-only');
          if (family === 'static-only') expect(compiled.guards.rowTwoMessage).toContain('Static-only buffers');
          for (const message of [
            compiled.guards.messageAfterFree,
            compiled.guards.numericSetterAfterFree,
            compiled.guards.stringSetterAfterFree,
            compiled.guards.numericGetterAfterFree,
            compiled.guards.stringGetterAfterFree,
          ]) {
            expect(message).toContain('released');
          }
        });
      }
    }
  }
});

describe('materializer mode selects distinct, mode-keyed WASM SpanBuffer classes', () => {
  it('caches one class per mode and restores the default when the override is cleared', () => {
    const schema = makeSchema('lazy');
    const eagerColumns = resolveEagerColumns(schema);
    const classFor = (mode: MaterializerMode) =>
      withMode(mode, () => getWasmSpanBufferClass(schema, 'mixed', 'current', eagerColumns));

    const compiled = classFor('compiled');
    const closure = classFor('closure');
    expect(closure).not.toBe(compiled);
    expect(classFor('compiled')).toBe(compiled);
    expect(classFor('closure')).toBe(closure);

    expect(withMode('closure', () => activeMaterializerMode())).toBe('closure');
    setMaterializerModeOverride(undefined);
    expect(activeMaterializerMode()).toBe('compiled');
    expect(getWasmSpanBufferClass(schema, 'mixed', 'current', eagerColumns)).toBe(compiled);
  });
});
