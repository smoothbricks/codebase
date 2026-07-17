/**
 * Parity suite: SpanBuffer classes built by arrow-builder's compiled
 * (new Function) materializer versus the closure-composed (no-eval)
 * materializer MUST be byte-identical in observable behavior. The closure
 * path — driven by the closureInit/closureMethods extension counterparts in
 * spanBuffer.ts — is what production workerd runs, so any divergence here is
 * a production bug.
 *
 * Non-deterministic state is normalized: span_id bytes inside _identity
 * (global counter), thread id (compared for equality of layout, not value —
 * it IS process-stable so compared directly), wall-clock timestamps are
 * written explicitly so lanes stay deterministic.
 */

import { afterEach, describe, expect, it } from 'bun:test';
import { type MaterializerMode, setMaterializerModeOverride } from '@smoothbricks/arrow-builder';
import { createOpMetadata } from '../opContext/defineOp.js';
import type { MessageLayoutFamily, MessagePhysicalLayout } from '../runtimeHint.js';
import { S } from '../schema/builder.js';
import { LogSchema } from '../schema/LogSchema.js';
import { mergeWithSystemSchema } from '../schema/systemSchema.js';
import { createChildSpanBuffer, createOverflowBuffer, createSpanBuffer, getSpanBufferClass } from '../spanBuffer.js';
import type { AnySpanBuffer } from '../types.js';
import { createTestTraceRoot } from './test-helpers.js';

afterEach(() => {
  setMaterializerModeOverride(undefined);
});

function withMode<T>(mode: MaterializerMode, fn: () => T): T {
  setMaterializerModeOverride(mode);
  try {
    return fn();
  } finally {
    setMaterializerModeOverride(undefined);
  }
}

const OP_METADATA = createOpMetadata('parity-op', '@test/parity', 'parity.test.ts', 'deadbeef', 7);

const FAMILIES: readonly MessageLayoutFamily[] = ['mixed', 'static-only', 'dynamic-only'];
const PHYSICALS: readonly MessagePhysicalLayout[] = ['current', 'specialized', 'packed'];

function makeSchema(): LogSchema {
  return new LogSchema(
    mergeWithSystemSchema({
      userId: S.category(),
      operation: S.enum(['SELECT', 'INSERT', 'UPDATE', 'DELETE']),
      duration: S.number(),
      success: S.boolean(),
    }),
  );
}

function callMethod(target: object, name: string, ...args: unknown[]): unknown {
  const method = Reflect.get(target, name);
  if (typeof method !== 'function') throw new Error(`Missing method: ${name}`);
  return Reflect.apply(method, target, args);
}

/** Deterministic write script driving generated setters and system lanes. */
function fillBuffer(buffer: AnySpanBuffer): void {
  callMethod(buffer, 'message', 0, 'span-name');
  if (buffer._messageLayoutFamily === 'static-only') callMethod(buffer, 'message', 1, 'terminal');
  else callMethod(buffer, 'message', 2, 'log-entry');
  buffer.timestamp[0] = 1000n;
  buffer.timestamp[2] = 2000n;
  if (buffer.entry_type) buffer.entry_type[2] = 5;
  if (buffer._rowHeaders) buffer._rowHeaders[2] = (7 << 8) | 5;
  if (buffer._messageIds) buffer._messageIds[2] = 3;
  if (buffer._logHeaders) buffer._logHeaders[2] = 9;
  callMethod(buffer, 'userId', 2, 'alice');
  callMethod(buffer, 'userId', 3, 'bob');
  callMethod(buffer, 'operation', 2, 1);
  callMethod(buffer, 'duration', 2, 12.5);
  callMethod(buffer, 'success', 2, true);
  callMethod(buffer, 'success', 3, false);
  callMethod(buffer, 'duration', 3, null);
  callMethod(buffer, 'line', 2, 42);
  callMethod(buffer, 'uint64_value', 2, 99n);
  buffer._writeIndex = 4;
}

function snapshotTypedArray(view: unknown): unknown {
  if (view === undefined || view === null) return view;
  if (ArrayBuffer.isView(view) && !(view instanceof DataView)) {
    return {
      ctor: view.constructor.name,
      bytes: Array.from(new Uint8Array(view.buffer, view.byteOffset, view.byteLength)),
    };
  }
  return { unexpected: String(view) };
}

const SCHEMA_COLUMNS = ['userId', 'operation', 'duration', 'success', 'line', 'uint64_value', 'error_code'] as const;

/** Normalized observable state; span-id bytes in _identity are zeroed (global counter). */
function snapshotBuffer(buffer: AnySpanBuffer): Record<string, unknown> {
  const identity = Array.from(buffer._identity);
  for (let i = 8; i < 12 && i < identity.length; i++) identity[i] = 0;
  const snapshot: Record<string, unknown> = {
    capacity: buffer._capacity,
    writeIndex: buffer._writeIndex,
    identity,
    threadId: buffer.thread_id,
    traceId: buffer.trace_id,
    spanStartTime: buffer._spanStartTime,
    lastLoggedTime: buffer._lastLoggedTime,
    hasParent: Reflect.get(buffer, '_hasParent'),
    parentThreadId: buffer.parent_thread_id,
    statsSealed: buffer._statsSealed,
    statsReservedRows: buffer._statsReservedRows,
    scopeValues: buffer._scopeValues,
    spanName: buffer._spanName,
    terminalMessage: buffer._terminalMessage,
    messageValues: buffer.message_values ? [...buffer.message_values] : undefined,
    timestamp: snapshotTypedArray(buffer.timestamp),
    entryType: snapshotTypedArray(buffer.entry_type),
    messageIds: snapshotTypedArray(buffer._messageIds),
    logHeaders: snapshotTypedArray(buffer._logHeaders),
    rowHeaders: snapshotTypedArray(buffer._rowHeaders),
    systemByteLength: buffer._system.byteLength,
  };
  for (const column of SCHEMA_COLUMNS) {
    const values = buffer.getColumnIfAllocated(column);
    const nulls = buffer.getNullsIfAllocated(column);
    snapshot[`${column}.values`] = Array.isArray(values) ? [...values] : snapshotTypedArray(values);
    snapshot[`${column}.nulls`] = nulls === undefined ? undefined : Array.from(nulls);
  }
  return snapshot;
}

interface FamilySnapshots {
  root: Record<string, unknown>;
  child: Record<string, unknown>;
  chained: Record<string, unknown>;
  identitySharedWithRoot: boolean;
  prototypeShared: boolean;
  statics: Record<string, unknown>;
}

function buildAndSnapshot(
  mode: MaterializerMode,
  schema: LogSchema,
  family: MessageLayoutFamily,
  physical: MessagePhysicalLayout,
): FamilySnapshots {
  return withMode(mode, () => {
    const SpanBufferClass = getSpanBufferClass(schema, family, physical);
    SpanBufferClass.stats.capacity = 16;
    SpanBufferClass.stats.totalWrites = 0;
    SpanBufferClass.stats.spansCreated = 0;
    const traceRoot = createTestTraceRoot(`parity-${family}-${physical}`);

    const root = createSpanBuffer(schema, traceRoot, OP_METADATA, 16, SpanBufferClass);
    fillBuffer(root);
    const child = createChildSpanBuffer(root, SpanBufferClass, OP_METADATA, OP_METADATA, 16);
    fillBuffer(child);
    const chained = createOverflowBuffer(root);
    fillBuffer(chained);
    callMethod(root, '_sealStatsChain');

    return {
      root: snapshotBuffer(root),
      child: snapshotBuffer(child),
      chained: snapshotBuffer(chained),
      identitySharedWithRoot: chained._identity === root._identity,
      prototypeShared: Object.getPrototypeOf(root) === Object.getPrototypeOf(chained),
      statics: {
        family: SpanBufferClass.messageLayoutFamily,
        physical: SpanBufferClass.messagePhysicalLayout,
        totalWrites: SpanBufferClass.stats.totalWrites,
        spansCreated: SpanBufferClass.stats.spansCreated,
      },
    };
  });
}

describe('SpanBuffer materializer parity (compiled vs closure-composed)', () => {
  for (const family of FAMILIES) {
    for (const physical of PHYSICALS) {
      it(`root/child/chained buffers are byte-identical: ${family} x ${physical}`, () => {
        const schema = makeSchema();
        const compiled = buildAndSnapshot('compiled', schema, family, physical);
        const closure = buildAndSnapshot('closure', schema, family, physical);
        expect(closure).toEqual(compiled);
        // Anti-vacuity: the scripts really wrote rows and sealed stats.
        expect(closure.root.writeIndex).toBe(4);
        expect(closure.statics.totalWrites).toBeGreaterThan(0);
        expect(closure.identitySharedWithRoot).toBe(true);
        expect(closure.prototypeShared).toBe(true);
      });
    }
  }

  it('caches classes per materializer mode without cross-mode pollution', () => {
    const schema = makeSchema();
    const Compiled = withMode('compiled', () => getSpanBufferClass(schema));
    const Closure = withMode('closure', () => getSpanBufferClass(schema));
    expect(Compiled).not.toBe(Closure);
    expect(withMode('compiled', () => getSpanBufferClass(schema))).toBe(Compiled);
    expect(withMode('closure', () => getSpanBufferClass(schema))).toBe(Closure);
  });

  it('exposes identical extension-member descriptors on both prototypes', () => {
    const schema = makeSchema();
    const memberNames = [
      'span_id',
      'thread_id',
      'trace_id',
      '_spanStartTime',
      '_lastLoggedTime',
      '_hasParent',
      'parent_span_id',
      'parent_thread_id',
      'isParentOf',
      'isChildOf',
      'copyThreadIdTo',
      'copyParentThreadIdTo',
      '_logSchema',
      '_messageLayoutFamily',
      '_messagePhysicalLayout',
      '_columns',
      '_stats',
      '_sealStats',
      '_sealStatsChain',
      'getOrCreateOverflow',
      'message',
    ];
    const describeMembers = (proto: object) =>
      memberNames.map((name) => {
        const descriptor = Object.getOwnPropertyDescriptor(proto, name);
        if (!descriptor) return { name, missing: true };
        return {
          name,
          kind: descriptor.get ? 'getter' : typeof descriptor.value,
          enumerable: descriptor.enumerable,
          configurable: descriptor.configurable,
          writable: descriptor.writable,
        };
      });

    const compiled = withMode('compiled', () => describeMembers(getSpanBufferClass(schema).prototype));
    const closure = withMode('closure', () => describeMembers(getSpanBufferClass(schema).prototype));
    expect(closure).toEqual(compiled);
    expect(compiled.some((member) => 'missing' in member)).toBe(false);
  });

  it('enforces the static-only message row guard identically', () => {
    const schema = makeSchema();
    const messageFor = (mode: MaterializerMode) =>
      withMode(mode, () => {
        const SpanBufferClass = getSpanBufferClass(schema, 'static-only', 'current');
        const traceRoot = createTestTraceRoot(`static-guard-${mode}`);
        const buffer = createSpanBuffer(schema, traceRoot, OP_METADATA, 16, SpanBufferClass);
        try {
          callMethod(buffer, 'message', 2, 'not allowed');
        } catch (error) {
          return error instanceof Error ? `${error.constructor.name}: ${error.message}` : String(error);
        }
        throw new Error('expected the static-only message guard to throw');
      });
    expect(messageFor('closure')).toBe(messageFor('compiled'));
    expect(messageFor('closure')).toContain('rows 0 and 1');
  });
});
