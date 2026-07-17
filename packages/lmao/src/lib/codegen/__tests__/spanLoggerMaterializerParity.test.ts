/**
 * Materializer parity for generated SpanLogger classes.
 *
 * Drives identical logging scripts through the compiled (string codegen) and
 * closure-composed (no-eval) SpanLogger classes against real SpanBuffers and
 * byte-compares the resulting buffer contents and writer state, including
 * entry_type/timestamp lanes and message storage for every layout family and
 * physical message layout the generator distinguishes.
 *
 * The closure mode is forced via setMaterializerModeOverride('closure') and
 * restored in afterEach.
 */

import { afterEach, describe, expect, it } from 'bun:test';
import { createHash } from 'node:crypto';
import { setMaterializerModeOverride } from '@smoothbricks/arrow-builder';
import { createTestTraceRoot } from '../../__tests__/test-helpers.js';
import { defineOpContext } from '../../defineOpContext.js';
import type { OpMetadata } from '../../opContext/opTypes.js';
import type { OpContext } from '../../opContext/types.js';
import type { CallsitePlan } from '../../physicalLayoutPlan.js';
import {
  type MessageLayoutFamily,
  type MessagePhysicalLayout,
  RUNTIME_HINT_ANALYZED_VALID,
  RUNTIME_HINT_LOG,
  RUNTIME_HINT_MESSAGE_LAYOUT_DYNAMIC_ONLY,
  RUNTIME_HINT_MESSAGE_LAYOUT_MIXED,
  RUNTIME_HINT_MESSAGE_LAYOUT_STATIC_ONLY,
  RUNTIME_HINT_MESSAGE_PHYSICAL_PACKED,
  RUNTIME_HINT_MESSAGE_PHYSICAL_SPECIALIZED,
  RUNTIME_HINT_RESULT,
} from '../../runtimeHint.js';
import { S } from '../../schema/builder.js';
import { defineLogSchema } from '../../schema/defineLogSchema.js';
import type { LogSchema } from '../../schema/types.js';
import { createSpanBuffer } from '../../spanBuffer.js';
import type { AnySpanBuffer } from '../../types.js';
import { registerVocabularyFragment, type VocabularyFragment } from '../../vocabularyRegistry.js';
import type { WriterState } from '../fixedPositionWriterGenerator.js';
import { createSpanLoggerClass } from '../spanLoggerGenerator.js';

// =============================================================================
// Vocabulary fixture (same fragment scheme as message-buffer-families.test.ts)
// =============================================================================

const encoder = new TextEncoder();

function encodeRecord(text: string): Uint8Array {
  const textBytes = encoder.encode(text);
  const record = new Uint8Array(4 + textBytes.length + 2);
  new DataView(record.buffer).setUint32(0, textBytes.length, true);
  record.set(textBytes, 4);
  return record;
}

function fragmentHash(fragment: Omit<VocabularyFragment, 'contentHash'>): string {
  const algorithm = encoder.encode(fragment.idAlgorithm);
  const byteLength =
    1 +
    2 +
    algorithm.length +
    4 +
    fragment.ids.length * 4 +
    4 +
    fragment.kindTags.length +
    4 +
    fragment.utf8.length +
    4 +
    fragment.offsets.length * 4;
  const bytes = new Uint8Array(byteLength);
  const view = new DataView(bytes.buffer);
  let offset = 0;
  bytes[offset++] = fragment.schemaVersion;
  view.setUint16(offset, algorithm.length, true);
  offset += 2;
  bytes.set(algorithm, offset);
  offset += algorithm.length;
  view.setUint32(offset, fragment.ids.length, true);
  offset += 4;
  for (const id of fragment.ids) {
    view.setUint32(offset, id, true);
    offset += 4;
  }
  view.setUint32(offset, fragment.kindTags.length, true);
  offset += 4;
  bytes.set(fragment.kindTags, offset);
  offset += fragment.kindTags.length;
  view.setUint32(offset, fragment.utf8.length, true);
  offset += 4;
  bytes.set(fragment.utf8, offset);
  offset += fragment.utf8.length;
  view.setUint32(offset, fragment.offsets.length, true);
  offset += 4;
  for (const boundary of fragment.offsets) {
    view.setInt32(offset, boundary, true);
    offset += 4;
  }
  return createHash('sha256').update(bytes).digest('hex');
}

function makeFragment(text: string): VocabularyFragment {
  const record = encodeRecord(text);
  const digest = createHash('sha256').update(Uint8Array.of(1)).update(record).digest();
  const fragment: Omit<VocabularyFragment, 'contentHash'> = {
    schemaVersion: 1,
    idAlgorithm: 'sha256-24-v1',
    ids: new Uint32Array([(digest[0] << 16) | (digest[1] << 8) | digest[2]]),
    kindTags: new Uint8Array([1]),
    utf8: record,
    offsets: new Int32Array([0, record.length]),
  };
  return { ...fragment, contentHash: fragmentHash(fragment) };
}

const PARITY_BINDING = registerVocabularyFragment(makeFragment('parity local vocabulary literal'));
const DENSE_INDEX = PARITY_BINDING[0];

// =============================================================================
// Schemas, op contexts, and layout-specific callsite plans
// =============================================================================

const CAPACITY = 64;

const schema = defineLogSchema({
  userId: S.category(),
  count: S.number(),
  enabled: S.boolean(),
  note: S.text(),
  level: S.enum(['low', 'mid', 'high']),
});
const context = defineOpContext({ logSchema: schema });
const runtimeSchema = context.logBinding.logSchema;

const emptySchema = defineLogSchema({});
const emptyContext = defineOpContext({ logSchema: emptySchema });
const emptyRuntimeSchema = emptyContext.logBinding.logSchema;

const FAMILY_BITS: Record<MessageLayoutFamily, number> = {
  mixed: RUNTIME_HINT_MESSAGE_LAYOUT_MIXED,
  'static-only': RUNTIME_HINT_MESSAGE_LAYOUT_STATIC_ONLY,
  'dynamic-only': RUNTIME_HINT_MESSAGE_LAYOUT_DYNAMIC_ONLY,
};
const PHYSICAL_BITS: Record<MessagePhysicalLayout, number> = {
  current: 0,
  specialized: RUNTIME_HINT_MESSAGE_PHYSICAL_SPECIALIZED,
  packed: RUNTIME_HINT_MESSAGE_PHYSICAL_PACKED,
};

interface ParityCase {
  readonly name: string;
  readonly family: MessageLayoutFamily;
  readonly physical: MessagePhysicalLayout;
  readonly dictionary: boolean;
}

const cases: readonly ParityCase[] = [
  { name: 'mixed/current', family: 'mixed', physical: 'current', dictionary: true },
  { name: 'mixed/current (raw fallback, empty dictionary)', family: 'mixed', physical: 'current', dictionary: false },
  { name: 'mixed/specialized', family: 'mixed', physical: 'specialized', dictionary: true },
  { name: 'mixed/packed', family: 'mixed', physical: 'packed', dictionary: true },
  { name: 'static-only/current', family: 'static-only', physical: 'current', dictionary: true },
  { name: 'static-only/specialized', family: 'static-only', physical: 'specialized', dictionary: true },
  { name: 'static-only/packed', family: 'static-only', physical: 'packed', dictionary: true },
  { name: 'dynamic-only/current', family: 'dynamic-only', physical: 'current', dictionary: false },
  { name: 'dynamic-only/packed', family: 'dynamic-only', physical: 'packed', dictionary: false },
];

function parityOpOptions(parityCase: ParityCase): { runtimeHint: number; localMessageDictionary?: number[] } {
  return {
    runtimeHint:
      RUNTIME_HINT_ANALYZED_VALID |
      FAMILY_BITS[parityCase.family] |
      PHYSICAL_BITS[parityCase.physical] |
      RUNTIME_HINT_LOG |
      RUNTIME_HINT_RESULT |
      CAPACITY,
    ...(parityCase.dictionary ? { localMessageDictionary: [DENSE_INDEX] } : {}),
  };
}

const opsByCase = new Map(
  cases.map((parityCase) => [
    parityCase.name,
    context.defineOp(`parity-${parityCase.name}`, (op) => op.ok(null), undefined, parityOpOptions(parityCase)),
  ]),
);

// =============================================================================
// Scenario driver
// =============================================================================

/** Structural view of the generated logger surface exercised by this suite. */
interface ParityLogger {
  [key: string]: unknown;
  info(message: string, fields?: Record<string, unknown>): ParityLogger;
  debug(message: string): ParityLogger;
  warn(message: string, fields?: Record<string, unknown>): ParityLogger;
  error(message: string, fields?: Record<string, unknown>): ParityLogger;
  trace(message: string): ParityLogger;
  _infoTemplate(vocabularyIndex: number): ParityLogger;
  _debugTemplate(vocabularyIndex: number): ParityLogger;
  _warnTemplate(vocabularyIndex: number): ParityLogger;
  _errorTemplate(vocabularyIndex: number): ParityLogger;
  _traceTemplate(vocabularyIndex: number): ParityLogger;
  with(attributes: Record<string, unknown>): ParityLogger;
  ffAccess(flagName: string, value: unknown): void;
  ffUsage(flagName: string, context?: Record<string, unknown>): ParityLogger;
  line(lineNumber: number): ParityLogger;
  error_code(code: string): ParityLogger;
  exception_stack(stack: string): ParityLogger;
  ff_value(value: string): ParityLogger;
  uint64_value(value: bigint): ParityLogger;
  userId(value: string): ParityLogger;
  count(value: number): ParityLogger;
  enabled(value: boolean): ParityLogger;
  note(value: string): ParityLogger;
  level(value: string): ParityLogger;
  readonly scope: Record<string, unknown> | undefined;
  _setScope(attributes: Record<string, unknown>): void;
  _prefillScopedAttributesOn(buffer: AnySpanBuffer): void;
}

type MaterializerChoice = 'compiled' | 'closure';

/** Runtime objects flow untyped out of Reflect/constructors; every real one is an object. */
function isWriterStateValue(value: unknown): value is WriterState {
  return typeof value === 'object' && value !== null;
}

function isParityLogger(value: unknown): value is ParityLogger {
  return typeof value === 'object' && value !== null;
}

function toParityLogger(value: unknown): ParityLogger {
  if (isParityLogger(value)) return value;
  throw new TypeError('Expected a SpanLogger instance');
}

/** The op surface the scenarios consume (metadata + resolved callsite plan). */
interface ParityOpHandle<T extends LogSchema, Ctx extends OpContext<T>> {
  readonly metadata: OpMetadata;
  readonly callsitePlan: CallsitePlan<T, Ctx>;
}

interface ScenarioSetup {
  readonly buffer: AnySpanBuffer;
  readonly state: WriterState;
  readonly logger: ParityLogger;
}

function setUpScenario<T extends LogSchema, Ctx extends OpContext<T>>(
  op: ParityOpHandle<T, Ctx>,
  rtSchema: T,
  parityCase: Pick<ParityCase, 'family' | 'physical'>,
  mode: MaterializerChoice,
  eagerColumns: readonly string[] = [],
): ScenarioSetup {
  op.callsitePlan.SpanBufferClass.stats.capacity = CAPACITY;
  const buffer = createSpanBuffer(
    rtSchema,
    createTestTraceRoot('parity-trace'),
    op.metadata,
    CAPACITY,
    op.callsitePlan.SpanBufferClass,
  );
  const spanContext = new op.callsitePlan.SpanContextClass(buffer, rtSchema, op.callsitePlan);
  op.callsitePlan.appenders.writeSpanStart(buffer, parityCase.family === 'static-only' ? DENSE_INDEX : 'parity root');
  const state = Reflect.get(spanContext._spanLogger, '_state');
  if (!isWriterStateValue(state)) throw new TypeError('Expected writer state on the span logger');

  if (mode === 'closure') setMaterializerModeOverride('closure');
  try {
    const LoggerClass = createSpanLoggerClass(rtSchema, parityCase.family, parityCase.physical, eagerColumns);
    return { buffer, state, logger: toParityLogger(new LoggerClass(state)) };
  } finally {
    setMaterializerModeOverride(undefined);
  }
}

/** Identical logging script for both materializers (schema with fields). */
function driveFieldSchema(setup: ScenarioSetup, parityCase: Pick<ParityCase, 'family' | 'physical'>): void {
  const { logger, buffer } = setup;
  const { family, physical } = parityCase;

  if (family === 'static-only') {
    const staticThrow = 'Dynamic log write reached a static-only callsite plan';
    expect(() => logger.info('nope')).toThrow(staticThrow);
    expect(() => logger.debug('nope')).toThrow(staticThrow);
    expect(() => logger.warn('nope')).toThrow(staticThrow);
    expect(() => logger.error('nope')).toThrow(staticThrow);
    expect(() => logger.trace('nope')).toThrow(staticThrow);
    expect(() => logger.ffAccess('flag-a', true)).toThrow('Feature flag write reached a static-only callsite plan');
    expect(() => logger.ffUsage('flag-b')).toThrow('Feature flag write reached a static-only callsite plan');
  } else {
    logger.info('parity {{braced}} info').line(11).error_code('E-1');
    logger.debug('parity debug');
    logger.warn('parity warn', { userId: 'warn-user', count: 7 });
    logger.error('parity error', { enabled: true });
    logger.trace('parity trace');
    logger.info('fields', { note: 'field-note' });
    logger.exception_stack('stack-1');
    logger.ff_value('ff-raw');
    logger.uint64_value(12345678901234567890n);
    logger.with({ userId: 'with-user', level: 'high', nullish: null, missing: undefined });
    logger.ffAccess('flag-a', true);
    logger.ffAccess('flag-null', null);
    logger.ffUsage('flag-b', { count: 3 });
    logger.ffUsage('flag-c');
  }

  if (family !== 'dynamic-only') {
    logger._infoTemplate(DENSE_INDEX).userId('tpl-user').count(42).enabled(true).note('tpl-note').level('mid');
    logger._debugTemplate(DENSE_INDEX);
    logger._warnTemplate(DENSE_INDEX).line(77);
    logger._errorTemplate(DENSE_INDEX);
    logger._traceTemplate(DENSE_INDEX);
    if (physical === 'packed') {
      expect(() => logger._infoTemplate(0x00ffffff)).toThrow('Packed message dense index exceeds 0xFFFFFE');
    }
  } else {
    const dynamicThrow = 'Static log write reached a dynamic-only callsite plan';
    expect(() => logger._infoTemplate(DENSE_INDEX)).toThrow(dynamicThrow);
    expect(() => logger._warnTemplate(DENSE_INDEX)).toThrow(dynamicThrow);
  }

  logger._setScope({ userId: 'scope-user', count: 5, enabled: false, note: 'scope-note', level: 'low' });
  logger._setScope({ note: null, count: 6 });
  expect(logger.scope).toEqual({ userId: 'scope-user', count: 6, enabled: false, level: 'low' });
  logger._prefillScopedAttributesOn(buffer);
}

// =============================================================================
// Buffer snapshots (timestamps normalized to written/unwritten flags)
// =============================================================================

/** Lanes exposed via prototype getters that an own-property walk would miss. */
const EXPLICIT_LANES = ['timestamp', 'entry_type', '_rowHeaders', '_messageIds', '_logHeaders', 'message_values'];
/** Own properties whose bytes embed per-buffer random identity (span/trace ids). */
const RANDOM_IDENTITY_KEYS = new Set(['_system', '_identity']);

function isArrayLikeView(value: ArrayBufferView): value is ArrayBufferView & ArrayLike<unknown> {
  return typeof Reflect.get(value, 'length') === 'number';
}

function snapshotValue(value: unknown): unknown {
  if (ArrayBuffer.isView(value) && !(value instanceof DataView) && isArrayLikeView(value)) {
    return Array.from(value);
  }
  if (Array.isArray(value)) return value.slice();
  const type = typeof value;
  if (value === null || type === 'number' || type === 'string' || type === 'boolean' || type === 'bigint') {
    return value;
  }
  return undefined;
}

function isDynamicRecord(value: object): value is Record<string, unknown> {
  return value !== null;
}

function snapshotBuffer(buffer: AnySpanBuffer): Record<string, unknown> {
  if (!isDynamicRecord(buffer)) throw new TypeError('Expected a buffer object');
  const dyn = buffer;

  // Timestamps are wall-clock: assert per-row written-ness, then zero them so
  // every remaining lane byte-compares exactly.
  const timestamps = buffer.timestamp;
  const timestampWritten: boolean[] = [];
  for (let i = 0; i < timestamps.length; i++) {
    timestampWritten.push(timestamps[i] !== 0n);
    timestamps[i] = 0n;
  }

  const snap: Record<string, unknown> = { timestampWritten };
  for (const key of EXPLICIT_LANES) {
    snap[key] = snapshotValue(dyn[key]);
  }
  for (const key of Object.keys(dyn).sort()) {
    if (RANDOM_IDENTITY_KEYS.has(key)) continue;
    if (key === '_scopeValues') {
      const scope = dyn[key];
      snap[key] = scope && typeof scope === 'object' ? { ...scope } : scope;
      continue;
    }
    const copied = snapshotValue(dyn[key]);
    if (copied !== undefined) snap[key] = copied;
  }
  return snap;
}

function runFieldSchemaScenario(parityCase: ParityCase, mode: MaterializerChoice): Record<string, unknown> {
  const op = opsByCase.get(parityCase.name);
  if (op === undefined) throw new Error(`No op defined for parity case ${parityCase.name}`);
  const setup = setUpScenario(op, runtimeSchema, parityCase, mode);
  driveFieldSchema(setup, parityCase);
  expect(setup.buffer._overflow).toBeUndefined();
  expect(setup.state._buffer).toBe(setup.buffer);
  // Every script appends rows beyond span-start/end, so the snapshot must
  // capture written timestamp lanes — guards against a vacuous comparison.
  expect(setup.buffer._writeIndex).toBeGreaterThan(2);
  const snapshot = snapshotBuffer(setup.buffer);
  const timestampWritten = snapshot.timestampWritten;
  expect(Array.isArray(timestampWritten) && timestampWritten.includes(true)).toBe(true);
  return snapshot;
}

// =============================================================================
// Suite
// =============================================================================

describe('SpanLogger materializer parity (compiled vs closure)', () => {
  afterEach(() => {
    setMaterializerModeOverride(undefined);
  });

  for (const parityCase of cases) {
    it(`produces byte-identical buffers for ${parityCase.name}`, () => {
      const compiled = runFieldSchemaScenario(parityCase, 'compiled');
      const closure = runFieldSchemaScenario(parityCase, 'closure');
      expect(closure).toEqual(compiled);
    });
  }

  it('produces byte-identical buffers with preallocated (eager) columns', () => {
    const parityCase = cases[0];
    const op = opsByCase.get(parityCase.name);
    if (op === undefined) throw new Error('Missing mixed/current op');
    const eager = ['userId', 'count'] as const;

    const run = (mode: MaterializerChoice): Record<string, unknown> => {
      const setup = setUpScenario(op, runtimeSchema, parityCase, mode, eager);
      // Warm the raw lanes through a lazy-mode logger so the preallocated
      // setters (which write `_userId_values`/`_count_values` directly) hit
      // allocated storage in both runs.
      const warm = toParityLogger(new (createSpanLoggerClass(runtimeSchema))(setup.state));
      warm.userId('warm-user').count(1);
      driveFieldSchema(setup, parityCase);
      return snapshotBuffer(setup.buffer);
    };

    const compiled = run('compiled');
    const closure = run('closure');
    expect(closure).toEqual(compiled);
  });

  it('produces byte-identical buffers for the empty schema', () => {
    const parityCase: ParityCase = { name: 'empty', family: 'mixed', physical: 'current', dictionary: false };
    const op = emptyContext.defineOp('parity-empty', (opCtx) => opCtx.ok(null), undefined, parityOpOptions(parityCase));

    const run = (mode: MaterializerChoice): Record<string, unknown> => {
      const setup = setUpScenario(op, emptyRuntimeSchema, parityCase, mode);
      const { logger, buffer } = setup;
      logger.info('empty {{schema}} info').line(3);
      logger.debug('empty debug');
      logger.warn('empty warn');
      logger.error('empty error').error_code('E-9').exception_stack('stack-9');
      logger.trace('empty trace');
      logger._infoTemplate(DENSE_INDEX);
      logger.ffAccess('flag-a', 42);
      logger.ffUsage('flag-b');
      logger.ff_value('ff-raw').uint64_value(7n);
      logger.with({ unknownField: 'ignored' });
      logger._setScope({ foo: 'bar' });
      logger._prefillScopedAttributesOn(buffer);
      return snapshotBuffer(buffer);
    };

    const compiled = run('compiled');
    const closure = run('closure');
    expect(closure).toEqual(compiled);
  });

  it('matches the compiled prototype member contract and instance shape', () => {
    const CompiledClass = createSpanLoggerClass(runtimeSchema, 'mixed', 'current');
    setMaterializerModeOverride('closure');
    const ClosureClass = createSpanLoggerClass(runtimeSchema, 'mixed', 'current');
    setMaterializerModeOverride(undefined);

    expect(ClosureClass).not.toBe(CompiledClass);
    expect(new Set(Reflect.ownKeys(ClosureClass.prototype))).toEqual(new Set(Reflect.ownKeys(CompiledClass.prototype)));

    for (const key of Reflect.ownKeys(CompiledClass.prototype)) {
      if (key === 'constructor') continue;
      const compiledDescriptor = Object.getOwnPropertyDescriptor(CompiledClass.prototype, key);
      const closureDescriptor = Object.getOwnPropertyDescriptor(ClosureClass.prototype, key);
      if (compiledDescriptor === undefined || closureDescriptor === undefined) {
        throw new Error(`Missing prototype member ${String(key)}`);
      }
      expect(closureDescriptor.enumerable).toBe(compiledDescriptor.enumerable);
      expect(closureDescriptor.configurable).toBe(compiledDescriptor.configurable);
      expect(closureDescriptor.writable).toBe(compiledDescriptor.writable);
      expect(typeof closureDescriptor.value).toBe(typeof compiledDescriptor.value);
      expect(typeof closureDescriptor.get).toBe(typeof compiledDescriptor.get);
      expect(typeof closureDescriptor.set).toBe(typeof compiledDescriptor.set);
    }

    const op = opsByCase.get(cases[0].name);
    if (op === undefined) throw new Error('Missing mixed/current op');
    const setup = setUpScenario(op, runtimeSchema, cases[0], 'closure');
    expect(Reflect.ownKeys(setup.logger as object)).toEqual(['_state']);
  });
});
