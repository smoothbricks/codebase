/**
 * Layout-derived span lifecycle writers, installed once on each SpanBuffer
 * class prototype.
 *
 * Definition time owns schema and layout; tracer time owns the physical
 * backend. The buffer class is where the two axes meet: a generated js-heap or
 * wasm class encodes its message layout here, and the thread lane's
 * ThreadSpanView carries its own binding-backed writers. Every lifecycle write
 * therefore reads `buffer._appenders` / `buffer._appendLogEntry` — no per-span
 * backend resolution exists anywhere.
 */

import { decodeVocabularyMessage, MAX_PACKED_MESSAGE_DENSE_INDEX } from './resolveMessage.js';
import type { MessageLayoutFamily, MessagePhysicalLayout } from './runtimeHint.js';
import { ENTRY_TYPE_SPAN_EXCEPTION, ENTRY_TYPE_SPAN_START } from './schema/systemSchema.js';
import { consumeSpanStartedAtAllocation, type TimestampAppendPrimitive } from './traceRoot.js';
import type { AnySpanBuffer } from './types.js';

export interface PhysicalAppenders {
  writeSpanStart(buffer: AnySpanBuffer, name: string | number): void;
  writeSpanEnd(buffer: AnySpanBuffer, entryType: number): void;
  writeLogEntry(buffer: AnySpanBuffer, entryType: number): number;
}

export const SPLIT_APPEND_LOG_ENTRY: TimestampAppendPrimitive = (traceRoot, buffer, entryType) =>
  traceRoot._appendLogEntry(traceRoot, buffer, entryType);

export const PACKED_APPEND_LOG_ENTRY: TimestampAppendPrimitive = (traceRoot, buffer, entryType) => {
  const row = buffer._writeIndex;
  const headers = buffer._rowHeaders;
  if (headers === undefined) throw new TypeError('Packed layout is missing row headers');
  buffer.timestamp[row] = traceRoot._timestampNow(traceRoot);
  headers[row] = entryType;
  buffer._writeIndex = row + 1;
  return row;
};

const CURRENT_BASE_APPENDERS = {
  writeSpanEnd(buffer: AnySpanBuffer, entryType: number): void {
    const traceRoot = buffer._traceRoot;
    traceRoot._writeSpanEnd(traceRoot, buffer, entryType);
  },
  writeLogEntry(buffer: AnySpanBuffer, entryType: number): number {
    return SPLIT_APPEND_LOG_ENTRY(buffer._traceRoot, buffer, entryType);
  },
};

function initializeCurrentSpan(buffer: AnySpanBuffer): Uint8Array {
  const entryTypes = buffer.entry_type;
  if (entryTypes === undefined) throw new TypeError('Current layout is missing entry types');
  if (!consumeSpanStartedAtAllocation(buffer)) {
    const traceRoot = buffer._traceRoot;
    buffer.timestamp[0] = traceRoot._timestampNow(traceRoot);
    entryTypes[0] = ENTRY_TYPE_SPAN_START;
    entryTypes[1] = ENTRY_TYPE_SPAN_EXCEPTION;
    buffer.timestamp[1] = 0n;
    buffer._writeIndex = 2;
  }
  return entryTypes;
}

const CURRENT_MIXED_APPENDERS: PhysicalAppenders = Object.freeze({
  ...CURRENT_BASE_APPENDERS,
  writeSpanStart(buffer: AnySpanBuffer, name: string | number): void {
    initializeCurrentSpan(buffer);
    if (typeof name === 'string') {
      buffer.message(0, name);
      return;
    }
    const localId = buffer._opMetadata._physicalLayoutPlan?.encodeLocalMessage(name) ?? 0;
    if (localId === 0) {
      const rawMessages = buffer.message_values;
      if (rawMessages === undefined) throw new TypeError('Current mixed layout is missing raw message storage');
      rawMessages[0] = decodeVocabularyMessage(buffer._vocabularyGeneration, name);
    } else {
      const messageIds = buffer._messageIds;
      if (messageIds === undefined) throw new TypeError('Current mixed layout is missing local message storage');
      messageIds[0] = localId;
    }
  },
});

const CURRENT_STATIC_APPENDERS: PhysicalAppenders = Object.freeze({
  ...CURRENT_BASE_APPENDERS,
  writeSpanStart(buffer: AnySpanBuffer, name: string | number): void {
    initializeCurrentSpan(buffer);
    if (typeof name === 'string') {
      buffer._spanName = name;
      return;
    }
    const localId = buffer._opMetadata._physicalLayoutPlan?.encodeLocalMessage(name) ?? 0;
    if (localId === 0) {
      buffer._spanName = name;
      return;
    }
    const messageIds = buffer._messageIds;
    if (messageIds === undefined) throw new TypeError('Current static layout is missing local message storage');
    messageIds[0] = localId;
  },
});

const CURRENT_DYNAMIC_APPENDERS: PhysicalAppenders = Object.freeze({
  ...CURRENT_BASE_APPENDERS,
  writeSpanStart(buffer: AnySpanBuffer, name: string | number): void {
    initializeCurrentSpan(buffer);
    buffer._spanName = name;
  },
});

const SPLIT_MIXED_APPENDERS: PhysicalAppenders = Object.freeze({
  writeSpanStart(buffer: AnySpanBuffer, name: string | number): void {
    if (typeof name === 'number') {
      const entryTypes = buffer.entry_type;
      const headers = buffer._logHeaders;
      if (entryTypes === undefined || headers === undefined) throw new TypeError('Split mixed layout is incomplete');
      if (!consumeSpanStartedAtAllocation(buffer)) {
        const traceRoot = buffer._traceRoot;
        buffer.timestamp[0] = traceRoot._timestampNow(traceRoot);
        entryTypes[0] = ENTRY_TYPE_SPAN_START;
        entryTypes[1] = ENTRY_TYPE_SPAN_EXCEPTION;
        buffer.timestamp[1] = 0n;
        buffer._writeIndex = 2;
      }
      headers[0] = name + 1;
      return;
    }
    const traceRoot = buffer._traceRoot;
    traceRoot._writeSpanStart(traceRoot, buffer, name);
  },
  writeSpanEnd(buffer: AnySpanBuffer, entryType: number): void {
    const traceRoot = buffer._traceRoot;
    traceRoot._writeSpanEnd(traceRoot, buffer, entryType);
  },
  writeLogEntry(buffer: AnySpanBuffer, entryType: number): number {
    return SPLIT_APPEND_LOG_ENTRY(buffer._traceRoot, buffer, entryType);
  },
});

const SPLIT_STATIC_APPENDERS: PhysicalAppenders = Object.freeze({
  ...SPLIT_MIXED_APPENDERS,
  writeSpanStart(buffer: AnySpanBuffer, name: string | number): void {
    const entryTypes = buffer.entry_type;
    const headers = buffer._logHeaders;
    if (entryTypes === undefined || headers === undefined) throw new TypeError('Split static layout is incomplete');
    if (!consumeSpanStartedAtAllocation(buffer)) {
      const traceRoot = buffer._traceRoot;
      buffer.timestamp[0] = traceRoot._timestampNow(traceRoot);
      entryTypes[0] = ENTRY_TYPE_SPAN_START;
      entryTypes[1] = ENTRY_TYPE_SPAN_EXCEPTION;
      buffer.timestamp[1] = 0n;
      buffer._writeIndex = 2;
    }
    if (typeof name === 'number') {
      headers[0] = name + 1;
    } else {
      buffer._spanName = name;
    }
  },
});

const SPLIT_DYNAMIC_APPENDERS: PhysicalAppenders = Object.freeze({
  ...SPLIT_MIXED_APPENDERS,
  writeSpanStart(buffer: AnySpanBuffer, name: string | number): void {
    const entryTypes = buffer.entry_type;
    if (entryTypes === undefined) throw new TypeError('Split dynamic layout is missing entry types');
    if (!consumeSpanStartedAtAllocation(buffer)) {
      const traceRoot = buffer._traceRoot;
      buffer.timestamp[0] = traceRoot._timestampNow(traceRoot);
      entryTypes[0] = ENTRY_TYPE_SPAN_START;
      entryTypes[1] = ENTRY_TYPE_SPAN_EXCEPTION;
      buffer.timestamp[1] = 0n;
      buffer._writeIndex = 2;
    }
    buffer._spanName = name;
  },
});

function packedAppenders(messageLayoutFamily: MessageLayoutFamily): PhysicalAppenders {
  return Object.freeze({
    writeSpanStart(buffer: AnySpanBuffer, name: string | number): void {
      const headers = buffer._rowHeaders;
      if (headers === undefined) throw new TypeError('Packed layout is missing row headers');
      const startedAtAllocation = consumeSpanStartedAtAllocation(buffer);
      if (!startedAtAllocation) buffer.timestamp[0] = buffer._traceRoot._timestampNow(buffer._traceRoot);
      if (typeof name === 'number') {
        if (name > MAX_PACKED_MESSAGE_DENSE_INDEX) throw new RangeError('Packed message dense index exceeds 0xFFFFFE');
        headers[0] = (((name + 1) << 8) | ENTRY_TYPE_SPAN_START) >>> 0;
      } else {
        headers[0] = ENTRY_TYPE_SPAN_START;
        if (messageLayoutFamily === 'static-only' || messageLayoutFamily === 'dynamic-only') {
          buffer._spanName = name;
        } else {
          const rawMessages = buffer.message_values;
          if (rawMessages === undefined) throw new TypeError('Packed mixed layout is missing raw message storage');
          if (typeof name !== 'string') throw new TypeError('Packed mixed numeric span name was not encoded');
          rawMessages[0] = name;
        }
      }
      if (!startedAtAllocation) {
        headers[1] = ENTRY_TYPE_SPAN_EXCEPTION;
        buffer.timestamp[1] = 0n;
        buffer._writeIndex = 2;
      }
    },
    writeSpanEnd(buffer: AnySpanBuffer, entryType: number): void {
      const headers = buffer._rowHeaders;
      if (headers === undefined) throw new TypeError('Packed layout is missing row headers');
      const traceRoot = buffer._traceRoot;
      buffer.timestamp[1] = traceRoot._timestampNow(traceRoot);
      headers[1] = entryType;
      buffer._sealStatsChain();
    },
    writeLogEntry(buffer: AnySpanBuffer, entryType: number): number {
      return PACKED_APPEND_LOG_ENTRY(buffer._traceRoot, buffer, entryType);
    },
  });
}

type AppendersKey = `${MessageLayoutFamily}:${MessagePhysicalLayout}`;

const APPENDERS_BY_MESSAGE_LAYOUT: Readonly<Record<AppendersKey, PhysicalAppenders>> = Object.freeze({
  'static-only:current': CURRENT_STATIC_APPENDERS,
  'mixed:current': CURRENT_MIXED_APPENDERS,
  'dynamic-only:current': CURRENT_DYNAMIC_APPENDERS,
  'static-only:specialized': SPLIT_STATIC_APPENDERS,
  'mixed:specialized': SPLIT_MIXED_APPENDERS,
  'dynamic-only:specialized': SPLIT_DYNAMIC_APPENDERS,
  'static-only:packed': packedAppenders('static-only'),
  'mixed:packed': packedAppenders('mixed'),
  'dynamic-only:packed': packedAppenders('dynamic-only'),
});

/** Lifecycle writers for a js-heap/wasm buffer class of this message layout. */
export function appendersForLayout(
  messageLayoutFamily: MessageLayoutFamily,
  messagePhysicalLayout: MessagePhysicalLayout,
): PhysicalAppenders {
  return APPENDERS_BY_MESSAGE_LAYOUT[`${messageLayoutFamily}:${messagePhysicalLayout}`];
}

/** Timestamped log-row append primitive for a js-heap/wasm buffer class of this layout. */
export function appendLogEntryForLayout(messagePhysicalLayout: MessagePhysicalLayout): TimestampAppendPrimitive {
  return messagePhysicalLayout === 'packed' ? PACKED_APPEND_LOG_ENTRY : SPLIT_APPEND_LOG_ENTRY;
}

/** Install the layout-selected lifecycle writers on a buffer class prototype. */
export function installLifecycleAppenders(
  prototype: object,
  messageLayoutFamily: MessageLayoutFamily,
  messagePhysicalLayout: MessagePhysicalLayout,
): void {
  Object.defineProperties(prototype, {
    _appenders: { value: appendersForLayout(messageLayoutFamily, messagePhysicalLayout) },
    _appendLogEntry: { value: appendLogEntryForLayout(messagePhysicalLayout) },
  });
}
