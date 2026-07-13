import { Column, utf8 } from '@uwdata/flechette';
import '../../generated/vocabulary.js';
import { ENTRY_TYPE_NAMES } from '../schema/systemSchema.js';
import type { VocabularyGeneration } from '../vocabularyRegistry.js';
import { makeArrowColumn } from './flechette.js';

const ENTRY_TYPE_UTF8 = new TextEncoder().encode(ENTRY_TYPE_NAMES.join(''));
const ENTRY_TYPE_OFFSETS = new Int32Array(ENTRY_TYPE_NAMES.length + 1);
for (let index = 0, offset = 0; index < ENTRY_TYPE_NAMES.length; index++) {
  ENTRY_TYPE_OFFSETS[index] = offset;
  offset += ENTRY_TYPE_NAMES[index].length;
  ENTRY_TYPE_OFFSETS[index + 1] = offset;
}

function makeUtf8Dictionary(length: number, offsets: Int32Array, values: Uint8Array): Column<unknown> {
  return makeArrowColumn({
    type: utf8(),
    length,
    nullCount: 0,
    offsets,
    values,
  });
}

const EMPTY_DICTIONARY_BYTES = new Uint8Array(0);
const EMPTY_DICTIONARY_OFFSETS = new Int32Array([0, 0]);
const decoder = new TextDecoder('utf-8', { fatal: true });

export interface VocabularyDictionaryPrefix {
  readonly column: Column<unknown>;
  readonly length: number;
  readonly valueToDenseIndex: ReadonlyMap<string, number>;
}

const vocabularyDictionaries = new WeakMap<VocabularyGeneration, VocabularyDictionaryPrefix>();

/**
 * Return the immutable Arrow dictionary prefix for one pinned vocabulary generation.
 * Record text is decoded and copied into Arrow UTF-8 buffers once per generation;
 * every flush thereafter reuses the same column and backing buffers.
 */
export function getVocabularyDictionaryPrefix(generation: VocabularyGeneration): VocabularyDictionaryPrefix {
  const cached = vocabularyDictionaries.get(generation);
  if (cached !== undefined) return cached;

  if (generation.ids.length === 0) {
    const emptyPrefix = Object.freeze({
      column: makeUtf8Dictionary(1, EMPTY_DICTIONARY_OFFSETS, EMPTY_DICTIONARY_BYTES),
      length: 1,
      valueToDenseIndex: new Map<string, number>([['', 0]]),
    });
    emptyPrefix.column.cache();
    vocabularyDictionaries.set(generation, emptyPrefix);
    return emptyPrefix;
  }

  const count = generation.ids.length;
  const offsets = new Int32Array(count + 1);
  const textLengths = new Uint32Array(count);
  let byteLength = 0;
  for (let denseIndex = 0; denseIndex < count; denseIndex++) {
    const recordOffset = generation.offsets[denseIndex];
    const textLength = new DataView(
      generation.records.buffer,
      generation.records.byteOffset + recordOffset,
      4,
    ).getUint32(0, true);
    textLengths[denseIndex] = textLength;
    byteLength += textLength;
    offsets[denseIndex + 1] = byteLength;
  }

  const values = new Uint8Array(byteLength);
  const valueToDenseIndex = new Map<string, number>();
  for (let denseIndex = 0; denseIndex < count; denseIndex++) {
    const textStart = generation.offsets[denseIndex] + 4;
    const text = generation.records.subarray(textStart, textStart + textLengths[denseIndex]);
    values.set(text, offsets[denseIndex]);
    const value = decoder.decode(text);
    if (!valueToDenseIndex.has(value)) valueToDenseIndex.set(value, denseIndex);
  }

  const prefix = Object.freeze({
    column: makeUtf8Dictionary(count, offsets, values),
    length: count,
    valueToDenseIndex,
  });
  prefix.column.cache();
  vocabularyDictionaries.set(generation, prefix);
  return prefix;
}


/**
 * Append one UTF-8 dictionary batch without copying the prefix buffers or
 * scanning its cached strings. Flechette serializes later dictionary batches
 * as Arrow dictionary deltas.
 */
export function appendVocabularyDictionarySuffix(
  prefix: VocabularyDictionaryPrefix,
  suffix: Column<unknown>,
  suffixValues: readonly string[],
): Column<unknown> {
  const dictionary = new Column([...prefix.column.data, ...suffix.data]);
  const prefixCache = prefix.column.cache();
  const cacheTarget = new Array<unknown>(prefix.length + suffixValues.length);
  dictionary._cache = new Proxy(cacheTarget, {
    get(target, property, receiver) {
      if (typeof property === 'string') {
        const index = Number(property);
        if (Number.isInteger(index) && index >= 0 && index < target.length && String(index) === property) {
          return index < prefix.length ? prefixCache[index] : suffixValues[index - prefix.length];
        }
      }
      return Reflect.get(target, property, receiver);
    },
  });
  return dictionary;
}

export const PREBUILT_ENTRY_TYPE_DICTIONARY = makeUtf8Dictionary(
  ENTRY_TYPE_NAMES.length,
  ENTRY_TYPE_OFFSETS,
  ENTRY_TYPE_UTF8,
);
