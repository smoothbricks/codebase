import type { AnySpanBuffer } from './types.js';
import type { VocabularyGeneration } from './vocabularyRegistry.js';

const decoder = new TextDecoder('utf-8', { fatal: true });
const decodedGenerations = new WeakMap<VocabularyGeneration, (string | undefined)[]>();

function decodeVocabularyMessage(generation: VocabularyGeneration, denseIndex: number): string {
  if (denseIndex >= generation.ids.length) {
    throw new Error(`Invalid vocabulary dense index ${denseIndex} for generation ${generation.generation}`);
  }
  let values = decodedGenerations.get(generation);
  if (values === undefined) {
    values = new Array<string | undefined>(generation.ids.length);
    decodedGenerations.set(generation, values);
  }
  const cached = values[denseIndex];
  if (cached !== undefined) return cached;

  const start = generation.offsets[denseIndex];
  const end = generation.offsets[denseIndex + 1];
  const record = generation.records.subarray(start, end);
  const textLength = new DataView(record.buffer, record.byteOffset, 4).getUint32(0, true);
  const value = decoder.decode(record.subarray(4, 4 + textLength));
  values[denseIndex] = value;
  return value;
}

/** Resolve one dynamic or process-dense vocabulary message row. */
export function resolveMessage(buffer: AnySpanBuffer, row: number): string | undefined {
  const header = buffer._logHeaders[row];
  if (header === 0) return buffer.message_values[row];
  const headerEntryType = header & 0xff;
  if (headerEntryType !== buffer.entry_type[row]) {
    throw new Error(`Log header entry type ${headerEntryType} does not match row ${row}`);
  }
  return decodeVocabularyMessage(buffer._vocabularyGeneration, header >>> 8);
}
