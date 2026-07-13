import type { AnySpanBuffer } from './types.js';
import type { VocabularyGeneration } from './vocabularyRegistry.js';

const decoder = new TextDecoder('utf-8', { fatal: true });
const decodedGenerations = new WeakMap<VocabularyGeneration, (string | undefined)[]>();
export const MAX_PACKED_MESSAGE_DENSE_INDEX = 0x00fffffe;

export function decodeVocabularyMessage(generation: VocabularyGeneration, denseIndex: number): string {
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

/** Resolve one entry type from either split or packed physical storage. */
export function resolveEntryType(buffer: AnySpanBuffer, row: number): number {
  const packed = buffer._rowHeaders;
  if (packed !== undefined) return packed[row] & 0xff;
  const split = buffer.entry_type;
  if (split === undefined) throw new TypeError('Span buffer has no declared entry-type storage');
  return split[row];
}

/** Resolve one dynamic or process-dense vocabulary message row. */
export function resolveMessage(buffer: AnySpanBuffer, row: number): string | undefined {
  if (row === 0 && buffer._spanName !== undefined) {
    return typeof buffer._spanName === 'number'
      ? decodeVocabularyMessage(buffer._vocabularyGeneration, buffer._spanName)
      : buffer._spanName;
  }
  if (row === 1 && buffer._terminalMessage !== undefined) return buffer._terminalMessage;

  if (buffer._messagePhysicalLayout === 'current') {
    const localId = buffer._messageIds?.[row] ?? 0;
    if (localId === 0) return buffer.message_values?.[row];
    const denseIndex = buffer._opMetadata._physicalLayoutPlan?.localMessageDictionary[localId - 1];
    if (denseIndex === undefined) throw new Error(`Missing local message dictionary entry ${localId}`);
    return decodeVocabularyMessage(buffer._vocabularyGeneration, denseIndex);
  }

  const packed = buffer._rowHeaders;
  if (packed !== undefined) {
    const encodedDenseIndex = packed[row] >>> 8;
    return encodedDenseIndex === 0
      ? buffer.message_values?.[row]
      : decodeVocabularyMessage(buffer._vocabularyGeneration, encodedDenseIndex - 1);
  }

  const encodedDenseIndex = buffer._logHeaders?.[row] ?? 0;
  return encodedDenseIndex === 0
    ? buffer.message_values?.[row]
    : decodeVocabularyMessage(buffer._vocabularyGeneration, encodedDenseIndex - 1);
}
