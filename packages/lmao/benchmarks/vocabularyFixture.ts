import { createHash } from 'node:crypto';
import {
  registerVocabularyFragment,
  type VocabularyFragment,
} from '../src/lib/vocabularyRegistry.js';

const encoder = new TextEncoder();

function encodeLogRecord(text: string): Uint8Array {
  const textBytes = encoder.encode(text);
  const record = new Uint8Array(4 + textBytes.length + 2);
  new DataView(record.buffer).setUint32(0, textBytes.length, true);
  record.set(textBytes, 4);
  return record;
}

function contentHash(fragment: Omit<VocabularyFragment, 'contentHash'>): string {
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

export function registerBenchmarkVocabulary(texts: readonly string[]): Uint32Array {
  const records = texts.map(encodeLogRecord);
  const offsets = new Int32Array(records.length + 1);
  const ids = new Uint32Array(records.length);
  let byteLength = 0;
  for (let ordinal = 0; ordinal < records.length; ordinal++) {
    const record = records[ordinal];
    byteLength += record.length;
    offsets[ordinal + 1] = byteLength;
    const digest = createHash('sha256').update(Uint8Array.of(1)).update(record).digest();
    ids[ordinal] = (digest[0] << 16) | (digest[1] << 8) | digest[2];
  }
  const utf8 = new Uint8Array(byteLength);
  let offset = 0;
  for (const record of records) {
    utf8.set(record, offset);
    offset += record.length;
  }
  const fragment: Omit<VocabularyFragment, 'contentHash'> = {
    schemaVersion: 1,
    idAlgorithm: 'sha256-24-v1',
    ids,
    kindTags: new Uint8Array(texts.length).fill(1),
    utf8,
    offsets,
  };
  return registerVocabularyFragment({ ...fragment, contentHash: contentHash(fragment) });
}
