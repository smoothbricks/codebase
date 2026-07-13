import { describe, expect, it } from 'bun:test';
import { createHash } from 'node:crypto';
import {
  getVocabularyGeneration,
  registerVocabularyFragment,
  type VocabularyFragment,
  type VocabularyGeneration,
  VocabularyRegistrationError,
} from '../vocabularyRegistry.js';

const encoder = new TextEncoder();
const decoder = new TextDecoder();

type RecordField = { name: string; column: string };
type RecordInput = { text: string; fields?: readonly RecordField[] };

function encodeRecord({ text, fields = [] }: RecordInput): Uint8Array {
  const textBytes = encoder.encode(text);
  const encodedFields = fields.map(({ name, column }) => ({
    name: encoder.encode(name),
    column: encoder.encode(column),
  }));
  let byteLength = 4 + textBytes.length + 2;
  for (const field of encodedFields) byteLength += 2 + field.name.length + 2 + field.column.length;
  const record = new Uint8Array(byteLength);
  const view = new DataView(record.buffer);
  let offset = 0;
  view.setUint32(offset, textBytes.length, true);
  offset += 4;
  record.set(textBytes, offset);
  offset += textBytes.length;
  view.setUint16(offset, encodedFields.length, true);
  offset += 2;
  for (const field of encodedFields) {
    view.setUint16(offset, field.name.length, true);
    offset += 2;
    record.set(field.name, offset);
    offset += field.name.length;
    view.setUint16(offset, field.column.length, true);
    offset += 2;
    record.set(field.column, offset);
    offset += field.column.length;
  }
  return record;
}

function stableId(kindTag: number, record: Uint8Array): number {
  const digest = createHash('sha256').update(Uint8Array.of(kindTag)).update(record).digest();
  return (digest[0] << 16) | (digest[1] << 8) | digest[2];
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

function makeFragment(
  inputs: readonly RecordInput[],
  kindTags: readonly number[] = inputs.map(() => 1),
): VocabularyFragment {
  const records = inputs.map(encodeRecord);
  const offsets = new Int32Array(records.length + 1);
  let byteLength = 0;
  for (let ordinal = 0; ordinal < records.length; ordinal++) {
    byteLength += records[ordinal].length;
    offsets[ordinal + 1] = byteLength;
  }
  const utf8 = new Uint8Array(byteLength);
  let offset = 0;
  for (const record of records) {
    utf8.set(record, offset);
    offset += record.length;
  }
  const withoutHash: Omit<VocabularyFragment, 'contentHash'> = {
    schemaVersion: 1,
    idAlgorithm: 'sha256-24-v1',
    ids: Uint32Array.from(records, (record, ordinal) => stableId(kindTags[ordinal], record)),
    kindTags: Uint8Array.from(kindTags),
    utf8,
    offsets,
  };
  return { ...withoutHash, contentHash: fragmentHash(withoutHash) };
}

function textAt(generation: VocabularyGeneration, denseIndex: number): string {
  const record = generation.records.subarray(generation.offsets[denseIndex], generation.offsets[denseIndex + 1]);
  const textLength = new DataView(record.buffer, record.byteOffset, record.byteLength).getUint32(0, true);
  return decoder.decode(record.subarray(4, 4 + textLength));
}

function decodedBinding(binding: Uint32Array): string[] {
  const generation = getVocabularyGeneration();
  return Array.from(binding, (denseIndex) => textAt(generation, denseIndex));
}

function expectRegistrationError(fragment: unknown, code: string): void {
  let thrown: unknown;
  try {
    Reflect.apply(registerVocabularyFragment, undefined, [fragment]);
  } catch (error) {
    thrown = error;
  }
  expect(thrown).toBeInstanceOf(VocabularyRegistrationError);
  if (!(thrown instanceof VocabularyRegistrationError)) throw new Error(`expected ${code}`);
  expect(thrown.message.startsWith(`${code}:`)).toBe(true);
}

describe('vocabulary fragment registration', () => {
  it('binds fragment ordinals directly while duplicate and overlapping registration preserve append-only generations', () => {
    const alpha = { text: 'registry overlap alpha', fields: [{ name: 'request.id', column: 'request_id' }] };
    const beta = { text: 'registry overlap beta' };
    const gamma = { text: 'registry overlap gamma', fields: [{ name: 'user', column: 'user_name' }] };
    const firstFragment = makeFragment([alpha, beta], [1, 2]);
    const overlappingFragment = makeFragment([beta, gamma], [2, 1]);

    const before = getVocabularyGeneration();
    const firstBinding = registerVocabularyFragment(firstFragment);
    const firstGeneration = getVocabularyGeneration();
    expect(firstGeneration.generation).toBe(before.generation + 1);
    expect(decodedBinding(firstBinding)).toEqual([alpha.text, beta.text]);

    const firstSnapshot = {
      ids: Array.from(firstGeneration.ids),
      kinds: Array.from(firstGeneration.kindTags),
      records: Array.from(firstGeneration.records),
      offsets: Array.from(firstGeneration.offsets),
    };
    const overlapBinding = registerVocabularyFragment(overlappingFragment);
    const secondGeneration = getVocabularyGeneration();

    expect(overlapBinding[0]).toBe(firstBinding[1]);
    expect(decodedBinding(overlapBinding)).toEqual([beta.text, gamma.text]);
    expect(secondGeneration.generation).toBe(firstGeneration.generation + 1);
    expect(Array.from(secondGeneration.ids.subarray(0, firstGeneration.ids.length))).toEqual(firstSnapshot.ids);
    expect(Array.from(secondGeneration.kindTags.subarray(0, firstGeneration.kindTags.length))).toEqual(
      firstSnapshot.kinds,
    );
    expect(Array.from(secondGeneration.records.subarray(0, firstGeneration.records.length))).toEqual(
      firstSnapshot.records,
    );
    expect(Array.from(secondGeneration.offsets.subarray(0, firstGeneration.offsets.length))).toEqual(
      firstSnapshot.offsets,
    );
    expect(Array.from(firstGeneration.ids)).toEqual(firstSnapshot.ids);
    expect(Array.from(firstGeneration.kindTags)).toEqual(firstSnapshot.kinds);
    expect(Array.from(firstGeneration.records)).toEqual(firstSnapshot.records);
    expect(Array.from(firstGeneration.offsets)).toEqual(firstSnapshot.offsets);

    const duplicateBinding = registerVocabularyFragment(firstFragment);
    expect(duplicateBinding).toBe(firstBinding);
    expect(getVocabularyGeneration()).toBe(secondGeneration);
  });

  it('decodes each fragment in ordinal order regardless of the records registration order', () => {
    const left = { text: 'registry ordinal left' };
    const right = { text: 'registry ordinal right' };
    const forward = makeFragment([left, right]);
    const reverse = makeFragment([right, left]);

    const forwardBinding = registerVocabularyFragment(forward);
    const generationAfterForward = getVocabularyGeneration();
    const reverseBinding = registerVocabularyFragment(reverse);

    expect(decodedBinding(forwardBinding)).toEqual([left.text, right.text]);
    expect(decodedBinding(reverseBinding)).toEqual([right.text, left.text]);
    expect(Array.from(reverseBinding)).toEqual([forwardBinding[1], forwardBinding[0]]);
    expect(getVocabularyGeneration()).toBe(generationAfterForward);
  });

  it('rejects malformed ABI, offsets, kind tags, record-derived IDs, UTF-8, and payload hashes before mutation', () => {
    const valid = makeFragment([{ text: 'registry validation sentinel' }]);
    const generation = getVocabularyGeneration();
    const malformedCases: Array<{ name: string; fragment: unknown; code: string }> = [
      {
        name: 'schema version',
        fragment: { ...valid, schemaVersion: 2 },
        code: 'LMAO_VOCABULARY_ABI_UNAVAILABLE',
      },
      {
        name: 'ID algorithm',
        fragment: { ...valid, idAlgorithm: 'sha256-24-v2' },
        code: 'LMAO_VOCABULARY_ABI_UNAVAILABLE',
      },
      {
        name: 'offset coverage',
        fragment: { ...valid, offsets: new Int32Array([1, valid.utf8.length]) },
        code: 'LMAO_VOCABULARY_FRAGMENT_INVALID',
      },
      {
        name: 'nonmonotonic offsets',
        fragment: (() => {
          const malformed = makeFragment([
            { text: 'registry offset first' },
            { text: 'registry offset second' },
            { text: 'registry offset third' },
          ]);
          return {
            ...malformed,
            offsets: new Int32Array([0, malformed.offsets[2], malformed.offsets[1], malformed.utf8.length]),
          };
        })(),
        code: 'LMAO_VOCABULARY_FRAGMENT_INVALID',
      },
      {
        name: 'unknown kind',
        fragment: makeFragment([{ text: 'registry unknown kind' }], [3]),
        code: 'LMAO_VOCABULARY_FRAGMENT_INVALID',
      },
      {
        name: 'record-derived ID mismatch',
        fragment: (() => {
          const mismatch = makeFragment([{ text: 'registry wrong stable ID' }]);
          const withoutHash = { ...mismatch, ids: Uint32Array.of(mismatch.ids[0] ^ 1) };
          return { ...withoutHash, contentHash: fragmentHash(withoutHash) };
        })(),
        code: 'LMAO_VOCABULARY_FRAGMENT_INVALID',
      },
      {
        name: 'invalid UTF-8 text',
        fragment: (() => {
          const malformed = makeFragment([{ text: 'registry malformed UTF-8' }]);
          const utf8 = malformed.utf8.slice();
          utf8[4] = 0xff;
          return { ...malformed, utf8 };
        })(),
        code: 'LMAO_VOCABULARY_FRAGMENT_INVALID',
      },
      {
        name: 'payload hash mismatch',
        fragment: { ...valid, contentHash: '0'.repeat(64) },
        code: 'LMAO_VOCABULARY_CONTENT_HASH_MISMATCH',
      },
    ];

    for (const testCase of malformedCases) {
      try {
        expectRegistrationError(testCase.fragment, testCase.code);
      } catch (error) {
        throw new Error(`case ${testCase.name} failed`, { cause: error });
      }
      expect(getVocabularyGeneration()).toBe(generation);
    }
  });

  it('rejects reuse of a registered content hash with a different payload before mutation', () => {
    const registered = makeFragment([{ text: 'registry content hash owner' }]);
    registerVocabularyFragment(registered);
    const generation = getVocabularyGeneration();
    const conflictingPayload = makeFragment([{ text: 'registry content hash impostor' }]);

    expectRegistrationError(
      { ...conflictingPayload, contentHash: registered.contentHash },
      'LMAO_VOCABULARY_CONTENT_HASH_COLLISION',
    );
    expect(getVocabularyGeneration()).toBe(generation);
    expect(decodedBinding(registerVocabularyFragment(registered))).toEqual(['registry content hash owner']);
  });

  it('rejects a stable-ID collision without partially registering the fragment', () => {
    const firstRecord = { text: 'stable collision fixture 585' };
    const secondRecord = { text: 'stable collision fixture 4248' };
    const first = makeFragment([firstRecord]);
    const second = makeFragment([secondRecord]);

    expect(first.ids[0]).toBe(15_813_490);
    expect(second.ids[0]).toBe(first.ids[0]);
    registerVocabularyFragment(first);
    const generation = getVocabularyGeneration();
    expectRegistrationError(second, 'LMAO_VOCABULARY_ID_COLLISION');
    expect(getVocabularyGeneration()).toBe(generation);
    expect(decodedBinding(registerVocabularyFragment(first))).toEqual([firstRecord.text]);
  });
});
