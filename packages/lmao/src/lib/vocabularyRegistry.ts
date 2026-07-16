export const REGISTER_VOCABULARY = Symbol.for('@smoothbricks/lmao/vocabulary/register/v1');

const VOCABULARY_STATE = Symbol.for('@smoothbricks/lmao/vocabulary/state/v1');
const SCHEMA_VERSION = 1;
const ID_ALGORITHM = 'sha256-24-v1';
const MAX_VOCABULARY_ID = 0x00ff_ffff;
const CONTENT_HASH_PATTERN = /^[0-9a-f]{64}$/;
const SHA256_INITIAL = new Uint32Array([
  0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19,
]);
const SHA256_ROUND = new Uint32Array([
  0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5, 0xd807aa98,
  0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786,
  0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8,
  0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13,
  0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819,
  0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a,
  0x5b9cca4f, 0x682e6ff3, 0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
  0xc67178f2,
]);

export interface VocabularyFragment {
  readonly schemaVersion: 1;
  readonly idAlgorithm: 'sha256-24-v1';
  readonly contentHash: string;
  readonly ids: Uint32Array;
  readonly kindTags: Uint8Array;
  readonly utf8: Uint8Array;
  readonly offsets: Int32Array;
}

export type VocabularyBinding = Uint32Array;

export interface VocabularyGeneration {
  readonly generation: number;
  readonly ids: Uint32Array;
  readonly kindTags: Uint8Array;
  readonly records: Uint8Array;
  readonly offsets: Int32Array;
}

type RegisteredFragment = {
  readonly ids: Uint32Array;
  readonly kindTags: Uint8Array;
  readonly utf8: Uint8Array;
  readonly offsets: Int32Array;
  readonly binding: VocabularyBinding;
};

type RegisteredEntry = {
  readonly kindTag: number;
  readonly record: Uint8Array;
  readonly denseIndex: number;
};

type VocabularyState = {
  readonly entriesById: Map<number, RegisteredEntry>;
  readonly fragmentsByHash: Map<string, RegisteredFragment>;
  readonly generations: VocabularyGeneration[];
  current: VocabularyGeneration;
};

export class VocabularyRegistrationError extends Error {
  override readonly name = 'VocabularyRegistrationError';
}

const decoder = new TextDecoder('utf-8', { fatal: true });
const algorithmBytes = new TextEncoder().encode(ID_ALGORITHM);

function fail(code: string, detail: string): never {
  throw new VocabularyRegistrationError(`${code}: ${detail}`);
}

function rotateRight(value: number, count: number): number {
  return (value >>> count) | (value << (32 - count));
}

function sha256(input: Uint8Array): Uint8Array {
  const bitLength = input.length * 8;
  const paddedLength = (input.length + 9 + 63) & ~63;
  const padded = new Uint8Array(paddedLength);
  padded.set(input);
  padded[input.length] = 0x80;
  const view = new DataView(padded.buffer);
  view.setUint32(paddedLength - 8, Math.floor(bitLength / 0x1_0000_0000), false);
  view.setUint32(paddedLength - 4, bitLength >>> 0, false);

  const hash = SHA256_INITIAL.slice();
  const words = new Uint32Array(64);
  for (let block = 0; block < paddedLength; block += 64) {
    for (let index = 0; index < 16; index++) words[index] = view.getUint32(block + index * 4, false);
    for (let index = 16; index < 64; index++) {
      const x = words[index - 15];
      const y = words[index - 2];
      const sigma0 = rotateRight(x, 7) ^ rotateRight(x, 18) ^ (x >>> 3);
      const sigma1 = rotateRight(y, 17) ^ rotateRight(y, 19) ^ (y >>> 10);
      words[index] = (words[index - 16] + sigma0 + words[index - 7] + sigma1) >>> 0;
    }
    let a = hash[0];
    let b = hash[1];
    let c = hash[2];
    let d = hash[3];
    let e = hash[4];
    let f = hash[5];
    let g = hash[6];
    let h = hash[7];
    for (let index = 0; index < 64; index++) {
      const sum1 = rotateRight(e, 6) ^ rotateRight(e, 11) ^ rotateRight(e, 25);
      const choose = (e & f) ^ (~e & g);
      const temp1 = (h + sum1 + choose + SHA256_ROUND[index] + words[index]) >>> 0;
      const sum0 = rotateRight(a, 2) ^ rotateRight(a, 13) ^ rotateRight(a, 22);
      const majority = (a & b) ^ (a & c) ^ (b & c);
      const temp2 = (sum0 + majority) >>> 0;
      h = g;
      g = f;
      f = e;
      e = (d + temp1) >>> 0;
      d = c;
      c = b;
      b = a;
      a = (temp1 + temp2) >>> 0;
    }
    hash[0] = (hash[0] + a) >>> 0;
    hash[1] = (hash[1] + b) >>> 0;
    hash[2] = (hash[2] + c) >>> 0;
    hash[3] = (hash[3] + d) >>> 0;
    hash[4] = (hash[4] + e) >>> 0;
    hash[5] = (hash[5] + f) >>> 0;
    hash[6] = (hash[6] + g) >>> 0;
    hash[7] = (hash[7] + h) >>> 0;
  }
  const result = new Uint8Array(32);
  const resultView = new DataView(result.buffer);
  for (let index = 0; index < hash.length; index++) resultView.setUint32(index * 4, hash[index], false);
  return result;
}

function hex(bytes: Uint8Array): string {
  let result = '';
  for (const byte of bytes) result += byte.toString(16).padStart(2, '0');
  return result;
}

function equalBytes(
  left: Uint8Array | Uint32Array | Int32Array,
  right: Uint8Array | Uint32Array | Int32Array,
): boolean {
  if (left.constructor !== right.constructor || left.length !== right.length) return false;
  for (let index = 0; index < left.length; index++) if (left[index] !== right[index]) return false;
  return true;
}

function samePayload(registered: RegisteredFragment, fragment: VocabularyFragment): boolean {
  return (
    equalBytes(registered.ids, fragment.ids) &&
    equalBytes(registered.kindTags, fragment.kindTags) &&
    equalBytes(registered.utf8, fragment.utf8) &&
    equalBytes(registered.offsets, fragment.offsets)
  );
}

function recordAt(fragment: VocabularyFragment, ordinal: number): Uint8Array {
  return fragment.utf8.subarray(fragment.offsets[ordinal], fragment.offsets[ordinal + 1]);
}

function validateRecord(record: Uint8Array, ordinal: number): void {
  const view = new DataView(record.buffer, record.byteOffset, record.byteLength);
  if (record.length < 6) fail('LMAO_VOCABULARY_FRAGMENT_INVALID', `record ${ordinal} is truncated`);
  let offset = 0;
  const textLength = view.getUint32(offset, true);
  offset += 4;
  if (textLength > record.length - offset - 2)
    fail('LMAO_VOCABULARY_FRAGMENT_INVALID', `record ${ordinal} text is truncated`);
  try {
    decoder.decode(record.subarray(offset, offset + textLength));
  } catch {
    fail('LMAO_VOCABULARY_FRAGMENT_INVALID', `record ${ordinal} text is not valid UTF-8`);
  }
  offset += textLength;
  const fieldCount = view.getUint16(offset, true);
  offset += 2;
  for (let field = 0; field < fieldCount; field++) {
    if (offset + 2 > record.length)
      fail('LMAO_VOCABULARY_FRAGMENT_INVALID', `record ${ordinal} field ${field} name length is truncated`);
    const nameLength = view.getUint16(offset, true);
    offset += 2;
    if (offset + nameLength + 2 > record.length)
      fail('LMAO_VOCABULARY_FRAGMENT_INVALID', `record ${ordinal} field ${field} name is truncated`);
    try {
      decoder.decode(record.subarray(offset, offset + nameLength));
    } catch {
      fail('LMAO_VOCABULARY_FRAGMENT_INVALID', `record ${ordinal} field ${field} name is not valid UTF-8`);
    }
    offset += nameLength;
    const columnLength = view.getUint16(offset, true);
    offset += 2;
    if (offset + columnLength > record.length)
      fail('LMAO_VOCABULARY_FRAGMENT_INVALID', `record ${ordinal} field ${field} column is truncated`);
    try {
      decoder.decode(record.subarray(offset, offset + columnLength));
    } catch {
      fail('LMAO_VOCABULARY_FRAGMENT_INVALID', `record ${ordinal} field ${field} column is not valid UTF-8`);
    }
    offset += columnLength;
  }
  if (offset !== record.length) fail('LMAO_VOCABULARY_FRAGMENT_INVALID', `record ${ordinal} has trailing bytes`);
}

function contentHash(fragment: VocabularyFragment): string {
  const size =
    1 +
    2 +
    algorithmBytes.length +
    4 +
    fragment.ids.length * 4 +
    4 +
    fragment.kindTags.length +
    4 +
    fragment.utf8.length +
    4 +
    fragment.offsets.length * 4;
  const bytes = new Uint8Array(size);
  const view = new DataView(bytes.buffer);
  let offset = 0;
  bytes[offset++] = fragment.schemaVersion;
  view.setUint16(offset, algorithmBytes.length, true);
  offset += 2;
  bytes.set(algorithmBytes, offset);
  offset += algorithmBytes.length;
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
  return hex(sha256(bytes));
}

function createState(): VocabularyState {
  const initial: VocabularyGeneration = Object.freeze({
    generation: 0,
    ids: new Uint32Array(0),
    kindTags: new Uint8Array(0),
    records: new Uint8Array(0),
    offsets: new Int32Array([0]),
  });
  return { entriesById: new Map(), fragmentsByHash: new Map(), generations: [initial], current: initial };
}

function globalState(): VocabularyState {
  const globals = globalThis as typeof globalThis & { [VOCABULARY_STATE]?: VocabularyState };
  let state = globals[VOCABULARY_STATE];
  if (state === undefined) {
    state = createState();
    Object.defineProperty(globals, VOCABULARY_STATE, {
      value: state,
      configurable: false,
      enumerable: false,
      writable: false,
    });
  }
  return state;
}

function validateFragmentStructure(fragment: VocabularyFragment): void {
  if (fragment.schemaVersion !== SCHEMA_VERSION || fragment.idAlgorithm !== ID_ALGORITHM)
    fail('LMAO_VOCABULARY_ABI_UNAVAILABLE', 'unsupported schema version or ID algorithm');
  if (!CONTENT_HASH_PATTERN.test(fragment.contentHash))
    fail('LMAO_VOCABULARY_FRAGMENT_INVALID', `invalid contentHash ${JSON.stringify(fragment.contentHash)}`);
  if (
    !(fragment.ids instanceof Uint32Array) ||
    !(fragment.kindTags instanceof Uint8Array) ||
    !(fragment.utf8 instanceof Uint8Array) ||
    !(fragment.offsets instanceof Int32Array)
  )
    fail('LMAO_VOCABULARY_FRAGMENT_INVALID', 'fragment arrays use incorrect types');
  if (fragment.kindTags.length !== fragment.ids.length || fragment.offsets.length !== fragment.ids.length + 1)
    fail('LMAO_VOCABULARY_FRAGMENT_INVALID', 'parallel lengths differ');
  if (fragment.offsets[0] !== 0 || fragment.offsets[fragment.offsets.length - 1] !== fragment.utf8.length)
    fail('LMAO_VOCABULARY_FRAGMENT_INVALID', 'offsets do not cover bytes');
  const localOrdinals = new Map<number, number>();
  for (let ordinal = 0; ordinal < fragment.ids.length; ordinal++) {
    const id = fragment.ids[ordinal];
    const kindTag = fragment.kindTags[ordinal];
    if (id === 0 || id > MAX_VOCABULARY_ID) fail('LMAO_VOCABULARY_FRAGMENT_INVALID', `stable ID ${id}`);
    if (kindTag !== 1 && kindTag !== 2) fail('LMAO_VOCABULARY_FRAGMENT_INVALID', `kind tag ${kindTag}`);
    if (fragment.offsets[ordinal] > fragment.offsets[ordinal + 1])
      fail('LMAO_VOCABULARY_FRAGMENT_INVALID', 'offsets are not monotonic');
    const record = recordAt(fragment, ordinal);
    validateRecord(record, ordinal);
    const idInput = new Uint8Array(record.length + 1);
    idInput[0] = kindTag;
    idInput.set(record, 1);
    const digest = sha256(idInput);
    const derivedId = (digest[0] << 16) | (digest[1] << 8) | digest[2];
    if (id !== derivedId || id === 0)
      fail('LMAO_VOCABULARY_FRAGMENT_INVALID', `stable ID ${id} does not match record ${ordinal}`);
    const previousOrdinal = localOrdinals.get(id);
    if (
      previousOrdinal !== undefined &&
      (fragment.kindTags[previousOrdinal] !== kindTag || !equalBytes(recordAt(fragment, previousOrdinal), record))
    ) {
      fail('LMAO_VOCABULARY_ID_COLLISION', `stable ID ${id} within fragment`);
    }
    localOrdinals.set(id, ordinal);
  }
}

function appendGeneration(
  state: VocabularyState,
  ids: readonly number[],
  kindTags: readonly number[],
  records: readonly Uint8Array[],
): void {
  if (ids.length === 0) return;
  const previous = state.current;
  let byteLength = previous.records.length;
  for (const record of records) byteLength += record.length;
  const generationIds = new Uint32Array(previous.ids.length + ids.length);
  generationIds.set(previous.ids);
  generationIds.set(ids, previous.ids.length);
  const generationKinds = new Uint8Array(previous.kindTags.length + kindTags.length);
  generationKinds.set(previous.kindTags);
  generationKinds.set(kindTags, previous.kindTags.length);
  const generationRecords = new Uint8Array(byteLength);
  generationRecords.set(previous.records);
  const generationOffsets = new Int32Array(generationIds.length + 1);
  generationOffsets.set(previous.offsets);
  let byteOffset = previous.records.length;
  for (let index = 0; index < records.length; index++) {
    generationRecords.set(records[index], byteOffset);
    byteOffset += records[index].length;
    generationOffsets[previous.ids.length + index + 1] = byteOffset;
  }
  const generation: VocabularyGeneration = Object.freeze({
    generation: previous.generation + 1,
    ids: generationIds,
    kindTags: generationKinds,
    records: generationRecords,
    offsets: generationOffsets,
  });
  state.generations.push(generation);
  state.current = generation;
}

function registerVocabulary(fragment: VocabularyFragment): VocabularyBinding {
  validateFragmentStructure(fragment);
  const state = globalState();
  const sameHash = state.fragmentsByHash.get(fragment.contentHash);
  if (sameHash !== undefined) {
    if (!samePayload(sameHash, fragment)) fail('LMAO_VOCABULARY_CONTENT_HASH_COLLISION', fragment.contentHash);
    return sameHash.binding;
  }
  if (contentHash(fragment) !== fragment.contentHash) {
    fail('LMAO_VOCABULARY_CONTENT_HASH_MISMATCH', fragment.contentHash);
  }
  for (let ordinal = 0; ordinal < fragment.ids.length; ordinal++) {
    const existing = state.entriesById.get(fragment.ids[ordinal]);
    if (
      existing !== undefined &&
      (existing.kindTag !== fragment.kindTags[ordinal] || !equalBytes(existing.record, recordAt(fragment, ordinal)))
    )
      fail('LMAO_VOCABULARY_ID_COLLISION', `stable ID ${fragment.ids[ordinal]}`);
  }

  const binding = new Uint32Array(fragment.ids.length);
  const appendedIds: number[] = [];
  const appendedKinds: number[] = [];
  const appendedRecords: Uint8Array[] = [];
  for (let ordinal = 0; ordinal < fragment.ids.length; ordinal++) {
    const id = fragment.ids[ordinal];
    let existing = state.entriesById.get(id);
    if (existing === undefined) {
      const record = recordAt(fragment, ordinal).slice();
      existing = {
        kindTag: fragment.kindTags[ordinal],
        record,
        denseIndex: state.current.ids.length + appendedIds.length,
      };
      state.entriesById.set(id, existing);
      appendedIds.push(id);
      appendedKinds.push(existing.kindTag);
      appendedRecords.push(record);
    }
    binding[ordinal] = existing.denseIndex;
  }
  appendGeneration(state, appendedIds, appendedKinds, appendedRecords);
  state.fragmentsByHash.set(fragment.contentHash, {
    ids: fragment.ids.slice(),
    kindTags: fragment.kindTags.slice(),
    utf8: fragment.utf8.slice(),
    offsets: fragment.offsets.slice(),
    binding,
  });
  return binding;
}

export type RegisterVocabulary = (fragment: VocabularyFragment) => VocabularyBinding;

function isRegisterVocabulary(value: unknown): value is RegisterVocabulary {
  return typeof value === 'function';
}

function installVocabularyRegistration(): RegisterVocabulary {
  const globals = globalThis as typeof globalThis & { [REGISTER_VOCABULARY]?: unknown };
  const installed = globals[REGISTER_VOCABULARY];
  if (installed !== undefined) {
    if (!isRegisterVocabulary(installed))
      fail('LMAO_VOCABULARY_ABI_UNAVAILABLE', 'global symbol contains a non-function');
    return installed;
  }
  Object.defineProperty(globals, REGISTER_VOCABULARY, {
    value: registerVocabulary,
    configurable: false,
    enumerable: false,
    writable: false,
  });
  return registerVocabulary;
}

export const registerVocabularyFragment = installVocabularyRegistration();

export function getVocabularyGeneration(): VocabularyGeneration {
  return globalState().current;
}
