import { bench, do_not_optimize, group, run, summary } from 'mitata';

/**
 * Benchmark for the target compiler-emitted vocabulary registration ABI.
 *
 * The registration callback is not shipped yet, so startup variants are an
 * isolated model. Its fragment shape and Symbol.for call path are the approved
 * ABI. The cold lookup mirrors generated vocabulary binary search; the hot
 * candidate is exactly the emitted `binding[fragmentLocalOrdinal]` load.
 */

const REGISTER_VOCABULARY = Symbol.for('@smoothbricks/lmao/vocabulary/register/v1');
const U24_MAX = 0xff_ffff;
const FULL_SIZES = [1, 16, 256, 65_535] as const;
const QUICK_SIZES = [1, 16, 256, 1_024] as const;
const QUICK = process.argv.includes('--quick');
const SIZES: readonly number[] = QUICK ? QUICK_SIZES : FULL_SIZES;
const LOOKUPS_PER_INVOCATION = QUICK ? 1_024 : 65_536;
const FORMAT = parseFormat(argumentValue('--format'));
const encoder = new TextEncoder();

type RunFormat = 'json' | 'markdown' | 'mitata' | 'quiet';

function argumentValue(name: string): string | undefined {
  const prefix = `${name}=`;
  const inline = process.argv.find((argument) => argument.startsWith(prefix));
  if (inline !== undefined) return inline.slice(prefix.length);
  const index = process.argv.indexOf(name);
  return index === -1 ? undefined : process.argv[index + 1];
}

function parseFormat(value: string | undefined): RunFormat {
  if (value === undefined) return 'mitata';
  if (value === 'json' || value === 'markdown' || value === 'mitata' || value === 'quiet') return value;
  throw new Error(`Unsupported --format=${value}; expected json, markdown, mitata, or quiet`);
}

type ContentHash = string;
type VocabularyBinding = Uint32Array;

interface VocabularyFragmentV1 {
  readonly schemaVersion: 1;
  readonly idAlgorithm: 'sha256-24-v1';
  readonly contentHash: ContentHash;
  readonly ids: Uint32Array;
  readonly kindTags: Uint8Array;
  readonly utf8: Uint8Array;
  readonly offsets: Int32Array;
}

type RegisterVocabulary = (fragment: VocabularyFragmentV1) => VocabularyBinding;

interface RegistrationObservation {
  readonly binding: VocabularyBinding;
  readonly registry: ModeledVocabularyRegistry;
}

class ModeledVocabularyRegistry {
  readonly denseByStableId = new Map<number, number>();
  readonly denseIds: number[] = [];
  readonly denseKindTags: number[] = [];
  readonly denseRecords: Uint8Array[] = [];
  readonly fragmentsByContentHash = new Map<ContentHash, VocabularyFragmentV1>();
  readonly generationEnds: number[] = [];

  register(fragment: VocabularyFragmentV1): VocabularyBinding {
    validateFragmentShape(fragment);

    const sameHash = this.fragmentsByContentHash.get(fragment.contentHash);
    if (sameHash !== undefined) {
      if (!equalFragment(sameHash, fragment)) {
        throw new Error(`LMAO_VOCABULARY_CONTENT_HASH_COLLISION: ${fragment.contentHash}`);
      }
      return this.bindExisting(fragment);
    }

    // Validate the whole fragment before publishing any dense entry.
    const localOrdinals = new Map<number, number>();
    for (let ordinal = 0; ordinal < fragment.ids.length; ordinal++) {
      const id = fragment.ids[ordinal]!;
      const earlierOrdinal = localOrdinals.get(id);
      if (earlierOrdinal !== undefined && !equalRecord(fragment, earlierOrdinal, fragment, ordinal)) {
        throw new Error(`LMAO_VOCABULARY_ID_COLLISION: stable ID ${id} within fragment ${fragment.contentHash}`);
      }
      localOrdinals.set(id, ordinal);

      const denseIndex = this.denseByStableId.get(id);
      if (
        denseIndex !== undefined &&
        (this.denseKindTags[denseIndex] !== fragment.kindTags[ordinal] ||
          !equalBytes(this.denseRecords[denseIndex]!, recordBytes(fragment, ordinal)))
      ) {
        throw new Error(`LMAO_VOCABULARY_ID_COLLISION: stable ID ${id} in fragment ${fragment.contentHash}`);
      }
    }

    const generationStart = this.denseIds.length;
    const binding = new Uint32Array(fragment.ids.length);
    for (let ordinal = 0; ordinal < fragment.ids.length; ordinal++) {
      const id = fragment.ids[ordinal]!;
      let denseIndex = this.denseByStableId.get(id);
      if (denseIndex === undefined) {
        denseIndex = this.denseIds.length;
        this.denseByStableId.set(id, denseIndex);
        this.denseIds.push(id);
        this.denseKindTags.push(fragment.kindTags[ordinal]!);
        // The emitting module owns the fragment, so retaining its byte view is valid.
        this.denseRecords.push(recordBytes(fragment, ordinal));
      }
      binding[ordinal] = denseIndex;
    }
    this.fragmentsByContentHash.set(fragment.contentHash, fragment);
    if (this.denseIds.length !== generationStart) this.generationEnds.push(this.denseIds.length);
    return binding;
  }

  private bindExisting(fragment: VocabularyFragmentV1): VocabularyBinding {
    const binding = new Uint32Array(fragment.ids.length);
    for (let ordinal = 0; ordinal < fragment.ids.length; ordinal++) {
      const denseIndex = this.denseByStableId.get(fragment.ids[ordinal]!);
      if (denseIndex === undefined) throw new Error('Equivalent fragment lost a registered stable ID');
      binding[ordinal] = denseIndex;
    }
    return binding;
  }
}

function validateFragmentShape(fragment: VocabularyFragmentV1): void {
  if (fragment.schemaVersion !== 1 || fragment.idAlgorithm !== 'sha256-24-v1') {
    throw new Error('LMAO_VOCABULARY_ABI_UNAVAILABLE');
  }
  if (fragment.ids.length !== fragment.kindTags.length || fragment.offsets.length !== fragment.ids.length + 1) {
    throw new Error('LMAO_VOCABULARY_FRAGMENT_INVALID: parallel lengths differ');
  }
  if (fragment.offsets[0] !== 0 || fragment.offsets[fragment.offsets.length - 1] !== fragment.utf8.length) {
    throw new Error('LMAO_VOCABULARY_FRAGMENT_INVALID: offsets do not cover UTF-8 bytes');
  }
  for (let ordinal = 0; ordinal < fragment.ids.length; ordinal++) {
    const id = fragment.ids[ordinal]!;
    if (id === 0 || id > U24_MAX) throw new Error(`LMAO_VOCABULARY_FRAGMENT_INVALID: stable ID ${id}`);
    const kindTag = fragment.kindTags[ordinal]!;
    if (kindTag !== 1 && kindTag !== 2) throw new Error(`LMAO_VOCABULARY_FRAGMENT_INVALID: kind ${kindTag}`);
    if (fragment.offsets[ordinal]! > fragment.offsets[ordinal + 1]!) {
      throw new Error('LMAO_VOCABULARY_FRAGMENT_INVALID: offsets are not monotonic');
    }
  }
}

function recordBytes(fragment: VocabularyFragmentV1, ordinal: number): Uint8Array {
  return fragment.utf8.subarray(fragment.offsets[ordinal]!, fragment.offsets[ordinal + 1]!);
}

function equalRecord(
  left: VocabularyFragmentV1,
  leftOrdinal: number,
  right: VocabularyFragmentV1,
  rightOrdinal: number,
): boolean {
  return (
    left.kindTags[leftOrdinal] === right.kindTags[rightOrdinal] &&
    equalBytes(recordBytes(left, leftOrdinal), recordBytes(right, rightOrdinal))
  );
}

function equalBytes(left: Uint8Array, right: Uint8Array): boolean {
  if (left.length !== right.length) return false;
  for (let index = 0; index < left.length; index++) if (left[index] !== right[index]) return false;
  return true;
}

function equalFragment(left: VocabularyFragmentV1, right: VocabularyFragmentV1): boolean {
  if (
    left.schemaVersion !== right.schemaVersion ||
    left.idAlgorithm !== right.idAlgorithm ||
    left.ids.length !== right.ids.length ||
    left.utf8.length !== right.utf8.length
  ) {
    return false;
  }
  for (let ordinal = 0; ordinal < left.ids.length; ordinal++) {
    if (left.ids[ordinal] !== right.ids[ordinal] || !equalRecord(left, ordinal, right, ordinal)) return false;
  }
  return true;
}

function stableIdForOrdinal(ordinal: number): number {
  return ((ordinal * 4_099 + 104_729) % U24_MAX) + 1;
}

function makeFragment(label: string, start: number, end: number): VocabularyFragmentV1 {
  const count = end - start;
  const ids = new Uint32Array(count);
  const kindTags = new Uint8Array(count);
  const records = new Array<Uint8Array>(count);
  const offsets = new Int32Array(count + 1);
  let utf8Length = 0;
  for (let ordinal = 0; ordinal < count; ordinal++) {
    const globalOrdinal = start + ordinal;
    ids[ordinal] = stableIdForOrdinal(globalOrdinal);
    kindTags[ordinal] = (globalOrdinal & 1) + 1;
    const bytes = encoder.encode(`compiler-vocabulary-${globalOrdinal}`);
    records[ordinal] = bytes;
    utf8Length += bytes.length;
    offsets[ordinal + 1] = utf8Length;
  }
  const utf8 = new Uint8Array(utf8Length);
  for (let ordinal = 0; ordinal < records.length; ordinal++) utf8.set(records[ordinal]!, offsets[ordinal]!);
  return {
    schemaVersion: 1,
    idAlgorithm: 'sha256-24-v1',
    contentHash: `modeled-content-${label}-${start}-${end}`,
    ids,
    kindTags,
    utf8,
    offsets,
  };
}

function installRegistrationCallback(): ModeledVocabularyRegistry {
  const registry = new ModeledVocabularyRegistry();
  (globalThis as unknown as Record<symbol, RegisterVocabulary>)[REGISTER_VOCABULARY] = (fragment) =>
    registry.register(fragment);
  return registry;
}

function emittedRegister(fragment: VocabularyFragmentV1): VocabularyBinding {
  const callback = (globalThis as unknown as Record<symbol, RegisterVocabulary>)[REGISTER_VOCABULARY];
  if (callback === undefined) throw new Error('LMAO_VOCABULARY_ABI_UNAVAILABLE');
  return callback(fragment);
}

function registerInitial(fragment: VocabularyFragmentV1): RegistrationObservation {
  const registry = installRegistrationCallback();
  return { registry, binding: emittedRegister(fragment) };
}

function registerDuplicate(fragment: VocabularyFragmentV1): RegistrationObservation {
  const registry = installRegistrationCallback();
  emittedRegister(fragment);
  return { registry, binding: emittedRegister(fragment) };
}

function registerOverlap(first: VocabularyFragmentV1, second: VocabularyFragmentV1): RegistrationObservation {
  const registry = installRegistrationCallback();
  emittedRegister(first);
  return { registry, binding: emittedRegister(second) };
}

function registerLazy(initial: VocabularyFragmentV1, lazy: VocabularyFragmentV1): RegistrationObservation {
  const registry = installRegistrationCallback();
  let binding = emittedRegister(initial);
  if (lazy.ids.length !== 0) binding = emittedRegister(lazy);
  return { registry, binding };
}

function checksum(registry: ModeledVocabularyRegistry): number {
  let value = 2_166_136_261;
  for (let denseIndex = 0; denseIndex < registry.denseIds.length; denseIndex++) {
    value = Math.imul(value ^ registry.denseIds[denseIndex]!, 16_777_619) >>> 0;
    value = Math.imul(value ^ registry.denseKindTags[denseIndex]!, 16_777_619) >>> 0;
    const bytes = registry.denseRecords[denseIndex]!;
    for (let offset = 0; offset < bytes.length; offset++) {
      value = Math.imul(value ^ bytes[offset]!, 16_777_619) >>> 0;
    }
  }
  return value;
}

function assertEqual(actual: unknown, expected: unknown, label: string): void {
  if (actual !== expected) throw new Error(`${label}: expected ${String(expected)}, received ${String(actual)}`);
}

function preflight(): string[] {
  const whole = makeFragment('preflight-whole', 0, 16);
  const initial = makeFragment('preflight-initial', 0, 10);
  const overlap = makeFragment('preflight-overlap', 6, 16);
  const registry = installRegistrationCallback();
  const initialBinding = emittedRegister(initial);
  const duplicateBinding = emittedRegister(initial);
  const overlapBinding = emittedRegister(overlap);

  for (let ordinal = 0; ordinal < initial.ids.length; ordinal++) {
    assertEqual(initialBinding[ordinal], ordinal, `initial binding[${ordinal}]`);
    assertEqual(duplicateBinding[ordinal], ordinal, `duplicate binding[${ordinal}]`);
  }
  for (let ordinal = 0; ordinal < overlap.ids.length; ordinal++) {
    assertEqual(overlapBinding[ordinal], ordinal + 6, `overlap binding[${ordinal}]`);
  }
  assertEqual(registry.denseIds.length, 16, 'idempotent/overlap dense count');
  assertEqual(registry.generationEnds.length, 2, 'dictionary generation count');
  assertEqual(registry.generationEnds[0], 10, 'initial generation prefix');
  assertEqual(registry.generationEnds[1], 16, 'lazy generation prefix');
  for (let ordinal = 0; ordinal < whole.ids.length; ordinal++) {
    assertEqual(registry.denseIds[ordinal], whole.ids[ordinal], `stable ID ${ordinal}`);
    assertEqual(registry.denseKindTags[ordinal], whole.kindTags[ordinal], `kind tag ${ordinal}`);
    if (!equalBytes(registry.denseRecords[ordinal]!, recordBytes(whole, ordinal))) {
      throw new Error(`canonical UTF-8 mismatch at dense index ${ordinal}`);
    }
  }

  const diagnostics: string[] = [];
  const idCollision = makeFragment('forced-id-collision', 3, 4);
  idCollision.utf8.fill(0x78);
  try {
    emittedRegister(idCollision);
  } catch (error) {
    diagnostics.push(error instanceof Error ? error.message : String(error));
  }
  assertEqual(
    diagnostics[0],
    `LMAO_VOCABULARY_ID_COLLISION: stable ID ${whole.ids[3]} in fragment modeled-content-forced-id-collision-3-4`,
    'stable-ID collision diagnostic',
  );

  const hashCollision: VocabularyFragmentV1 = {
    ...makeFragment('forced-hash-collision', 0, 10),
    contentHash: initial.contentHash,
  };
  hashCollision.utf8.fill(0x79);
  try {
    emittedRegister(hashCollision);
  } catch (error) {
    diagnostics.push(error instanceof Error ? error.message : String(error));
  }
  assertEqual(
    diagnostics[1],
    `LMAO_VOCABULARY_CONTENT_HASH_COLLISION: ${initial.contentHash}`,
    'content-hash collision diagnostic',
  );
  return diagnostics;
}

function sortedLookup(fragment: VocabularyFragmentV1): { ids: Uint32Array; dense: Uint32Array } {
  const pairs = Array.from(fragment.ids, (id, dense) => ({ id, dense })).sort((left, right) => left.id - right.id);
  return {
    ids: Uint32Array.from(pairs, (pair) => pair.id),
    dense: Uint32Array.from(pairs, (pair) => pair.dense),
  };
}

function coldStableIdRemap(id: number, ids: Uint32Array, dense: Uint32Array): number {
  let low = 0;
  let high = ids.length - 1;
  while (low <= high) {
    const middle = (low + high) >>> 1;
    const candidate = ids[middle]!;
    if (candidate === id) return dense[middle]!;
    if (candidate < id) low = middle + 1;
    else high = middle - 1;
  }
  return -1;
}

const diagnostics = preflight();
console.error('Vocabulary registration benchmark (target ABI model)');
console.error(`quick=${QUICK}; collision preflight=${diagnostics.join(' | ')}`);

for (const size of SIZES) {
  const whole = makeFragment(`whole-${size}`, 0, size);
  const overlapStart = Math.floor(size / 2);
  const firstEnd = Math.max(1, Math.ceil(size * 0.75));
  const first = makeFragment(`overlap-a-${size}`, 0, firstEnd);
  const second = makeFragment(`overlap-b-${size}`, overlapStart, size);
  const lazySplit = Math.max(1, Math.ceil(size / 2));
  const lazyInitial = makeFragment(`generation-initial-${size}`, 0, lazySplit);
  const lazyAppend = makeFragment(`generation-lazy-${size}`, lazySplit, size);

  const observations = [
    registerInitial(whole),
    registerDuplicate(whole),
    registerOverlap(first, second),
    registerLazy(lazyInitial, lazyAppend),
  ];
  const expectedChecksum = checksum(observations[0]!.registry);
  for (const observation of observations) {
    assertEqual(observation.registry.denseIds.length, size, `dense size ${size}`);
    assertEqual(checksum(observation.registry), expectedChecksum, `semantic checksum ${size}`);
  }

  const dictionaryBytes = whole.utf8.byteLength + whole.offsets.byteLength;
  const bindingBytes = whole.ids.length * Uint32Array.BYTES_PER_ELEMENT;
  console.error(
    `size=${size} fragment-bytes=${whole.ids.byteLength + whole.kindTags.byteLength + dictionaryBytes} dictionary-bytes=${dictionaryBytes} binding-bytes=${bindingBytes}`,
  );

  summary(() => {
    group(`startup registration — vocabulary ${size}`, () => {
      bench('baseline model: initial Symbol.for typed-array fragment', () => {
        const observation = registerInitial(whole);
        do_not_optimize(observation.binding);
      });
      bench('candidate workload: idempotent duplicate fragment', () => {
        const observation = registerDuplicate(whole);
        do_not_optimize(observation.binding);
      });
      bench('candidate workload: overlapping fragments', () => {
        const observation = registerOverlap(first, second);
        do_not_optimize(observation.binding);
      });
      bench('candidate workload: lazy append-only generation', () => {
        const observation = registerLazy(lazyInitial, lazyAppend);
        do_not_optimize(observation.binding);
      });
    });
  });

  const lookupRegistry = installRegistrationCallback();
  const binding = emittedRegister(whole);
  const sorted = sortedLookup(whole);
  for (let ordinal = 0; ordinal < size; ordinal++) {
    assertEqual(
      coldStableIdRemap(whole.ids[ordinal]!, sorted.ids, sorted.dense),
      binding[ordinal],
      `cold remap ${ordinal}`,
    );
  }

  summary(() => {
    group(`warmed dense binding — vocabulary ${size}`, () => {
      bench('baseline: stable-ID cold binary remap', () => {
        let sum = 0;
        for (let iteration = 0; iteration < LOOKUPS_PER_INVOCATION; iteration++) {
          const ordinal = iteration % size;
          sum += coldStableIdRemap(whole.ids[ordinal]!, sorted.ids, sorted.dense);
        }
        do_not_optimize(sum);
      });
      bench('candidate: direct binding[fragmentLocalOrdinal]', () => {
        let sum = 0;
        for (let iteration = 0; iteration < LOOKUPS_PER_INVOCATION; iteration++) {
          sum += binding[iteration % size]!;
        }
        do_not_optimize(sum);
      });
    });
  });

  do_not_optimize(lookupRegistry);
}

await run({ format: FORMAT, colors: FORMAT === 'mitata', throw: true });
