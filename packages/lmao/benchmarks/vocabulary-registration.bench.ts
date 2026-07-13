import { bench, do_not_optimize, group, run, summary } from 'mitata';
import {
  getVocabularyGeneration,
  registerVocabularyFragment,
  VocabularyRegistrationError,
  type VocabularyFragment,
} from '../src/lib/vocabularyRegistry.js';
import {
  benchmarkVocabularyStableId,
  createBenchmarkVocabularyFragment,
} from './vocabularyFixture.js';

const FULL_SIZES: readonly number[] = Object.freeze([1, 16, 256, 65_535]);
const QUICK_SIZES: readonly number[] = Object.freeze([1, 16, 256, 1_024]);
const QUICK = process.argv.includes('--quick');
const SIZES = QUICK ? QUICK_SIZES : FULL_SIZES;
const LOOKUPS_PER_INVOCATION = QUICK ? 1_024 : 65_536;
const FORMAT = parseFormat(argumentValue('--format'));

type RunFormat = 'json' | 'markdown' | 'mitata' | 'quiet';

function argumentValue(name: string): string | undefined {
  const prefix = `${name}=`;
  for (let index = 0; index < process.argv.length; index++) {
    const argument = process.argv[index];
    if (argument === undefined) continue;
    if (argument.startsWith(prefix)) return argument.slice(prefix.length);
    if (argument === name) return process.argv[index + 1];
  }
  return undefined;
}

function parseFormat(value: string | undefined): RunFormat {
  if (value === undefined) return 'mitata';
  if (value === 'json' || value === 'markdown' || value === 'mitata' || value === 'quiet') return value;
  throw new Error(`Unknown Mitata format: ${value}`);
}

function requireNumber(values: Uint32Array, index: number, label: string): number {
  const value = values[index];
  if (value === undefined) throw new RangeError(`${label} index ${index} is out of range`);
  return value;
}

function collisionFreeTexts(label: string, size: number, reserved: Set<number>): string[] {
  const texts = new Array<string>(size);
  for (let ordinal = 0; ordinal < size; ordinal++) {
    let nonce = 0;
    while (true) {
      const text = `${label}/literal-${ordinal}/nonce-${nonce}`;
      let id: number;
      try {
        id = benchmarkVocabularyStableId(text);
      } catch (error) {
        if (!(error instanceof Error) || !error.message.includes('stable ID zero')) throw error;
        nonce++;
        continue;
      }
      if (reserved.has(id)) {
        nonce++;
        continue;
      }
      reserved.add(id);
      texts[ordinal] = text;
      break;
    }
  }
  return texts;
}

function sortedLookup(fragment: VocabularyFragment, binding: Uint32Array): {
  readonly ids: Uint32Array;
  readonly dense: Uint32Array;
} {
  const pairs = Array.from(fragment.ids, (id, ordinal) => ({ id, dense: requireNumber(binding, ordinal, 'binding') }));
  pairs.sort((left, right) => left.id - right.id);
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
    const candidate = requireNumber(ids, middle, 'sorted stable IDs');
    if (candidate === id) return requireNumber(dense, middle, 'sorted dense indices');
    if (candidate < id) low = middle + 1;
    else high = middle - 1;
  }
  throw new Error(`Stable vocabulary ID ${id} is not registered`);
}

function bindingChecksum(binding: Uint32Array): number {
  let checksum = 2_166_136_261;
  for (const denseIndex of binding) checksum = Math.imul(checksum ^ denseIndex, 16_777_619) >>> 0;
  return checksum;
}

function assertProductionRegistration(
  fragment: VocabularyFragment,
  binding: Uint32Array,
  overlap: VocabularyFragment,
  overlapBinding: Uint32Array,
): void {
  const duplicate = registerVocabularyFragment(fragment);
  if (duplicate !== binding) throw new Error('Duplicate production registration did not return the cached binding');
  for (let ordinal = 0; ordinal < overlap.ids.length; ordinal++) {
    const overlapDense = requireNumber(overlapBinding, ordinal, 'overlap binding');
    const wholeOrdinal = fragment.ids.indexOf(requireNumber(overlap.ids, ordinal, 'overlap stable IDs'));
    if (wholeOrdinal < 0 || overlapDense !== requireNumber(binding, wholeOrdinal, 'whole binding')) {
      throw new Error(`Overlap binding mismatch at ordinal ${ordinal}`);
    }
  }
  const generation = getVocabularyGeneration();
  for (let ordinal = 0; ordinal < fragment.ids.length; ordinal++) {
    const denseIndex = requireNumber(binding, ordinal, 'binding');
    if (requireNumber(generation.ids, denseIndex, 'generation stable IDs') !== requireNumber(fragment.ids, ordinal, 'fragment stable IDs')) {
      throw new Error(`Generation stable ID mismatch at ordinal ${ordinal}`);
    }
  }
}

function validateHashFailure(fragment: VocabularyFragment): string {
  const mismatched = { ...fragment, contentHash: '0'.repeat(64) };
  try {
    registerVocabularyFragment(mismatched);
  } catch (error) {
    if (error instanceof VocabularyRegistrationError && error.message.includes('LMAO_VOCABULARY_CONTENT_HASH_MISMATCH')) {
      return error.message;
    }
    throw error;
  }
  throw new Error('Production vocabulary registry accepted a mismatched content hash');
}

const reservedIds = new Set<number>(getVocabularyGeneration().ids);
const hashProbe = createBenchmarkVocabularyFragment(collisionFreeTexts('hash-preflight', 1, reservedIds));
const hashDiagnostic = validateHashFailure(hashProbe);
console.error(`Vocabulary registration benchmark (production ABI); quick=${QUICK}; preflight=${hashDiagnostic}`);

for (const size of SIZES) {
  const texts = collisionFreeTexts(`production-size-${size}`, size, reservedIds);
  const fragment = createBenchmarkVocabularyFragment(texts);
  const binding = registerVocabularyFragment(fragment);
  const overlapStart = Math.floor(size / 2);
  const overlap = createBenchmarkVocabularyFragment(texts.slice(overlapStart));
  const overlapBinding = registerVocabularyFragment(overlap);
  assertProductionRegistration(fragment, binding, overlap, overlapBinding);
  const sorted = sortedLookup(fragment, binding);
  const expectedChecksum = bindingChecksum(binding);
  if (bindingChecksum(registerVocabularyFragment(fragment)) !== expectedChecksum) {
    throw new Error(`Duplicate registration checksum mismatch for size ${size}`);
  }
  const generation = getVocabularyGeneration();
  const fragmentBytes = fragment.ids.byteLength + fragment.kindTags.byteLength + fragment.utf8.byteLength + fragment.offsets.byteLength;
  console.error(
    `size=${size} fragment-bytes=${fragmentBytes} binding-bytes=${binding.byteLength} generation=${generation.generation}`,
  );

  group(`production vocabulary registration | size=${size}`, () => {
    summary(() => {
      bench('production/duplicate-fragment-registration', () => {
        do_not_optimize(registerVocabularyFragment(fragment));
      }).baseline(true);
      bench('production/duplicate-overlap-fragment-registration', () => {
        do_not_optimize(registerVocabularyFragment(overlap));
      });
    });
  });

  group(`vocabulary lookup | size=${size} lookups=${LOOKUPS_PER_INVOCATION}`, () => {
    summary(() => {
      bench('cold/stable-id-binary-search', () => {
        let sum = 0;
        for (let iteration = 0; iteration < LOOKUPS_PER_INVOCATION; iteration++) {
          const ordinal = iteration % size;
          sum += coldStableIdRemap(requireNumber(fragment.ids, ordinal, 'fragment stable IDs'), sorted.ids, sorted.dense);
        }
        do_not_optimize(sum);
      });
      bench('hot/direct-dense-binding', () => {
        let sum = 0;
        for (let iteration = 0; iteration < LOOKUPS_PER_INVOCATION; iteration++) {
          sum += requireNumber(binding, iteration % size, 'binding');
        }
        do_not_optimize(sum);
      }).baseline(true);
    });
  });
}

await run({ format: FORMAT, colors: FORMAT === 'mitata', throw: true });
