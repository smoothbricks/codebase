// Generated from the canonical root lmao.vocabulary.json manifest.
// Do not edit; run bun tooling/lmao-vocabulary.mjs sync.

import { registerVocabularyFragment } from '../lib/vocabularyRegistry.js';

export const VOCABULARY_SCHEMA_VERSION = 1 as const;
export const VOCABULARY_ID_ALGORITHM = "sha256-24-v1" as const;
export const VOCABULARY_CONTENT_HASH = "1f92e229d16bd3470e7dc1f15673f689f9350886788f2475a18236e228f6c6ae" as const;
export const VOCABULARY_IDS = new Uint32Array([273228,377410,2878065,7140773,8012750,9474871,12761596,13676444,13700822,15317875,15419721,16094209,16369894]);
export const VOCABULARY_KIND_TAGS = new Uint8Array([1,1,1,1,1,1,1,1,1,1,2,1,1]);
export const VOCABULARY_VALUES: readonly string[] = Object.freeze(["No items to validate","Processing complete","Processing items","Received items to process","Running validation","Starting data processing","Validation failed","Validation passed","json-sensitive <>&   ","literal braces: {ok} for {region}","loaded {userId} in {elapsedMs}ms","native fixture log","validate-items"]);
export const VOCABULARY_UTF8 = new Uint8Array([78,111,32,105,116,101,109,115,32,116,111,32,118,97,108,105,100,97,116,101,80,114,111,99,101,115,115,105,110,103,32,99,111,109,112,108,101,116,101,80,114,111,99,101,115,115,105,110,103,32,105,116,101,109,115,82,101,99,101,105,118,101,100,32,105,116,101,109,115,32,116,111,32,112,114,111,99,101,115,115,82,117,110,110,105,110,103,32,118,97,108,105,100,97,116,105,111,110,83,116,97,114,116,105,110,103,32,100,97,116,97,32,112,114,111,99,101,115,115,105,110,103,86,97,108,105,100,97,116,105,111,110,32,102,97,105,108,101,100,86,97,108,105,100,97,116,105,111,110,32,112,97,115,115,101,100,106,115,111,110,45,115,101,110,115,105,116,105,118,101,32,60,62,38,32,226,128,168,226,128,169,108,105,116,101,114,97,108,32,98,114,97,99,101,115,58,32,123,111,107,125,32,102,111,114,32,123,114,101,103,105,111,110,125,108,111,97,100,101,100,32,123,117,115,101,114,73,100,125,32,105,110,32,123,101,108,97,112,115,101,100,77,115,125,109,115,110,97,116,105,118,101,32,102,105,120,116,117,114,101,32,108,111,103,118,97,108,105,100,97,116,101,45,105,116,101,109,115]);
export const VOCABULARY_UTF8_OFFSETS = new Int32Array([0,20,39,55,80,98,122,139,156,181,214,246,264,278]);
export const VOCABULARY_DENSE_INDICES = new Uint32Array([1,8,7,10,2,9,11,5,3,0,12,4,6]);
export const VOCABULARY_FRAGMENT_UTF8 = new Uint8Array([19,0,0,0,80,114,111,99,101,115,115,105,110,103,32,99,111,109,112,108,101,116,101,0,0,25,0,0,0,106,115,111,110,45,115,101,110,115,105,116,105,118,101,32,60,62,38,32,226,128,168,226,128,169,0,0,17,0,0,0,86,97,108,105,100,97,116,105,111,110,32,112,97,115,115,101,100,0,0,32,0,0,0,108,111,97,100,101,100,32,123,117,115,101,114,73,100,125,32,105,110,32,123,101,108,97,112,115,101,100,77,115,125,109,115,2,0,6,0,117,115,101,114,73,100,6,0,117,115,101,114,73,100,9,0,101,108,97,112,115,101,100,77,115,9,0,101,108,97,112,115,101,100,77,115,16,0,0,0,80,114,111,99,101,115,115,105,110,103,32,105,116,101,109,115,0,0,33,0,0,0,108,105,116,101,114,97,108,32,98,114,97,99,101,115,58,32,123,111,107,125,32,102,111,114,32,123,114,101,103,105,111,110,125,1,0,6,0,114,101,103,105,111,110,6,0,114,101,103,105,111,110,18,0,0,0,110,97,116,105,118,101,32,102,105,120,116,117,114,101,32,108,111,103,0,0,24,0,0,0,83,116,97,114,116,105,110,103,32,100,97,116,97,32,112,114,111,99,101,115,115,105,110,103,0,0,25,0,0,0,82,101,99,101,105,118,101,100,32,105,116,101,109,115,32,116,111,32,112,114,111,99,101,115,115,0,0,20,0,0,0,78,111,32,105,116,101,109,115,32,116,111,32,118,97,108,105,100,97,116,101,0,0,14,0,0,0,118,97,108,105,100,97,116,101,45,105,116,101,109,115,0,0,18,0,0,0,82,117,110,110,105,110,103,32,118,97,108,105,100,97,116,105,111,110,0,0,17,0,0,0,86,97,108,105,100,97,116,105,111,110,32,102,97,105,108,101,100,0,0]);
export const VOCABULARY_FRAGMENT_OFFSETS = new Int32Array([0,25,56,79,155,177,232,256,286,317,343,363,387,410]);

function assertVocabularyStructure(): void {
  const count = VOCABULARY_IDS.length;
  if (VOCABULARY_KIND_TAGS.length !== count || VOCABULARY_DENSE_INDICES.length !== count || VOCABULARY_FRAGMENT_OFFSETS.length !== count + 1) throw new Error('invalid generated vocabulary: parallel lengths differ');
  if (VOCABULARY_UTF8_OFFSETS.length !== VOCABULARY_VALUES.length + 1 || VOCABULARY_UTF8_OFFSETS[0] !== 0 || VOCABULARY_UTF8_OFFSETS.at(-1) !== VOCABULARY_UTF8.length) throw new Error('invalid generated vocabulary: dense UTF-8 coverage');
  if (VOCABULARY_FRAGMENT_OFFSETS[0] !== 0 || VOCABULARY_FRAGMENT_OFFSETS.at(-1) !== VOCABULARY_FRAGMENT_UTF8.length) throw new Error('invalid generated vocabulary: fragment UTF-8 coverage');
  for (let index = 0; index < count; index++) {
    const id = VOCABULARY_IDS[index];
    const kind = VOCABULARY_KIND_TAGS[index];
    if (id === 0 || id > 0xffffff || (index > 0 && VOCABULARY_IDS[index - 1] >= id)) throw new Error('invalid generated vocabulary: IDs');
    if (kind !== 1 && kind !== 2) throw new Error('invalid generated vocabulary: kind tag');
    if (VOCABULARY_DENSE_INDICES[index] >= VOCABULARY_VALUES.length) throw new Error('invalid generated vocabulary: dense index');
    if (VOCABULARY_FRAGMENT_OFFSETS[index] > VOCABULARY_FRAGMENT_OFFSETS[index + 1]) throw new Error('invalid generated vocabulary: fragment offsets');
  }
  for (let index = 1; index < VOCABULARY_UTF8_OFFSETS.length; index++) if (VOCABULARY_UTF8_OFFSETS[index - 1] > VOCABULARY_UTF8_OFFSETS[index]) throw new Error('invalid generated vocabulary: dense offsets');
}
assertVocabularyStructure();

function vocabularyIndex(id: number): number {
  let low = 0;
  let high = VOCABULARY_IDS.length - 1;
  while (low <= high) {
    const middle = (low + high) >>> 1;
    const candidate = VOCABULARY_IDS[middle];
    if (candidate === id) return middle;
    if (candidate < id) low = middle + 1;
    else high = middle - 1;
  }
  return -1;
}

export function lookupVocabularyDenseIndex(id: number): number { const index = vocabularyIndex(id); return index < 0 ? -1 : VOCABULARY_DENSE_INDICES[index]; }
export function lookupVocabularyValue(id: number): string | undefined { const index = vocabularyIndex(id); return index < 0 ? undefined : VOCABULARY_VALUES[VOCABULARY_DENSE_INDICES[index]]; }
export function lookupVocabularyKindTag(id: number): 1 | 2 | undefined { const index = vocabularyIndex(id); return index < 0 ? undefined : VOCABULARY_KIND_TAGS[index] as 1 | 2; }

export const VOCABULARY_FRAGMENT = { schemaVersion: VOCABULARY_SCHEMA_VERSION, idAlgorithm: VOCABULARY_ID_ALGORITHM, contentHash: VOCABULARY_CONTENT_HASH, ids: VOCABULARY_IDS, kindTags: VOCABULARY_KIND_TAGS, utf8: VOCABULARY_FRAGMENT_UTF8, offsets: VOCABULARY_FRAGMENT_OFFSETS } as const;
export const VOCABULARY_BINDING = registerVocabularyFragment(VOCABULARY_FRAGMENT);
