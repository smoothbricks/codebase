import { getVocabularyDictionaryPrefix } from '../../arrow/vocabularyDictionary.js';
import { getVocabularyGeneration } from '../../vocabularyRegistry.js';

const generation = getVocabularyGeneration();
const prefix = getVocabularyDictionaryPrefix(generation);

console.log(
  JSON.stringify({
    generation: generation.generation,
    ids: Array.from(generation.ids),
    prefixLength: prefix.length,
    prefixValues: Array.from(prefix.column),
    emptyDenseIndex: prefix.valueToDenseIndex.get(''),
  }),
);
