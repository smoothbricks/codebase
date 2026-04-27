/**
 * Renovate replacement mapping lookup
 *
 * Provides static lookup of known package replacements from Renovate's curated
 * replacements.json data (filtered to npm datasource entries only).
 */

import { readFileSync } from 'node:fs';
import { join } from 'node:path';

export interface ReplacementSuggestion {
  replacementName: string;
  replacementVersion: string;
  description: string;
}

type ReplacementsData = Record<string, ReplacementSuggestion>;

const dataPath = join(import.meta.dirname, '..', 'data', 'npm-replacements.json');

let _replacements: ReplacementsData | null = null;

function getReplacements(): ReplacementsData {
  if (!_replacements) {
    _replacements = JSON.parse(readFileSync(dataPath, 'utf-8')) as ReplacementsData;
  }
  return _replacements;
}

/**
 * Look up a replacement suggestion for a deprecated package.
 *
 * Uses a static mapping derived from Renovate's curated replacements.json
 * (npm datasource entries only).
 *
 * @param packageName - npm package name to look up
 * @returns Replacement suggestion if found, null otherwise
 */
export function findReplacement(packageName: string): ReplacementSuggestion | null {
  return getReplacements()[packageName] ?? null;
}
