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
const replacements: ReplacementsData = JSON.parse(readFileSync(dataPath, 'utf-8')) as ReplacementsData;

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
  return replacements[packageName] ?? null;
}
