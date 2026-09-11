// Where a project's Wrangler configuration lives, and how its file formats become one model.
//
// Wrangler reads whichever of `wrangler.jsonc`, `wrangler.json` and `wrangler.toml` it finds
// first and says nothing about the rest: with a `.jsonc` and a `.toml` side by side, wrangler
// 4.131.0 deploys the `.jsonc` silently, whichever of the two the repo is actually editing.
// Discovery here follows the same precedence but refuses the pick, naming every file it found.
//
// Both JSON spellings go through the JSONC parser because wrangler does — comments and trailing
// commas parse in `wrangler.json` too (verified against wrangler 4.131.0) — so the format only
// ever decides which parser runs. Past this module there is one model and no config text.

import { existsSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import { getStaticTOMLValue, parseTOML } from 'toml-eslint-parser';
import typia from 'typia';
import type { WranglerDocument } from './stage.js';

/** Which parser a source config needs; `wrangler.json` and `wrangler.jsonc` share one. */
export type WranglerConfigFormat = 'jsonc' | 'toml';

/** Wrangler's own precedence: the first of these that exists is the one it reads. */
export const WRANGLER_SOURCE_CONFIG_FILES = ['wrangler.jsonc', 'wrangler.json', 'wrangler.toml'] as const;

export interface WranglerSourceConfig {
  /** The file wrangler itself would read, ready to hand back to it as `--config`. */
  path: string;
  document: WranglerDocument;
}

const isWranglerDocument = typia.createIs<WranglerDocument>();

/**
 * The one source config this project declares. Two configs is a refusal, not a precedence
 * exercise: the repo would deploy whichever file wrangler prefers, which is not necessarily the
 * one anybody is editing.
 */
export function discoverWranglerSourceConfig(cwd: string): { path: string; format: WranglerConfigFormat } {
  const found = WRANGLER_SOURCE_CONFIG_FILES.filter((file) => existsSync(join(cwd, file)));
  const [first] = found;
  if (first === undefined) {
    throw new Error(
      `${cwd} declares no Wrangler configuration (${WRANGLER_SOURCE_CONFIG_FILES.join(', ')}); pass --config for a build-generated flat configuration.`,
    );
  }
  if (found.length > 1) {
    throw new Error(
      `${cwd} declares more than one Wrangler configuration: ${found.join(', ')}. Wrangler would read ${first} and ignore the rest without saying so; keep exactly one.`,
    );
  }
  return { path: join(cwd, first), format: first === 'wrangler.toml' ? 'toml' : 'jsonc' };
}

/** The discovered source config, parsed into the model every stage derivation reads. */
export function readWranglerSourceConfig(cwd: string): WranglerSourceConfig {
  const { path, format } = discoverWranglerSourceConfig(cwd);
  return { path, document: parseWranglerConfig(path, readFileSync(path, 'utf8'), format) };
}

/** One document out of either format. The text is never handed on: everything downstream reads data. */
export function parseWranglerConfig(path: string, text: string, format: WranglerConfigFormat): WranglerDocument {
  const value = parseConfigText(path, text, format);
  // TOML carries values JSON does not, and the temporary per-stage config is JSON whatever the
  // source format was. Name them here rather than let JSON.stringify shift a date by its timezone
  // or write `null` for an infinity halfway through a deploy.
  if (format === 'toml') assertJsonRepresentable(value, '', path);
  if (!isWranglerDocument(value)) {
    throw new Error(
      `${path} is not a Wrangler configuration document: expected an object, optionally with env blocks.`,
    );
  }
  return value;
}

function parseConfigText(path: string, text: string, format: WranglerConfigFormat): unknown {
  try {
    return format === 'toml' ? getStaticTOMLValue(parseTOML(text)) : Bun.JSONC.parse(text);
  } catch (error) {
    const detail = error instanceof Error ? error.message : String(error);
    throw new Error(`${path} is not valid ${format === 'toml' ? 'TOML' : 'JSONC'}: ${detail}`);
  }
}

function assertJsonRepresentable(value: unknown, path: string, file: string): void {
  if (value === null || typeof value === 'string' || typeof value === 'boolean') return;
  if (typeof value === 'number') {
    if (Number.isFinite(value)) return;
    throw new Error(`${file} declares ${value} at ${path}, which JSON cannot carry; write a finite number.`);
  }
  if (Array.isArray(value)) {
    for (const [index, item] of value.entries()) {
      assertJsonRepresentable(item, `${path}[${index}]`, file);
    }
    return;
  }
  if (isPlainObject(value)) {
    for (const [key, item] of Object.entries(value)) {
      assertJsonRepresentable(item, path ? `${path}.${key}` : key, file);
    }
    return;
  }
  throw new Error(
    `${file} declares a bare TOML ${value instanceof Date ? 'date or time' : typeof value} at ${path}, which JSON cannot carry; quote it as a string.`,
  );
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  if (typeof value !== 'object' || value === null) return false;
  const prototype: unknown = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}
