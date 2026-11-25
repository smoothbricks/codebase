#!/usr/bin/env node
import { readFile } from 'node:fs/promises';

import { wholeSuiteMinimum } from '../executors/bounded-exec/suite-minimum.js';
import { junitCaseTimes } from '../executors/bounded-exec/verdict.js';
import { BOUNDED_TEST_IDLE_TIMEOUT_MS } from '../test-backstop.js';

// Prints the whole-suite reservation minimum (`wholeSuiteMinimum`) from candidate JUnit XML reports or `--walls-json`,
// so external consumers share bounded-exec's single floor definition. Missing candidate reports (ENOENT) are skipped;
// any other read error or invalid flag exits non-zero naming the path or reason.
const USAGE =
  'usage: smoo-nx-suite-minimum --want <n> [--idle-ms <ms>] [--aging-ms <ms>] [--walls-json <json>] [<junit.xml>...]\n';
const POSITIVE_INT = /^[1-9]\d*$/;

type ParsedArgs =
  | {
      readonly ok: true;
      readonly want: number;
      readonly idleMs: number;
      readonly agingMs: number | null;
      readonly wallsJson: readonly number[] | null;
      readonly paths: readonly string[];
    }
  | { readonly ok: false; readonly error: string };

function parseNonNegativeNumber(
  flag: string,
  raw: string | undefined,
): { readonly ok: true; readonly value: number } | { readonly ok: false; readonly error: string } {
  if (raw === undefined || raw.trim() === '') return { ok: false, error: `${flag} requires a value` };
  const value = Number(raw);
  if (!Number.isFinite(value) || value < 0) {
    return { ok: false, error: `${flag} must be a finite non-negative number, got ${JSON.stringify(raw)}` };
  }
  return { ok: true, value };
}

function parseWallsJson(
  raw: string | undefined,
): { readonly ok: true; readonly walls: readonly number[] } | { readonly ok: false; readonly error: string } {
  if (raw === undefined || raw.trim() === '') return { ok: false, error: '--walls-json requires a value' };
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return { ok: false, error: `--walls-json must be valid JSON, got ${JSON.stringify(raw)}` };
  }
  if (!Array.isArray(parsed)) {
    return { ok: false, error: '--walls-json must be a JSON array of finite non-negative numbers' };
  }
  const walls: number[] = [];
  for (const item of parsed) {
    if (typeof item !== 'number' || !Number.isFinite(item) || item < 0) {
      return { ok: false, error: '--walls-json must contain only finite non-negative numbers' };
    }
    walls.push(item);
  }
  return { ok: true, walls };
}

function parseArgs(argv: readonly string[]): ParsedArgs {
  let want: number | null = null;
  let idleMs = BOUNDED_TEST_IDLE_TIMEOUT_MS;
  let agingMs: number | null = null;
  let wallsJson: readonly number[] | null = null;
  const paths: string[] = [];
  let positionalOnly = false;

  for (let index = 0; index < argv.length; index++) {
    const arg = argv[index];
    if (arg === undefined) continue;
    if (positionalOnly) {
      paths.push(arg);
      continue;
    }
    if (arg === '--') {
      positionalOnly = true;
      continue;
    }
    const readValue = (flag: string): string | undefined => {
      if (arg.startsWith(`${flag}=`)) return arg.slice(flag.length + 1);
      index += 1;
      return argv[index];
    };
    if (arg === '--want' || arg.startsWith('--want=')) {
      const raw = readValue('--want');
      if (raw === undefined || !POSITIVE_INT.test(raw) || !Number.isSafeInteger(Number(raw))) {
        return { ok: false, error: `--want must be a positive integer, got ${JSON.stringify(raw)}` };
      }
      want = Number(raw);
      continue;
    }
    if (arg === '--idle-ms' || arg.startsWith('--idle-ms=')) {
      const parsed = parseNonNegativeNumber('--idle-ms', readValue('--idle-ms'));
      if (!parsed.ok) return parsed;
      idleMs = parsed.value;
      continue;
    }
    if (arg === '--aging-ms' || arg.startsWith('--aging-ms=')) {
      const parsed = parseNonNegativeNumber('--aging-ms', readValue('--aging-ms'));
      if (!parsed.ok) return parsed;
      agingMs = parsed.value;
      continue;
    }
    if (arg === '--walls-json' || arg.startsWith('--walls-json=')) {
      const parsed = parseWallsJson(readValue('--walls-json'));
      if (!parsed.ok) return parsed;
      wallsJson = parsed.walls;
      continue;
    }
    if (arg.startsWith('-')) {
      return { ok: false, error: `unknown flag ${JSON.stringify(arg)}` };
    }
    paths.push(arg);
  }

  if (want === null) return { ok: false, error: '--want is required' };
  if (wallsJson !== null && paths.length > 0) {
    return { ok: false, error: '--walls-json cannot be combined with JUnit file paths' };
  }
  return { ok: true, want, idleMs, agingMs, wallsJson, paths };
}

function isErrnoException(error: unknown): error is NodeJS.ErrnoException {
  return error instanceof Error && 'code' in error;
}

const parsed = parseArgs(process.argv.slice(2));
if (!parsed.ok) {
  process.stderr.write(`smoo-nx-suite-minimum: ${parsed.error}\n${USAGE}`);
  process.exit(2);
}

if (parsed.wallsJson !== null) {
  process.stdout.write(`${wholeSuiteMinimum(parsed.wallsJson, parsed.want, parsed.idleMs, parsed.agingMs)}\n`);
  process.exit(0);
}

for (const candidate of parsed.paths) {
  let xml: string;
  try {
    xml = await readFile(candidate, 'utf8');
  } catch (error: unknown) {
    if (isErrnoException(error) && error.code === 'ENOENT') continue;
    const detail = error instanceof Error ? error.message : String(error);
    process.stderr.write(`smoo-nx-suite-minimum: cannot read ${candidate}: ${detail}\n`);
    process.exit(1);
  }
  const walls = junitCaseTimes(xml)
    .filter(({ seconds }) => seconds >= 0)
    .map(({ seconds }) => seconds * 1000);
  if (walls.length > 0) {
    process.stdout.write(`${wholeSuiteMinimum(walls, parsed.want, parsed.idleMs, parsed.agingMs)}\n`);
    process.exit(0);
  }
}

process.stdout.write('1\n');
