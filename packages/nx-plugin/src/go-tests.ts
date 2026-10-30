import { readFileSync } from 'node:fs';
import { dirname, isAbsolute, resolve } from 'node:path';
import { isRecord } from './type-guards.js';

/**
 * The manifest `smoo-nx-go-test-build` writes beside the test binaries it builds with `go test -c`, and
 * `smoo-nx-go-test-run` runs. Every path in it is relative to the manifest's own directory, so the binaries and the
 * packages they test are found wherever both are restored together, as an Nx cache restore puts them back.
 */
export const GO_TESTS_MANIFEST = 'go-tests.json';

/** One package's test binary. */
export interface GoTestBinary {
  /** The binary `go test -c` built for the package. */
  readonly binary: string;
  /** The package directory: `go test` runs a package's binary there, so its relative test data resolves alike. */
  readonly dir: string;
  /** The package's import path, every event's `Package`. */
  readonly importPath: string;
}

export interface GoTestsManifest {
  readonly version: 1;
  /** One entry per package with test files, sorted by import path. */
  readonly tests: readonly GoTestBinary[];
}

export type GoTestsRead =
  | { readonly ok: true; readonly tests: readonly GoTestBinary[] }
  | { readonly ok: false; readonly reason: string };

/** The binaries the manifest at `path` lists, their paths resolved against its directory, or why it lists none. */
export function readGoTestsManifest(path: string): GoTestsRead {
  let parsed: unknown;
  try {
    parsed = JSON.parse(readFileSync(path, 'utf8'));
  } catch (error) {
    return { ok: false, reason: `${path} cannot be read: ${error instanceof Error ? error.message : String(error)}` };
  }
  if (!isRecord(parsed) || parsed.version !== 1 || !Array.isArray(parsed.tests)) {
    return { ok: false, reason: `${path} is no version 1 ${GO_TESTS_MANIFEST}: it needs {"version":1,"tests":[…]}` };
  }
  const base = dirname(path);
  const tests: GoTestBinary[] = [];
  const listed = new Set<string>();
  for (const [index, entry] of parsed.tests.entries()) {
    if (
      !isRecord(entry) ||
      !isRelativePath(entry.binary) ||
      !isRelativePath(entry.dir) ||
      typeof entry.importPath !== 'string' ||
      entry.importPath === ''
    ) {
      return { ok: false, reason: `${path}: tests[${index}] needs a relative binary and dir and an importPath` };
    }
    if (listed.has(entry.importPath)) {
      return { ok: false, reason: `${path}: ${entry.importPath} is listed twice` };
    }
    listed.add(entry.importPath);
    tests.push({ binary: resolve(base, entry.binary), dir: resolve(base, entry.dir), importPath: entry.importPath });
  }
  return { ok: true, tests };
}

function isRelativePath(value: unknown): value is string {
  return typeof value === 'string' && value !== '' && !isAbsolute(value);
}
