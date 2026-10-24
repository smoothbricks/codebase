import { existsSync, realpathSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { CARGO_ANCESTOR_INPUTS } from '../cargo-source-hash.js';

/** What a Git discovery or a Cargo manifest/config walk from a directory finds there or in an ancestor. */
const TREE_MARKERS = ['.git', ...CARGO_ANCESTOR_INPUTS] as const;

/** The first Git or Cargo marker at `path` or above it, which a walk from `path` would adopt; `null` when none. */
export function enclosingTreeMarker(path: string): string | null {
  for (let directory = path; ; directory = dirname(directory)) {
    for (const marker of TREE_MARKERS) {
      if (existsSync(join(directory, marker))) return join(directory, marker);
    }
    if (dirname(directory) === directory) return null;
  }
}

let proven: string | undefined;

/**
 * A physical temp parent no Git or Cargo walk leaves: the inherited TMPDIR when it is one, else the system temp
 * directory. A fixture that asserts it is outside every work tree and workspace makes its root here, so the suite
 * does not depend on where its runner pointed TMPDIR. Neither one qualifying is said, with what encloses each.
 */
export function outsideTreesTmpdir(): string {
  if (proven !== undefined) return proven;
  const refused: string[] = [];
  for (const candidate of [tmpdir(), process.platform === 'darwin' ? '/private/tmp' : '/tmp']) {
    let physical: string;
    try {
      physical = realpathSync(candidate);
    } catch (error) {
      refused.push(`${candidate}: ${error instanceof Error ? error.message : String(error)}`);
      continue;
    }
    const marker = enclosingTreeMarker(physical);
    if (marker === null) {
      proven = physical;
      return physical;
    }
    refused.push(`${candidate} (${physical}) lies below ${marker}`);
  }
  throw new Error(`no temp parent outside every Git work tree and Cargo workspace: ${refused.join('; ')}`);
}
