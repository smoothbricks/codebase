import { spawnSync } from 'node:child_process';
import { readFileSync, writeFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { checkBendFile } from '@smoothbricks/bend-emit';

/** A Bend law book bound to a Rust module: the model it checks and the kernel it writes. */
export interface LawModule {
  /** What its errors call it. */
  readonly label: string;
  /** The book, relative to the repository root. */
  readonly modelName: string;
  /** The emitted Rust module. */
  readonly kernel: URL;
  readonly functions: readonly string[];
  readonly laws: readonly string[];
  readonly cap: number;
  readonly properties: number;
  readonly propertyLength: number;
  /** The command that regenerates it, which a stale `--check` names. */
  readonly regenerate: string;
}

/**
 * Check the book, emit its Rust module, format it, and write it, or with `--check` refuse when the committed kernel
 * differs from what the book emits.
 */
export async function emitLaw(law: LawModule): Promise<void> {
  const model = new URL(`../../../${law.modelName}`, import.meta.url);
  const book = await checkBendFile(fileURLToPath(model));
  const emission = book.emit({
    target: {
      kind: 'rust-module',
      modelName: law.modelName,
      flags: [
        '--fns',
        law.functions.join(','),
        '--laws',
        law.laws.join(','),
        '--cap',
        String(law.cap),
        '--props',
        String(law.properties),
        '--prop-len',
        String(law.propertyLength),
        '--rust-module',
        '--model-name',
        law.modelName,
      ],
    },
    functions: [...law.functions],
    laws: [...law.laws],
    cap: law.cap,
    properties: law.properties,
    propertyLength: law.propertyLength,
  });
  if (emission.kind !== 'module') throw new Error(`${law.label}: expected a Rust module`);
  const formatted = spawnSync('rustfmt', ['--edition', '2024'], {
    input: emission.source,
    encoding: 'utf8',
    timeout: 30_000,
  });
  if (formatted.error || formatted.status !== 0) {
    throw new Error(`${law.label}: rustfmt failed (exit ${formatted.status}): ${formatted.stderr}`, {
      cause: formatted.error,
    });
  }
  if (process.argv.includes('--check')) {
    if (readFileSync(law.kernel, 'utf8') !== formatted.stdout) {
      throw new Error(`${law.label}: kernel differs; run ${law.regenerate}`);
    }
  } else {
    writeFileSync(law.kernel, formatted.stdout);
  }
}
