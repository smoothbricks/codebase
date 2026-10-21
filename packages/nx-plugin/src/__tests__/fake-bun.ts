import { mkdir, writeFile } from 'node:fs/promises';
import { join } from 'node:path';

/** The marks a fake `bun` leaves beside itself, so a test sees whether it was asked its version or run as the command. */
export const FAKE_BUN_INVOKED = 'invoked';
export const FAKE_BUN_RAN = 'ran.marker';

/**
 * An executable `bun` in `directory` that prints `version` for `--version` and otherwise runs as the command: it
 * leaves `ran.marker` beside itself. Every invocation, the version probe included, leaves `invoked`. It uses only shell
 * builtins, so a PATH that names nothing but its directory still runs it.
 */
export async function fakeBun(directory: string, version: string): Promise<string> {
  await mkdir(directory, { recursive: true });
  const path = join(directory, 'bun');
  await writeFile(
    path,
    [
      '#!/bin/sh',
      `: >> "\${0%/*}/${FAKE_BUN_INVOKED}"`,
      `if [ "$1" = "--version" ]; then echo ${version}; exit 0; fi`,
      `: > "\${0%/*}/${FAKE_BUN_RAN}"`,
      '',
    ].join('\n'),
    { mode: 0o755 },
  );
  return path;
}

/** A `bun` that fails when asked its version, printing `said` on stderr. */
export async function failingBun(directory: string, said: string): Promise<string> {
  await mkdir(directory, { recursive: true });
  const path = join(directory, 'bun');
  await writeFile(path, ['#!/bin/sh', `echo ${said} >&2`, 'exit 3', ''].join('\n'), { mode: 0o755 });
  return path;
}
