import { fileURLToPath } from 'node:url';
import { shellWord } from '../../runtime-input.js';
import { BOUNDED_TEST_IDLE_TIMEOUT_MS } from '../../test-backstop.js';
import type { NativeBoundedExecOptions, ShellBoundedExecOptions } from './schema.js';

/** Set by bounded-exec, never by a target: metadata belongs to one invocation, even for concurrent runs of a target. */
export const FIXTURE_DIRECTORY_ENV = 'BOUNDED_EXEC_FIXTURE_DIR';

const FIXTURE_TIMEOUT_MS = 3_600_000;
const BINARIES = `"$${FIXTURE_DIRECTORY_ENV}/binaries-metadata.json"`;
const CARGO = `"$${FIXTURE_DIRECTORY_ENV}/cargo-metadata.json"`;
const TOOL_CONFIG = shellWord(`smoo:${fileURLToPath(new URL('../../../nextest.toml', import.meta.url))}`);

/** Native test binaries stay in Cargo's own target directory; only the recipe's two metadata files are private. */
export function nativeTestPhases({
  nativeTest,
  command,
  timeoutMs,
  idleTimeoutMs,
  ...options
}: NativeBoundedExecOptions): ShellBoundedExecOptions {
  const { cargoArgs, selectionArgs, runArgs } = nativeTest;
  const args = (words: readonly string[]): string => (words.length === 0 ? '' : ` ${words.map(shellWord).join(' ')}`);
  const config = `--user-config-file none --tool-config-file ${TOOL_CONFIG}`;
  const reuse = `--binaries-metadata ${BINARIES} --cargo-metadata ${CARGO}`;
  const select = args(selectionArgs ?? []);
  const verdict =
    runArgs?.some((arg) => arg === '--no-tests' || arg.startsWith('--no-tests=')) === true ? '' : ' --no-tests=fail';
  const run = `cargo --frozen nextest run ${reuse} ${config}${select}${verdict}${args(runArgs ?? [])}`;
  return {
    ...options,
    command: command ?? run,
    fixtureCommand:
      `cargo --frozen metadata --format-version 1 --all-features > ${CARGO} && ` +
      `cargo --frozen nextest list${args(cargoArgs)} --list-type binaries-only --message-format json --cargo-metadata ${CARGO} ${config} > ${BINARIES}`,
    fixtureTimeoutMs: FIXTURE_TIMEOUT_MS,
    ...(timeoutMs === undefined
      ? { idleTimeoutMs: idleTimeoutMs ?? BOUNDED_TEST_IDLE_TIMEOUT_MS }
      : { timeoutMs, idleTimeoutMs }),
    ...(command === undefined && options.parallelism !== 1
      ? { unitsCommand: `cargo --frozen nextest list ${reuse} ${config}${select} -T oneline` }
      : {}),
  };
}
