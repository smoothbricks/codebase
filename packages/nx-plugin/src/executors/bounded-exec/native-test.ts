import { fileURLToPath } from 'node:url';
import { shellWord } from '../../runtime-input.js';
import { BOUNDED_TEST_IDLE_TIMEOUT_MS } from '../../test-backstop.js';
import type { NativeBoundedExecOptions, ShellBoundedExecOptions } from './schema.js';

/** Set by bounded-exec, never by a target: metadata belongs to one invocation, even for concurrent runs of a target. */
export const FIXTURE_DIRECTORY_ENV = 'BOUNDED_EXEC_FIXTURE_DIR';
/**
 * Set by bounded-exec for a fixture target that opts into same-volume clones (`templateClones`): the task's test
 * template directory, made under {@link TEMPLATE_PARENT} and removed when the task ends, after its command subtree.
 */
export const TEMPLATE_DIRECTORY_ENV = 'BOUNDED_EXEC_TEMPLATE_DIR';
/**
 * Where fixtures are cloned (cowshed-os `test_template.rs`, `TEMPORARY`): `/tmp`, which is `/private/tmp` on Darwin. A
 * template clones only within its volume, so its directory is made there and not in the task's TMPDIR, which a RAM
 * lease puts on another volume.
 */
export const TEMPLATE_PARENT = process.platform === 'darwin' ? '/private/tmp' : '/tmp';
/**
 * The short-directory form a task's template directory is named in, `cowshed-dtest-<executor pid>-templates-…`
 * (cowshed-os `test_template.rs`, `ROOT_PREFIX`): the executor removes it when the task ends, and one a killed executor
 * left is taken by the lane setup's sweep, which reads the pid such a name records and takes it once no process holds
 * that pid.
 */
export const TEMPLATE_PREFIX = 'cowshed-dtest-';

const FIXTURE_TIMEOUT_MS = 3_600_000;
const BINARIES = `"$${FIXTURE_DIRECTORY_ENV}/binaries-metadata.json"`;
const CARGO = `"$${FIXTURE_DIRECTORY_ENV}/cargo-metadata.json"`;
const TOOL_CONFIG = shellWord(`smoo:${fileURLToPath(new URL('../../../nextest.toml', import.meta.url))}`);

const LAYOUT = shellWord(
  fileURLToPath(
    new URL(
      import.meta.url.endsWith('.ts') ? './native-fixture-layout.ts' : './native-fixture-layout.js',
      import.meta.url,
    ),
  ),
);

/** Cargo's compiled units remain shared. Only promoted executables get a private final name for this run. */
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
  const list = `cargo --frozen nextest list${args(cargoArgs)} --list-type binaries-only --message-format json --cargo-metadata ${CARGO} ${config}`;
  const metadata = 'cargo --frozen metadata --format-version 1 --all-features';
  const layout = `"$${FIXTURE_DIRECTORY_ENV}/layout.toml"`;
  const prepare =
    `${metadata} > ${CARGO} && mode=$(${shellWord(process.execPath)} ${LAYOUT} "$${FIXTURE_DIRECTORY_ENV}"${args(cargoArgs)}) && ` +
    `{ [ "$mode" != cold ] || ${list} > /dev/null; } && ` +
    `if [ "$mode" = shared ]; then ${list} > ${BINARIES}; else ` +
    `${metadata} --config ${layout} > ${CARGO} && ${list} --config ${layout} > ${BINARIES}; fi`;
  return {
    ...options,
    command: command ?? run,
    fixtureCommand: prepare,
    fixtureTimeoutMs: FIXTURE_TIMEOUT_MS,
    ...(timeoutMs === undefined
      ? { idleTimeoutMs: idleTimeoutMs ?? BOUNDED_TEST_IDLE_TIMEOUT_MS }
      : { timeoutMs, idleTimeoutMs }),
    ...(command === undefined && options.parallelism !== 1
      ? { unitsCommand: `cargo --frozen nextest list ${reuse} ${config}${select} -T oneline` }
      : {}),
  };
}
