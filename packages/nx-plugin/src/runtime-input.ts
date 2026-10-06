/**
 * The one script every Nx runtime input runs through, named as the workspace
 * root's `node_modules` links this package: Nx runs runtime inputs from the
 * workspace root. `runtime-input.sh` explains why it exists: Nx hashes a failed
 * input's error text as its value.
 */
export const RUNTIME_INPUT_SCRIPT = 'node_modules/@smoothbricks/nx-plugin/runtime-input.sh';

/** A runtime input whose failure stops Nx. `command` is shell words, already quoted. */
export function runtimeInput(command: string): { runtime: string } {
  return { runtime: `sh ${RUNTIME_INPUT_SCRIPT} ${command}` };
}

/** One shell word: bare when nothing in it is special to `sh`, single-quoted otherwise. */
export function shellWord(word: string): string {
  return /^[\w@%+=:,./-]+$/.test(word) ? word : `'${word.replaceAll("'", `'"'"'`)}'`;
}
