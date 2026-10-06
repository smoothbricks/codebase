import { expect } from 'bun:test';

/**
 * What Nx hashes for a `runtime` input: the command's stdout, from `sh -c` in
 * the workspace root. `process.env` is passed because `Bun.spawn`'s default
 * environment is the one this process started with, and a fixture's
 * `CARGO_HOME` lives in `process.env` (see `useFixtureCargoHome`).
 */
export async function runtimeInputValue(command: string, workspaceRoot: string): Promise<string> {
  const child = Bun.spawn(['sh', '-c', command], {
    cwd: workspaceRoot,
    env: process.env,
    stdout: 'pipe',
    stderr: 'pipe',
  });
  const [exitCode, stdout, stderr] = await Promise.all([
    child.exited,
    new Response(child.stdout).text(),
    new Response(child.stderr).text(),
  ]);
  expect({ command, exitCode, stderr }).toMatchObject({ command, exitCode: 0 });
  return stdout;
}
