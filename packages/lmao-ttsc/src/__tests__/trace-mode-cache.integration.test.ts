import { expect, test } from 'bun:test';
import { fileURLToPath } from 'node:url';

// The plugin output depends on the compile's NODE_ENV (spec 01o §6.3), so no transform cache may deliver a module of
// a test compile to a production compile of the same project state, or the reverse.
const probe = fileURLToPath(new URL('./trace-mode-probe.ts', import.meta.url));

async function compileInChild(deliveries: readonly string[]): Promise<boolean[]> {
  const child = Bun.spawn([process.execPath, probe, ...deliveries], { stdout: 'pipe', stderr: 'pipe' });
  const [stdout, stderr, exitCode] = await Promise.all([
    new Response(child.stdout).text(),
    new Response(child.stderr).text(),
    child.exited,
  ]);
  if (exitCode !== 0) throw new Error(`trace-mode probe exited ${exitCode}:\n${stderr || stdout}`);
  return JSON.parse(stdout);
}

test('one transform cache delivers no module of a test compile to a production compile of the same state', async () => {
  expect(await compileInChild(['entry=test', 'second=production'])).toEqual([true, false]);
  expect(await compileInChild(['entry=production', 'second=test'])).toEqual([false, true]);
});
