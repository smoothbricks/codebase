import { expect, it } from 'bun:test';
import { execFileSync, spawn } from 'node:child_process';
import { chmod, mkdir, mkdtemp, readFile, realpath, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { churnScript } from './__tests__/churn-group.js';

const cliModule = join(import.meta.dir, 'bin', 'smoo-nx-cargo-hash.ts');

/** A workspace of one crate, a `cargo` that is the churn group, and the two FIFOs it reports through. */
async function churningCargoWorkspace(root: string) {
  const workspace = join(root, 'workspace');
  await mkdir(join(workspace, 'src'), { recursive: true });
  await writeFile(join(workspace, 'Cargo.toml'), '[package]\nname = "app"\nversion = "0.1.0"\nedition = "2021"\n');
  await writeFile(join(workspace, 'src/lib.rs'), 'pub fn answer() -> u8 { 1 }\n');
  const bin = join(root, 'bin');
  await mkdir(bin);
  const script = join(root, 'churn.sh');
  await writeFile(script, churnScript());
  await writeFile(join(bin, 'cargo'), `#!/bin/sh\nexec sh ${script} "$CHURN_LIFETIME" "$CHURN_UP"\n`);
  await chmod(join(bin, 'cargo'), 0o755);
  const lifetime = join(root, 'lifetime.fifo');
  const up = join(root, 'up.fifo');
  execFileSync('mkfifo', [lifetime, up]);
  return {
    workspace,
    lifetime,
    up,
    environment: {
      ...process.env,
      PATH: `${bin}:${process.env.PATH ?? ''}`,
      CARGO_HOME: join(root, 'cargo-home'),
      CHURN_LIFETIME: lifetime,
      CHURN_UP: up,
    },
  };
}

// A standalone `smoo-nx-cargo-hash` forwards a signal to Cargo's process group and then raises it again. macOS does not
// hold a fork back for a group signal in flight, so a member forked as that SIGKILL goes out is not reached by it, and
// one that holds Cargo's output pipe would also keep the bounded call from ever settling. The Cargo here is a group
// that forks as fast as it can; the end of file of the FIFO every member holds is the death of the last one, and a
// kill that is not repeated leaves an attempt that only the backstop ends. The kill is made where nothing can be
// awaited, so Cargo's leader is still unreaped, and listed, when it gives up: the hash says so on stderr, as listed
// and not as running.
it('a standalone smoo-nx-cargo-hash ended by a signal leaves no member of a forking cargo behind', async () => {
  for (const attempt of [1, 2, 3]) {
    const root = await realpath(await mkdtemp(join(tmpdir(), 'cargo-child-kill-')));
    try {
      const { workspace, lifetime, up, environment } = await churningCargoWorkspace(root);
      // Each end of a FIFO opens only once the other does: read both before Cargo can write.
      const gone = readFile(lifetime, 'utf8');
      const said = readFile(up, 'utf8');
      const stderr: Buffer[] = [];
      const hash = spawn(process.execPath, [cliModule, 'Cargo.toml'], {
        cwd: workspace,
        env: environment,
        stdio: ['ignore', 'ignore', 'pipe'],
      });
      hash.stderr.on('data', (chunk: Buffer) => stderr.push(chunk));
      // `close`, not `exit`: stderr is read to its end, so what the hash said as it was ended is all there.
      const ended = new Promise<{ code: number | null; signal: NodeJS.Signals | null; error?: string }>((resolve) => {
        hash.once('close', (code, signal) => resolve({ code, signal }));
        hash.once('error', (error) => resolve({ code: null, signal: null, error: error.message }));
      });
      try {
        // A hash that cannot start, or ends before Cargo is up, would leave the read of `up` waiting for ever.
        const first = await Promise.race([said, ended]);
        if (typeof first !== 'string') {
          const ending = JSON.stringify({ ...first, stderr: Buffer.concat(stderr).toString() });
          throw new Error(`smoo-nx-cargo-hash ended before its cargo was up: ${ending}`);
        }
        const group = Number.parseInt(first, 10);
        hash.kill('SIGTERM');
        expect(await ended, `attempt ${attempt} stderr: ${Buffer.concat(stderr).toString()}`).toEqual({
          code: null,
          signal: 'SIGTERM',
        });
        expect(await gone).toBe('');
        expect(Buffer.concat(stderr).toString(), `attempt ${attempt}`).toContain(
          `process group ${group} was still listed`,
        );
      } finally {
        // An attempt that failed before the signal must not leave its group running.
        hash.kill('SIGTERM');
        await ended;
      }
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  }
});
