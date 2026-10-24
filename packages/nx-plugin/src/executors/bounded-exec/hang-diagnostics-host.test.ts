import { describe, expect, it } from 'bun:test';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { processTable } from '../../testing.js';
import { stopProbe } from './hang-diagnostics.js';

/**
 * The privileged hang probe against the real sudo. A host proof: every cow sandbox denies `/usr/bin/sudo` itself, and
 * the probe needs this host's passwordless sudo grant for `lsof`; without it the test fails naming sudo's refusal.
 */
describe('privileged hang probe host proofs', () => {
  it('ends a privileged probe through sudo, so its root lsof does not outlive sudo', async () => {
    // The privileged probe exists only on Darwin, where it runs `sudo -n lsof`.
    if (process.platform !== 'darwin') return;
    // `-r 30` lists once, then sleeps holding its pipe: alive and silent when stopped, as a slow scan is.
    const command = `lsof -r 30 -nP -p ${process.pid}`;
    const probe = spawn('sudo', ['-n', ...command.split(' ')], { detached: true, stdio: ['ignore', 'pipe', 'pipe'] });
    try {
      const exited = once(probe, 'exit');
      let stderr = '';
      probe.stderr.setEncoding('utf8').on('data', (chunk: string) => {
        stderr += chunk;
      });
      // The first listing is the readiness event: sudo has finished starting and lsof runs as root.
      const listed = await Promise.race([once(probe.stdout, 'data').then(() => true), exited.then(() => false)]);
      expect(listed, `sudo -n ${command} ended before listing: ${stderr}`).toBe(true);
      if (probe.pid === undefined) throw new Error('sudo has no pid');
      stopProbe('sudo', probe.pid);
      await exited;
      const left = (await processTable()).filter((entry) => entry.command.trim() === command);
      expect(left, 'root lsof rows left after sudo exited').toEqual([]);
    } finally {
      probe.stdout.destroy();
      probe.stderr.destroy();
    }
  });
});
