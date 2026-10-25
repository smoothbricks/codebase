import { expect, it } from 'bun:test';
import { tmpdir } from 'node:os';

// This file reaches the runner through the preload's ttsc compile, which reads
// the plugin build environment on a worker thread. A worker started with
// SHARE_ENV makes Bun replace the process.env object; node:os keeps reading the
// object it captured when it loaded, so tmpdir() would stop following TMPDIR.
it('keeps process.env live for node:os after compiling through the preload', () => {
  const outer = process.env.TMPDIR;
  const changed = `${outer ?? '/tmp'}/preload-environment-probe`;
  process.env.TMPDIR = changed;
  try {
    expect(tmpdir()).toBe(changed);
  } finally {
    if (outer === undefined) delete process.env.TMPDIR;
    else process.env.TMPDIR = outer;
  }
});
