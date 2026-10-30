import { describe, expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import { existsSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { nxTools, toolExitStatus } from './nx-tools.js';

const smoo = fileURLToPath(new URL('../../bin/smoo', import.meta.url));

describe('smoo nx-<tool>', () => {
  it('finds every smoo-nx bin of the tool packages the CLI depends on', () => {
    const tools = nxTools();
    for (const [tool, pkg] of [
      ['nx-failures', 'failure-summary'],
      ['nx-nextest-binaries', 'nextest-extract'],
      ['nx-build-inputs', 'build-inputs'],
      ['nx-cargo-hash', 'nx-plugin'],
    ]) {
      expect(tools.get(tool)).toEndWith(`/${pkg}/${pkg === 'nx-plugin' ? 'dist' : 'dist/ts'}/bin/smoo-${tool}.js`);
    }
    for (const script of tools.values()) expect(existsSync(script)).toBe(true);
  });

  it("runs the tool with its arguments and exits with the tool's status", () => {
    const root = mkdtempSync(join(tmpdir(), 'smoo-nx-tools-'));
    try {
      const log = join(root, 'gate.log');
      writeFileSync(log, '> nx run a:test\n\nerror: boom\n\nFailed tasks:\n\n- a:test\n');
      const summary = spawnSync(smoo, ['nx-failures', log], { encoding: 'utf8' });
      expect({ status: summary.status, stdout: summary.stdout }).toEqual({
        status: 0,
        stdout: 'failed a:test\nerror: boom\n',
      });
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
    const usage = spawnSync(smoo, ['nx-failures'], { encoding: 'utf8' });
    expect({ status: usage.status, stderr: usage.stderr }).toEqual({
      status: 64,
      stderr: 'usage: smoo-nx-failures OUTPUT\n',
    });
    const unknown = spawnSync(smoo, ['nx-absent'], { encoding: 'utf8' });
    expect(unknown.status).toBe(64);
    expect(unknown.stderr).toStartWith('smoo: no tool nx-absent; the tools are ');
    expect(unknown.stderr).toContain('nx-failures');
  });

  it('reports a signal as 128 plus its number, and a status-less exit as 1', () => {
    expect(toolExitStatus(0, null)).toBe(0);
    expect(toolExitStatus(3, null)).toBe(3);
    expect(toolExitStatus(null, 'SIGTERM')).toBe(143);
    expect(toolExitStatus(null, null)).toBe(1);
  });
});
