import { describe, expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import { existsSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { nxTools } from './nx-tools.js';

const smoo = fileURLToPath(new URL('../../bin/smoo', import.meta.url));

describe('smoo nx-<tool>', () => {
  it('finds every smoo-nx bin of the tool packages the CLI depends on', () => {
    const tools = nxTools();
    for (const [tool, pkg] of [
      ['nx-failures', 'failure-summary'],
      ['nx-nextest-extract', 'nextest-extract'],
      ['nx-build-inputs', 'build-inputs'],
      ['nx-cargo-hash', 'nx-plugin'],
    ]) {
      expect(tools.get(tool)).toEndWith(`/${pkg}/${pkg === 'nx-plugin' ? 'dist' : 'dist/ts'}/bin/smoo-${tool}.js`);
    }
    for (const script of tools.values()) expect(existsSync(script)).toBe(true);
  });

  it("runs the tool with its arguments and exits with the tool's status", () => {
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
});
