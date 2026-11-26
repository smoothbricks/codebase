import { afterEach, beforeEach, describe, expect, it } from 'bun:test';
import { chmodSync, existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { cargoFetchedInput } from './cargo-fetched-input.js';
import { RUNTIME_INPUT_SCRIPT } from './runtime-input.js';

// README "Cargo cache boundaries": the registry input is `fetched` from the managed shell's current stamp without
// starting Cargo, and from an offline fetch otherwise. The repository here has a stand-in for each program the input
// starts: a `cargo` that records its arguments and succeeds unless told to refuse, and a setup-environment that
// records what it was asked and answers as the test says.
describe('the cargo-fetch registry input', () => {
  let root = '';
  beforeEach(() => {
    root = mkdtempSync(join(tmpdir(), 'cargo-fetched-input-'));
    mkdirSync(join(root, 'bin'));
    mkdirSync(join(root, 'tooling', 'direnv'), { recursive: true });
    mkdirSync(join(root, 'packages', 'native'), { recursive: true });
    writeFileSync(
      join(root, 'bin', 'cargo'),
      `#!/bin/sh\nprintf '%s|%s\\n' "$PWD" "$*" >> ${JSON.stringify(join(root, 'cargo-runs'))}\n[ ! -e ${JSON.stringify(join(root, 'registry-incomplete'))} ]\n`,
    );
    chmodSync(join(root, 'bin', 'cargo'), 0o755);
    writeFileSync(
      join(root, 'tooling', 'direnv', 'setup-environment.ts'),
      [
        "import { appendFileSync, existsSync } from 'node:fs';",
        `appendFileSync(${JSON.stringify(join(root, 'stamp-asks'))}, process.argv.slice(2).join(' ') + '\\n');`,
        `process.exit(existsSync(${JSON.stringify(join(root, 'stamp-current'))}) ? 0 : 1);`,
        '',
      ].join('\n'),
    );
  });
  afterEach(() => {
    rmSync(root, { recursive: true, force: true });
  });

  const lines = (file: string): string[] =>
    existsSync(join(root, file)) ? readFileSync(join(root, file), 'utf8').trimEnd().split('\n') : [];
  /** The input's command as Nx runs it, in the workspace root, without the wrapper that only reports a failure. */
  const value = (projectRoot: string, manifests: readonly string[]): string => {
    const { runtime } = cargoFetchedInput(projectRoot, manifests);
    const wrapper = `sh ${RUNTIME_INPUT_SCRIPT} `;
    expect(runtime.startsWith(wrapper)).toBe(true);
    const run = Bun.spawnSync(['sh', '-c', runtime.slice(wrapper.length)], {
      cwd: root,
      env: { PATH: `${join(root, 'bin')}:${dirname(process.execPath)}:${process.env.PATH ?? ''}` },
      stdout: 'pipe',
      stderr: 'pipe',
    });
    expect(run.exitCode).toBe(0);
    expect(run.stderr.toString()).toBe('');
    return run.stdout.toString().trim();
  };

  it('is fetched from a current stamp without starting cargo', () => {
    writeFileSync(join(root, 'stamp-current'), '');
    expect(value('packages/native', ['Cargo.toml'])).toBe('fetched');
    expect(lines('stamp-asks')).toEqual(['--cargo --check --workspace packages/native']);
    expect(lines('cargo-runs')).toEqual([]);
  });

  it('runs the offline fetch when the stamp is not current, and is fetched when the registry holds the graph', () => {
    expect(value('packages/native', ['Cargo.toml'])).toBe('fetched');
    expect(lines('cargo-runs')).toEqual([`${join(root, 'packages', 'native')}|fetch --locked --offline --quiet`]);
  });

  it('is a value of its own run when neither the stamp nor the registry holds the graph', () => {
    writeFileSync(join(root, 'registry-incomplete'), '');
    const first = value('.', ['Cargo.toml']);
    expect(first).toMatch(/^unfetched \d+ \d+$/);
    expect(lines('cargo-runs')).toEqual([`${root}|fetch --locked --offline --quiet`]);
  });

  it('still fetches offline a manifest the stamp does not cover, beside a current stamp', () => {
    writeFileSync(join(root, 'stamp-current'), '');
    expect(value('packages/native', ['Cargo.toml', 'wasm/Cargo.toml'])).toBe('fetched');
    expect(lines('cargo-runs')).toEqual([
      `${join(root, 'packages', 'native')}|fetch --locked --manifest-path wasm/Cargo.toml --offline --quiet`,
    ]);
    writeFileSync(join(root, 'registry-incomplete'), '');
    expect(value('packages/native', ['Cargo.toml', 'wasm/Cargo.toml'])).toMatch(/^unfetched /);
  });

  it('asks no stamp for a project whose only manifest the shell does not stamp', () => {
    writeFileSync(join(root, 'stamp-current'), '');
    expect(value('packages/native', ['wasm/Cargo.toml'])).toBe('fetched');
    expect(lines('stamp-asks')).toEqual([]);
    expect(lines('cargo-runs')).toHaveLength(1);
  });
});
