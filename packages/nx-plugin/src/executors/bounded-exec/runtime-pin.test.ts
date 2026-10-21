import { afterEach, describe, expect, it } from 'bun:test';
import { access, mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { delimiter, join } from 'node:path';
import { FAKE_BUN_INVOKED, failingBun, fakeBun } from '../../__tests__/fake-bun.js';
import {
  checkRuntimePin,
  declaredBun,
  describeRuntimePinRefusal,
  type RuntimePinRefusal,
  resolveOnPath,
  runsBun,
} from './runtime-pin.js';

const directories: string[] = [];

afterEach(async () => {
  await Promise.all(directories.splice(0).map((directory) => rm(directory, { recursive: true, force: true })));
});

/** A workspace root, with a root `package.json` holding `manifest` as text when there is one. */
async function workspace(manifest?: string): Promise<string> {
  const root = await mkdtemp(join(tmpdir(), 'smoothbricks-runtime-pin-'));
  directories.push(root);
  if (manifest !== undefined) {
    await writeFile(join(root, 'package.json'), manifest);
  }
  return root;
}

const declaring = (declaration: string): string => JSON.stringify({ name: 'workspace', packageManager: declaration });

async function exists(path: string): Promise<boolean> {
  try {
    await access(path);
    return true;
  } catch {
    return false;
  }
}

describe('runsBun', () => {
  for (const command of [
    'bun test',
    'bun',
    'cd packages/x && bun test --timeout=0',
    'FOO=1 bun run build',
    '(cd x; bun a)',
    "sh -c 'bun test'",
    'a | bun b',
  ]) {
    it(`guards ${command}`, () => {
      expect(runsBun(command)).toBe(true);
    });
  }

  for (const command of [
    '../runtime/bun test',
    '/usr/local/bin/bun test',
    'cargo nextest run',
    'bunx tsc',
    'node bun.js',
    'bun-runtime test',
    'echo bunny',
  ]) {
    it(`leaves ${command} to its own binary or to no bun at all`, () => {
      expect(runsBun(command)).toBe(false);
    });
  }
});

describe('resolveOnPath', () => {
  it('takes the first executable regular file, as the shell does', async () => {
    const root = await workspace();
    await writeFile(join(root, 'a-bun'), '', { mode: 0o644 });
    await mkdir(join(root, 'plain'));
    await writeFile(join(root, 'plain', 'bun'), '', { mode: 0o644 });
    await mkdir(join(root, 'dir', 'bun'), { recursive: true });
    await fakeBun(join(root, 'first'), '1.0.0');
    await fakeBun(join(root, 'second'), '2.0.0');

    const path = ['missing', 'plain', 'dir', 'first', 'second'].map((name) => join(root, name)).join(delimiter);

    expect(resolveOnPath('bun', path, root)).toBe(join(root, 'first', 'bun'));
  });

  it("reads an empty entry and a relative one from the command's directory", async () => {
    const root = await workspace();
    await fakeBun(join(root, 'bin'), '1.0.0');
    await writeFile(join(root, 'bun'), '', { mode: 0o755 });

    expect(resolveOnPath('bun', 'bin', root)).toBe(join(root, 'bin', 'bun'));
    expect(resolveOnPath('bun', '', root)).toBe(join(root, 'bun'));
  });

  it('finds nothing without a PATH or on one that holds none', async () => {
    const root = await workspace();

    expect(resolveOnPath('bun', undefined, root)).toBeNull();
    expect(resolveOnPath('bun', join(root, 'nowhere'), root)).toBeNull();
  });
});

describe('declaredBun', () => {
  it('declares nothing without a manifest, a packageManager, or a Bun one', async () => {
    expect(declaredBun(await workspace())).toEqual({ ok: true, value: null });
    expect(declaredBun(await workspace('{"name":"workspace"}'))).toEqual({ ok: true, value: null });
    expect(declaredBun(await workspace(declaring('pnpm@9.1.0')))).toEqual({ ok: true, value: null });
  });

  it('reads the version of bun@<version>, with or without the content hash', async () => {
    const root = await workspace(declaring('bun@1.4.3'));
    expect(declaredBun(root)).toEqual({
      ok: true,
      value: { manifest: join(root, 'package.json'), declaration: 'bun@1.4.3', version: '1.4.3' },
    });

    const hashed = await workspace(declaring('bun@1.4.3+sha512.abcdef'));
    expect(declaredBun(hashed)).toEqual({
      ok: true,
      value: { manifest: join(hashed, 'package.json'), declaration: 'bun@1.4.3+sha512.abcdef', version: '1.4.3' },
    });

    const prerelease = await workspace(declaring('bun@1.5.0-canary.1'));
    expect(declaredBun(prerelease)).toMatchObject({ ok: true, value: { version: '1.5.0-canary.1' } });
  });

  it('refuses a Bun declaration that names no version, rather than reading it as none', async () => {
    for (const declaration of ['bun@latest', 'bun@1.4', 'bun@', 'bun@^1.4.3']) {
      const root = await workspace(declaring(declaration));
      expect(declaredBun(root)).toEqual({
        ok: false,
        error: { code: 'malformed-declaration', manifest: join(root, 'package.json'), declaration },
      });
    }
  });

  it('refuses a manifest it cannot read', async () => {
    const root = await workspace('{ not json');

    const declared = declaredBun(root);

    expect(declared.ok).toBe(false);
    expect(declared.ok ? null : declared.error).toMatchObject({
      code: 'unreadable-manifest',
      manifest: join(root, 'package.json'),
    });
  });
});

describe('checkRuntimePin', () => {
  it('refuses a leg whose first bun on PATH prints another version, naming the binary', async () => {
    const root = await workspace(declaring('bun@1.4.3'));
    const bun = await fakeBun(join(root, 'bin'), '1.4.2');

    const refusal = await checkRuntimePin({
      root,
      command: 'bun test --timeout=0 fleet-it',
      env: { PATH: join(root, 'bin') },
      cwd: root,
      pathSource: "the target's env",
    });

    expect(refusal).toEqual({
      code: 'mismatch',
      declared: { manifest: join(root, 'package.json'), declaration: 'bun@1.4.3', version: '1.4.3' },
      bun,
      version: '1.4.2',
      pathSource: "the target's env",
    });
  });

  it('names both versions, the binary and the config each came from, and the symptom the refusal replaces', async () => {
    const root = await workspace(declaring('bun@1.4.3'));
    const bun = await fakeBun(join(root, 'bin'), '1.4.2');
    const refusal = await checkRuntimePin({
      root,
      command: 'bun test',
      env: { PATH: join(root, 'bin') },
      cwd: root,
      pathSource: "the target's env",
    });

    const message = describeRuntimePinRefusal(refusal ?? failed('no refusal'), 'nx-plugin:test-shard1');

    expect(message).toContain('nx-plugin:test-shard1: refusing to run the command');
    expect(message).toContain('declares Bun 1.4.3');
    expect(message).toContain('is Bun 1.4.2');
    expect(message).toContain(`packageManager "bun@1.4.3" in ${join(root, 'package.json')}`);
    expect(message).toContain(`PATH from the target's env; its first \`bun\` is ${bun}, which prints 1.4.2`);
    // The value of the refusal is invisible unless the symptom it replaces is written next to it.
    expect(message).toContain('cut mid-string while the child exits 0');
    expect(message.endsWith('\n')).toBe(true);
  });

  it('lets a leg run on the declared version', async () => {
    const root = await workspace(declaring('bun@1.4.3'));
    await fakeBun(join(root, 'bin'), '1.4.3');

    const refusal = await checkRuntimePin({
      root,
      command: 'bun test',
      env: { PATH: join(root, 'bin') },
      cwd: root,
      pathSource: 'the inherited environment',
    });

    expect(refusal).toBeNull();
  });

  it('judges the first bun on PATH, the one the command gets, not a later one', async () => {
    const root = await workspace(declaring('bun@1.4.3'));
    await fakeBun(join(root, 'shadowing'), '1.4.2');
    await fakeBun(join(root, 'declared'), '1.4.3');
    const check = { root, command: 'bun test', cwd: root, pathSource: 'the inherited environment' } as const;

    const shadowed = await checkRuntimePin({
      ...check,
      env: { PATH: [join(root, 'shadowing'), join(root, 'declared')].join(delimiter) },
    });
    const leading = await checkRuntimePin({
      ...check,
      env: { PATH: [join(root, 'declared'), join(root, 'shadowing')].join(delimiter) },
    });

    expect(shadowed).toMatchObject({ code: 'mismatch', bun: join(root, 'shadowing', 'bun'), version: '1.4.2' });
    expect(leading).toBeNull();
  });

  it('asks nothing of a bun it has no reason to ask: no declaration, no bun in the command, no bun on PATH', async () => {
    const declared = await workspace(declaring('bun@1.4.3'));
    const undeclared = await workspace('{"name":"workspace"}');
    const another = await workspace(declaring('pnpm@9.1.0'));
    await fakeBun(join(declared, 'bin'), '1.4.2');
    await fakeBun(join(undeclared, 'bin'), '1.4.2');
    await fakeBun(join(another, 'bin'), '1.4.2');
    const pathOf = (root: string): NodeJS.ProcessEnv => ({ PATH: join(root, 'bin') });

    const results = [
      // A command that names no bun: whatever Bun is on PATH, the leg does not run it.
      await checkRuntimePin({
        root: declared,
        command: 'cargo nextest run',
        env: pathOf(declared),
        cwd: declared,
        pathSource: 'the inherited environment',
      }),
      // A workspace that declares no Bun, or another package manager.
      await checkRuntimePin({
        root: undeclared,
        command: 'bun test',
        env: pathOf(undeclared),
        cwd: undeclared,
        pathSource: 'the inherited environment',
      }),
      await checkRuntimePin({
        root: another,
        command: 'bun test',
        env: pathOf(another),
        cwd: another,
        pathSource: 'the inherited environment',
      }),
      // No bun on PATH: the shell says so when the command runs.
      await checkRuntimePin({
        root: declared,
        command: 'bun test',
        env: { PATH: join(declared, 'nowhere') },
        cwd: declared,
        pathSource: 'the inherited environment',
      }),
    ];

    expect(results).toEqual([null, null, null, null]);
    for (const root of [declared, undeclared, another]) {
      expect(await exists(join(root, 'bin', FAKE_BUN_INVOKED))).toBe(false);
    }
  });

  it('refuses a bun that does not say which version it is, naming what it said', async () => {
    const root = await workspace(declaring('bun@1.4.3'));
    const bun = await failingBun(join(root, 'bin'), 'wrapper has no route');

    const refusal = await checkRuntimePin({
      root,
      command: 'bun test',
      env: { PATH: join(root, 'bin') },
      cwd: root,
      pathSource: 'the inherited environment',
    });

    expect(refusal).toMatchObject({ code: 'unreadable-version', bun });
    expect(refusal?.code === 'unreadable-version' ? refusal.reason : '').toContain('wrapper has no route');
  });

  it('refuses a declaration it cannot compare against before it looks for a bun', async () => {
    const root = await workspace(declaring('bun@latest'));
    await fakeBun(join(root, 'bin'), '1.4.3');

    const refusal = await checkRuntimePin({
      root,
      command: 'bun test',
      env: { PATH: join(root, 'bin') },
      cwd: root,
      pathSource: 'the inherited environment',
    });

    expect(refusal).toEqual({
      code: 'malformed-declaration',
      manifest: join(root, 'package.json'),
      declaration: 'bun@latest',
    });
    expect(await exists(join(root, 'bin', FAKE_BUN_INVOKED))).toBe(false);
  });
});

describe('describeRuntimePinRefusal', () => {
  const declared = { manifest: '/workspace/package.json', declaration: 'bun@1.4.3', version: '1.4.3' };

  it('says what each refusal is about, and who refused when the run has no task', () => {
    const unreadable: RuntimePinRefusal = {
      code: 'unreadable-version',
      declared,
      bun: '/path/to/bun',
      reason: '`bun --version` failed: exit 3',
      pathSource: 'the inherited environment',
    };
    const malformed: RuntimePinRefusal = {
      code: 'malformed-declaration',
      manifest: '/workspace/package.json',
      declaration: 'bun@latest',
    };
    const manifest: RuntimePinRefusal = {
      code: 'unreadable-manifest',
      manifest: '/workspace/package.json',
      reason: 'Unexpected token',
    };

    expect(describeRuntimePinRefusal(unreadable)).toContain(
      'bounded-exec: refusing to run the command: the workspace declares Bun 1.4.3, and the `bun` it would run did not say',
    );
    expect(describeRuntimePinRefusal(unreadable)).toContain('/path/to/bun: `bun --version` failed: exit 3');
    expect(describeRuntimePinRefusal(unreadable)).toContain('cut mid-string while the child exits 0');
    expect(describeRuntimePinRefusal(malformed)).toContain('declares packageManager "bun@latest", which names no Bun');
    expect(describeRuntimePinRefusal(manifest)).toContain(
      "cannot read the workspace's declaration in /workspace/package.json: Unexpected token",
    );
  });
});

function failed(message: string): never {
  throw new Error(message);
}
