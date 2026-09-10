import { describe, expect, it } from 'bun:test';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import type { SecretSpec } from '../../managed/raw/tooling/direnv/secret-references.ts';

/**
 * The resolver under test is a managed raw script: at runtime it loads as
 * plain Bun source during bootstrap, BEFORE any workspace package — and
 * therefore before the ttsc/Typia transform preloads exist. The tests honor
 * that: the `import type` above typechecks this file against the real module
 * under `tsconfig.test.json` but is erased at runtime, and every behavioral
 * test loads the raw module in a plain `bun -e` child the same way
 * setup-environment.ts does, with no preloads in the chain.
 */
const RAW_RESOLVER = resolve(
  import.meta.dir,
  '..',
  '..',
  'managed',
  'raw',
  'tooling',
  'direnv',
  'secret-references.ts',
);

// The child scripts import the raw module dynamically on purpose: a static
// value import would route it through the ttsc/Typia transform preloads, which
// is exactly the chain bootstrap never has. Loading via a plain bun child
// reproduces the real bootstrap module load — no plugins.
const RESOLVE_SCRIPT = `
const resolver = await import(Bun.argv[1]);
const request = JSON.parse(Bun.argv[2]);
try {
  process.stdout.write(JSON.stringify({ resolved: await resolver.resolveSecretEnvironment(request) }));
} catch (error) {
  process.stdout.write(JSON.stringify({ error: error instanceof Error ? error.message : String(error) }));
}
`;

const PARSE_SCRIPT = `
const resolver = await import(Bun.argv[1]);
try {
  process.stdout.write(JSON.stringify({ parsed: resolver.parseSmooSecrets(JSON.parse(Bun.argv[2])) }));
} catch (error) {
  process.stdout.write(JSON.stringify({ error: error instanceof Error ? error.message : String(error) }));
}
`;

const NPMRC_SCRIPT = `
const resolver = await import(Bun.argv[1]);
const npmrc = JSON.parse(Bun.argv[2]);
const names = [...resolver.registryAuthEnvNames(npmrc)];
process.stdout.write(JSON.stringify({ names }));
`;

interface BootstrapOutcome {
  readonly resolved?: Record<string, string>;
  readonly parsed?: unknown;
  readonly error?: string;
}

/** Narrows a harness JSON envelope with `in` checks instead of assertions: unknown shapes fail the test. Field-level expectations below do the precise checking. */
function isBootstrapOutcome(value: unknown): value is BootstrapOutcome {
  if (typeof value !== 'object' || value === null) return false;
  if ('resolved' in value && (typeof value.resolved !== 'object' || value.resolved === null)) return false;
  if ('error' in value && typeof value.error !== 'string') return false;
  return true;
}
function mustOutcome(stdout: string): BootstrapOutcome {
  const value: unknown = JSON.parse(stdout);
  if (!isBootstrapOutcome(value)) throw new Error(`bootstrap harness returned a malformed envelope: ${stdout}`);
  return value;
}

function mustNames(stdout: string): string[] {
  const value: unknown = JSON.parse(stdout);
  if (typeof value !== 'object' || value === null || !('names' in value)) {
    throw new Error(`bootstrap harness returned a malformed envelope: ${stdout}`);
  }
  const names: unknown = value.names;
  if (!Array.isArray(names) || !names.every((entry: unknown): entry is string => typeof entry === 'string')) {
    throw new Error(`bootstrap harness returned malformed names: ${stdout}`);
  }
  return names;
}

/**
 * Provider commands must run on macOS and NixOS alike, whose /bin and
 * /usr/bin layouts differ (NixOS has neither echo nor printf there), so the
 * tests execute Bun itself instead of host utilities. Real spawn path,
 * portable binary.
 */
const emit = (text: string): readonly [string, ...string[]] => [
  process.execPath,
  '-e',
  `process.stdout.write(${JSON.stringify(text)})`,
];
/**
 * Runs one script against the raw module in a plain Bun child. The child env
 * carries only PATH/HOME so parent suite state (CI, workspace tokens) cannot
 * leak into the routing flags; every decision input travels through the
 * explicit request instead.
 */
async function runInBootstrap(
  script: string,
  ...payload: string[]
): Promise<{ stdout: string; stderr: string; exitCode: number }> {
  const proc = Bun.spawn({
    cmd: ['bun', '-e', script, RAW_RESOLVER, ...payload],
    env: { PATH: process.env['PATH'], HOME: process.env['HOME'] },
    stdin: 'ignore',
    stdout: 'pipe',
    stderr: 'pipe',
  });
  const [stdout, stderr, exitCode] = await Promise.all([
    new Response(proc.stdout).text(),
    new Response(proc.stderr).text(),
    proc.exited,
  ]);
  return { stdout, stderr, exitCode };
}

async function resolveInBootstrap(options: {
  root: string;
  env?: Record<string, string | undefined>;
}): Promise<BootstrapOutcome> {
  const { stdout, stderr, exitCode } = await runInBootstrap(
    RESOLVE_SCRIPT,
    JSON.stringify({ root: options.root, env: options.env ?? {} }),
  );
  expect(stderr).toBe('');
  expect(exitCode).toBe(0);
  return mustOutcome(stdout);
}

/** Builds a fixture repository whose package.json and .npmrc are the resolver's real inputs. */
async function withFixture(
  options: { secrets: Record<string, SecretSpec>; npmrc?: string; remoteCache?: unknown },
  run: (root: string) => Promise<void>,
): Promise<void> {
  const root = await mkdtemp(join(tmpdir(), 'smoo-secret-references-'));
  try {
    await writeFile(
      join(root, 'package.json'),
      JSON.stringify({
        name: 'fixture',
        version: '0.0.0',
        smoo: { secrets: options.secrets, remoteCache: options.remoteCache },
      }),
    );
    if (options.npmrc !== undefined) {
      await writeFile(join(root, '.npmrc'), options.npmrc);
    }
    await run(root);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}

describe('smoo.secrets shape validation', () => {
  const parse = async (packageJson: unknown): Promise<BootstrapOutcome> => {
    const { stdout, exitCode } = await runInBootstrap(PARSE_SCRIPT, JSON.stringify(packageJson));
    expect(exitCode).toBe(0);
    return mustOutcome(stdout);
  };

  it('returns an empty map when nothing is declared', async () => {
    expect((await parse({ name: 'fixture' })).parsed).toEqual({});
    expect((await parse({ name: 'fixture', smoo: {} })).parsed).toEqual({});
    expect((await parse({ smoo: { secrets: {} } })).parsed).toEqual({});
  });

  it('accepts a declared command map', async () => {
    const outcome = await parse({ smoo: { secrets: { SMOO_TOKEN: { command: ['op', 'read', 'smoo'] } } } });
    expect(outcome.parsed).toEqual({ SMOO_TOKEN: { command: ['op', 'read', 'smoo'] } });
  });

  it('rejects malformed shapes naming the variable, never a value', async () => {
    const secretCommand = 'op read op://vault/super-secret';
    for (const malformed of [
      { smoo: { secrets: { SMOO_TOKEN: secretCommand } } },
      { smoo: { secrets: { SMOO_TOKEN: { command: secretCommand } } } },
      { smoo: { secrets: { SMOO_TOKEN: { command: [] } } } },
      { smoo: { secrets: { SMOO_TOKEN: { command: ['op', 7] } } } },
    ]) {
      const outcome = await parse(malformed);
      expect(outcome.error).toContain('SMOO_TOKEN');
      expect(outcome.error).not.toContain(secretCommand);
    }
  });

  it('rejects environment names a shell cannot export', async () => {
    expect((await parse({ smoo: { secrets: { 'has-dash': { command: ['op'] } } } })).error).toContain('has-dash');
    expect((await parse({ smoo: { secrets: { 'a b': { command: ['op'] } } } })).error).toContain('a b');
    expect((await parse({ smoo: { secrets: { '': { command: ['op'] } } } })).error).toContain('smoo.secrets');
  });
});

describe('registryAuthEnvNames', () => {
  const names = async (npmrc: string | null): Promise<string[]> => {
    const { stdout, exitCode } = await runInBootstrap(NPMRC_SCRIPT, JSON.stringify(npmrc));
    expect(exitCode).toBe(0);
    return mustNames(stdout);
  };

  it('collects ${VAR} references from .npmrc text', async () => {
    expect(
      await names('@axe.sc:registry=https://npm.example.net\n//npm.example.net/:_authToken=${SMOO_READ_TOKEN}\n'),
    ).toEqual(['SMOO_READ_TOKEN']);
  });

  it('an absent .npmrc contributes nothing', async () => {
    expect(await names(null)).toEqual([]);
  });
});

describe('resolveSecretEnvironment', () => {
  it('an existing nonempty environment value wins and no command runs', async () => {
    await withFixture(
      { secrets: { SMOO_TOKEN: { command: ['definitely-not-a-real-smoo-binary-xyz'] } } },
      async (root) => {
        // The declared command cannot run at all, so a resolution attempt would
        // fail loudly; an empty outcome proves the existing value preempted it.
        expect(await resolveInBootstrap({ root, env: { SMOO_TOKEN: 'already-set' } })).toEqual({ resolved: {} });
      },
    );
  });

  it('an empty environment value does not win; the provider supplies the value', async () => {
    await withFixture({ secrets: { SMOO_TOKEN: { command: emit('tok-from-provider') } } }, async (root) => {
      expect(await resolveInBootstrap({ root, env: { SMOO_TOKEN: '' } })).toEqual({
        resolved: { SMOO_TOKEN: 'tok-from-provider' },
      });
    });
  });

  it('trims one terminal newline, LF or CRLF, preserving interior content', async () => {
    await withFixture({ secrets: { SMOO_TOKEN: { command: emit('tok-from-provider\n') } } }, async (root) => {
      expect(await resolveInBootstrap({ root, env: {} })).toEqual({ resolved: { SMOO_TOKEN: 'tok-from-provider' } });
    });
    await withFixture({ secrets: { SMOO_TOKEN: { command: emit('tok\r\nmid\r\n') } } }, async (root) => {
      expect(await resolveInBootstrap({ root, env: {} })).toEqual({ resolved: { SMOO_TOKEN: 'tok\r\nmid' } });
    });
  });

  it('missing variables in CI refuse with injected-secret guidance and run nothing', async () => {
    await withFixture({ secrets: { SMOO_MISSING: { command: emit('NEVER-RAN-VALUE') } } }, async (root) => {
      const outcome = await resolveInBootstrap({ root, env: { CI: 'true' } });
      expect(outcome.error).toContain('SMOO_MISSING');
      expect(outcome.error).toContain('inject');
      expect(outcome.error).not.toContain('NEVER-RAN-VALUE');
    });
  });

  it('an unreachable cache-token provider does not block the install, while other secrets still refuse', async () => {
    await withFixture(
      {
        secrets: {
          NX_REMOTE_CACHE_TOKEN: { command: ['definitely-not-a-real-smoo-binary-xyz'] },
          SMOO_TOKEN: { command: emit('tok-from-provider') },
        },
        remoteCache: { server: 'https://nx-cache.example.net', tokenSecret: 'NX_REMOTE_CACHE_TOKEN' },
      },
      async (root) => {
        // A cache is an optimization: its provider being down installs
        // dependencies anyway. Every other declared secret keeps its refusal.
        expect(await resolveInBootstrap({ root, env: {} })).toEqual({
          resolved: { SMOO_TOKEN: 'tok-from-provider' },
        });
        const withoutCacheDeclaration = await resolveInBootstrap({ root, env: { CI: 'true' } });
        expect(withoutCacheDeclaration.error).toContain('SMOO_TOKEN');
        expect(withoutCacheDeclaration.error).not.toContain('NX_REMOTE_CACHE_TOKEN');
      },
    );
  });

  it('a present variable is not reported while another refuses in CI', async () => {
    await withFixture(
      {
        secrets: {
          SMOO_MISSING: { command: emit('x') },
          SMOO_PRESENT: { command: emit('x') },
        },
      },
      async (root) => {
        const outcome = await resolveInBootstrap({ root, env: { CI: 'true', SMOO_PRESENT: 'injected' } });
        expect(outcome.error).toContain('SMOO_MISSING');
        expect(outcome.error).not.toContain('SMOO_PRESENT');
      },
    );
  });

  it('cowshed workspaces exclude .npmrc-referenced variables from local resolution', async () => {
    await withFixture(
      {
        npmrc: '//npm.example.net/:_authToken=${SMOO_NPM_TOKEN}\n',
        secrets: { SMOO_NPM_TOKEN: { command: emit('LOCAL-RESOLVED-VALUE') } },
      },
      async (root) => {
        const outcome = await resolveInBootstrap({ root, env: { COWSHED_WORKSPACE_TOKEN: 'gateway-session' } });
        expect(outcome.error).toContain('SMOO_NPM_TOKEN');
        expect(outcome.error).toContain('cowshed gateway');
        expect(outcome.error).not.toContain('LOCAL-RESOLVED-VALUE');
      },
    );
  });

  it('generic variables still resolve through the provider inside a cowshed workspace', async () => {
    await withFixture(
      {
        npmrc: '//npm.example.net/:_authToken=${SMOO_UNDECLARED}\n',
        secrets: { SMOO_GENERIC: { command: emit('generic-tok') } },
      },
      async (root) => {
        expect(await resolveInBootstrap({ root, env: { COWSHED_WORKSPACE_TOKEN: 'gateway-session' } })).toEqual({
          resolved: { SMOO_GENERIC: 'generic-tok' },
        });
      },
    );
  });

  it('provider failures name the variable, the exit code, and nothing secret-shaped', async () => {
    await withFixture(
      {
        secrets: {
          SMOO_TOKEN: {
            command: [process.execPath, '-e', 'process.stderr.write("token=sk-live-stderr\\n");process.exit(3)'],
          },
        },
      },
      async (root) => {
        const outcome = await resolveInBootstrap({ root, env: {} });
        expect(outcome.error).toContain('SMOO_TOKEN');
        expect(outcome.error).toContain('exited with code 3');
        expect(outcome.error).not.toContain('sk-live-stderr');
        expect(outcome.error).not.toContain('echo token');
      },
    );
  });

  it('an empty provider output refuses instead of assigning an empty secret', async () => {
    await withFixture({ secrets: { SMOO_TOKEN: { command: emit('') } } }, async (root) => {
      const outcome = await resolveInBootstrap({ root, env: {} });
      expect(outcome.error).toContain('SMOO_TOKEN');
      expect(outcome.error).toContain('no output');
    });
  });

  it('a missing provider program refuses with start guidance, echoing nothing', async () => {
    await withFixture(
      { secrets: { SMOO_TOKEN: { command: ['definitely-not-a-real-smoo-binary-xyz'] } } },
      async (root) => {
        const outcome = await resolveInBootstrap({ root, env: {} });
        expect(outcome.error).toContain('SMOO_TOKEN');
        expect(outcome.error).toContain('could not be started');
        expect(outcome.error).not.toContain('definitely-not-a-real-smoo-binary-xyz');
      },
    );
  });

  it('aggregates every failure into one refusal', async () => {
    await withFixture(
      {
        secrets: {
          SMOO_ONE: { command: ['definitely-not-a-real-smoo-binary-xyz'] },
          SMOO_TWO: { command: ['also-not-a-real-smoo-binary-xyz'] },
        },
      },
      async (root) => {
        const outcome = await resolveInBootstrap({ root, env: { CI: 'true' } });
        expect(outcome.error).toContain('SMOO_ONE');
        expect(outcome.error).toContain('SMOO_TWO');
      },
    );
  });

  it('a broken manifest refuses with the file named', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoo-secret-references-'));
    try {
      await writeFile(join(root, 'package.json'), '{ not json');
      const outcome = await resolveInBootstrap({ root, env: {} });
      expect(outcome.error).toContain('not valid JSON');
      expect(outcome.error).toContain('package.json');
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });
});

/**
 * Redaction is a byte operation on output the caller already captured, so the
 * harness carries both directions as base64: a text-only round trip would
 * hide exactly the invalid-UTF-8 bytes a failing install can emit.
 */
const MASK_SCRIPT = `
const resolver = await import(Bun.argv[1]);
const { output, values } = JSON.parse(Bun.argv[2]);
const captured = Buffer.from(output, 'base64');
const masked = resolver.maskSecretValues(captured, values);
process.stdout.write(JSON.stringify({
  masked: Buffer.from(masked).toString('base64'),
  captured: captured.toString('base64'),
}));
`;

describe('maskSecretValues', () => {
  const mask = async (output: Uint8Array, values: readonly string[]): Promise<{ masked: Buffer; captured: Buffer }> => {
    const { stdout, stderr, exitCode } = await runInBootstrap(
      MASK_SCRIPT,
      JSON.stringify({ output: Buffer.from(output).toString('base64'), values }),
    );
    expect(stderr).toBe('');
    expect(exitCode).toBe(0);
    const value: unknown = JSON.parse(stdout);
    if (
      typeof value !== 'object' ||
      value === null ||
      !('masked' in value) ||
      typeof value.masked !== 'string' ||
      !('captured' in value) ||
      typeof value.captured !== 'string'
    ) {
      throw new Error(`bootstrap harness returned a malformed envelope: ${stdout}`);
    }
    return { masked: Buffer.from(value.masked, 'base64'), captured: Buffer.from(value.captured, 'base64') };
  };

  it('replaces every occurrence and leaves the surrounding failure legible', async () => {
    const output = Buffer.from('error: 401 for https://x:hunter2@registry/pkg\nretrying with hunter2\n');
    const { masked, captured } = await mask(output, ['hunter2']);
    expect(masked.toString()).toBe('error: 401 for https://x:*******@registry/pkg\nretrying with *******\n');
    expect(masked.length).toBe(output.length);
    // The caller keeps its captured bytes: redaction returns a copy, and a
    // node Buffer's `slice` would have handed back a view of these.
    expect(captured.toString()).toBe(output.toString());
  });

  it('redacts each declared value even when one is a prefix of another', async () => {
    expect((await mask(Buffer.from('AB ABCD AB'), ['ABCD', 'AB'])).masked.toString()).toBe('** **** **');
  });

  it('survives a false start on the first byte', async () => {
    expect((await mask(Buffer.from('aab aaab'), ['aab'])).masked.toString()).toBe('*** a***');
  });

  it('masks whole bytes and carries invalid UTF-8 through untouched', async () => {
    const output = Buffer.concat([Buffer.from([0xff, 0xfe]), Buffer.from('kå'), Buffer.from([0x00])]);
    const { masked } = await mask(output, ['kå']);
    expect([...masked]).toEqual([0xff, 0xfe, 0x2a, 0x2a, 0x2a, 0x00]);
  });

  it('leaves output alone when nothing is declared or a value is empty', async () => {
    expect((await mask(Buffer.from('nothing to hide'), [])).masked.toString()).toBe('nothing to hide');
    expect((await mask(Buffer.from('nothing to hide'), [''])).masked.toString()).toBe('nothing to hide');
  });
});
