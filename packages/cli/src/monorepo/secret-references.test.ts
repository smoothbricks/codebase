import { describe, expect, it } from 'bun:test';
import { existsSync, readFileSync } from 'node:fs';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { managedAssetsRoot } from '@smoothbricks/nx-plugin/managed-assets';
import type { SecretSpec } from '../../../nx-plugin/managed/raw/tooling/direnv/secret-references.ts';

/**
 * The resolver under test is a managed raw script: at runtime it loads as
 * plain Bun source during bootstrap, BEFORE any workspace package — and
 * therefore before the ttsc/Typia transform preloads exist. The tests honor
 * that: the `import type` above typechecks this file against the real module
 * under `tsconfig.test.json` but is erased at runtime, and every behavioral
 * test loads the raw module in a plain `bun -e` child the same way
 * setup-environment.ts does, with no preloads in the chain.
 */
const RAW_RESOLVER = join(managedAssetsRoot, 'raw/tooling/direnv/secret-references.ts');

// The child scripts import the raw module dynamically on purpose: a static
// value import would route it through the ttsc/Typia transform preloads, which
// is exactly the chain bootstrap never has. Loading via a plain bun child
// reproduces the real bootstrap module load — no plugins.
const RESOLVE_SCRIPT = `
const resolver = await import(Bun.argv[1]);
const request = JSON.parse(Bun.argv[2]);
try {
  const { values, deferred } = await resolver.resolveSecretEnvironment(request);
  process.stdout.write(JSON.stringify({ resolved: values, deferred }));
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

const GROUPS_SCRIPT = `
const resolver = await import(Bun.argv[1]);
const { root } = JSON.parse(Bun.argv[2]);
const groups = resolver.readSecretGroups(root).map(({ name, group, derivedGroup }) => ({ name, group, derivedGroup }));
process.stdout.write(JSON.stringify({ groups }));
`;

interface BootstrapOutcome {
  readonly resolved?: Record<string, string>;
  readonly deferred?: readonly { readonly name: string; readonly group: string; readonly guidance: string }[];
  readonly parsed?: unknown;
  readonly error?: string;
}

interface GroupListing {
  readonly groups: { name: string; group: string; derivedGroup: string }[];
}

function isGroupListing(value: unknown): value is GroupListing {
  if (typeof value !== 'object' || value === null || !('groups' in value) || !Array.isArray(value.groups)) return false;
  return value.groups.every(
    (entry: unknown) =>
      typeof entry === 'object' &&
      entry !== null &&
      'name' in entry &&
      typeof entry.name === 'string' &&
      'group' in entry &&
      typeof entry.group === 'string' &&
      'derivedGroup' in entry &&
      typeof entry.derivedGroup === 'string',
  );
}

/** Narrows a harness JSON envelope with `in` checks instead of assertions: unknown shapes fail the test. Field-level expectations below do the precise checking. */
function isBootstrapOutcome(value: unknown): value is BootstrapOutcome {
  if (typeof value !== 'object' || value === null) return false;
  if ('resolved' in value && (typeof value.resolved !== 'object' || value.resolved === null)) return false;
  if ('deferred' in value && !Array.isArray(value.deferred)) return false;
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

/** `emit`, plus one line appended to `ledger` every time the command actually runs. */
const emitAndRecord = (text: string, ledger: string): readonly [string, ...string[]] => [
  process.execPath,
  '-e',
  `require('node:fs').appendFileSync(${JSON.stringify(ledger)}, '1\\n');process.stdout.write(${JSON.stringify(text)})`,
];

/**
 * A ledger a provider command appends to, and the count of what it recorded.
 * Counting invocations is the only direct evidence that a path ran no
 * provider command: an absent value is also what a silently failing provider
 * leaves behind, and "no credential prompt" is the whole point of the rule.
 */
async function withProviderLedger(
  run: (ledger: { readonly path: string; readonly invocations: () => number }) => Promise<void>,
): Promise<void> {
  const dir = await mkdtemp(join(tmpdir(), 'smoo-provider-ledger-'));
  const path = join(dir, 'invocations');
  try {
    await run({
      path,
      invocations: () =>
        existsSync(path)
          ? readFileSync(path, 'utf8')
              .split('\n')
              .filter((line) => line.length > 0).length
          : 0,
    });
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
}
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
  /**
   * Defaults to the group shell entry resolves, which is what a direnv
   * reload performs. The resolver itself declares no default — resolving the
   * wrong group is a credential prompt — so every case that is not shell
   * entry names its group out loud.
   */
  group?: string;
}): Promise<BootstrapOutcome> {
  const { stdout, stderr, exitCode } = await runInBootstrap(
    RESOLVE_SCRIPT,
    JSON.stringify({ root: options.root, env: options.env ?? {}, group: options.group ?? 'shell' }),
  );
  expect(stderr).toBe('');
  expect(exitCode).toBe(0);
  return mustOutcome(stdout);
}

/** The groups this repository's own declarations settle on, per declared variable. */
async function groupsInBootstrap(root: string): Promise<{ name: string; group: string; derivedGroup: string }[]> {
  const { stdout, stderr, exitCode } = await runInBootstrap(GROUPS_SCRIPT, JSON.stringify({ root }));
  expect(stderr).toBe('');
  expect(exitCode).toBe(0);
  const value: unknown = JSON.parse(stdout);
  if (!isGroupListing(value)) throw new Error(`bootstrap harness returned a malformed listing: ${stdout}`);
  return value.groups;
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

  it('accepts any group label the command line can name, and rejects one it cannot', async () => {
    // The shape is validated, never the value: a repository naming a group
    // of its own must not need a new smoo release to run it.
    expect(
      (await parse({ smoo: { secrets: { SMOO_TOKEN: { command: ['op'], group: 'deploy-eu' } } } })).parsed,
    ).toEqual({ SMOO_TOKEN: { command: ['op'], group: 'deploy-eu' } });
    for (const malformed of ['', 'two words', 7]) {
      const outcome = await parse({ smoo: { secrets: { SMOO_TOKEN: { command: ['op'], group: malformed } } } });
      expect(outcome.error).toContain('SMOO_TOKEN');
      expect(outcome.error).toContain('group');
    }
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
      await names('@acme:registry=https://npm.example.net\n//npm.example.net/:_authToken=${SMOO_READ_TOKEN}\n'),
    ).toEqual(['SMOO_READ_TOKEN']);
  });

  it('an absent .npmrc contributes nothing', async () => {
    expect(await names(null)).toEqual([]);
  });
});

/**
 * The group is derived from declarations the repository already carries, so
 * a manifest states a group only where those cannot: nobody restates a fact
 * the repo already declares.
 */
describe('group derivation', () => {
  const REGISTRY_NPMRC = '//npm.example.net/:_authToken=${SMOO_NPM_TOKEN}\n';

  it('derives registry from a .npmrc reference, nx-cache from the cache token, and shell from neither', async () => {
    await withFixture(
      {
        npmrc: REGISTRY_NPMRC,
        secrets: {
          SMOO_NPM_TOKEN: { command: emit('x') },
          NX_REMOTE_CACHE_TOKEN: { command: emit('x') },
          SMOO_TOKEN: { command: emit('x') },
        },
        remoteCache: { server: 'https://nx-cache.example.net', tokenSecret: 'NX_REMOTE_CACHE_TOKEN' },
      },
      async (root) => {
        expect(await groupsInBootstrap(root)).toEqual([
          { name: 'SMOO_NPM_TOKEN', group: 'registry', derivedGroup: 'registry' },
          { name: 'NX_REMOTE_CACHE_TOKEN', group: 'nx-cache', derivedGroup: 'nx-cache' },
          { name: 'SMOO_TOKEN', group: 'shell', derivedGroup: 'shell' },
        ]);
      },
    );
  });

  it('a declared group wins, and the derivation it overrode stays visible', async () => {
    await withFixture(
      {
        npmrc: REGISTRY_NPMRC,
        secrets: {
          SMOO_NPM_TOKEN: { command: emit('x'), group: 'shell' },
          SMOO_TOKEN: { command: emit('x'), group: 'deploy' },
        },
      },
      async (root) => {
        expect(await groupsInBootstrap(root)).toEqual([
          { name: 'SMOO_NPM_TOKEN', group: 'shell', derivedGroup: 'registry' },
          { name: 'SMOO_TOKEN', group: 'deploy', derivedGroup: 'shell' },
        ]);
      },
    );
  });

  it('the cache token stays nx-cache even when .npmrc also references it', async () => {
    await withFixture(
      {
        npmrc: '//npm.example.net/:_authToken=${NX_REMOTE_CACHE_TOKEN}\n',
        secrets: { NX_REMOTE_CACHE_TOKEN: { command: emit('x') } },
        remoteCache: { server: 'https://nx-cache.example.net', tokenSecret: 'NX_REMOTE_CACHE_TOKEN' },
      },
      async (root) => {
        // The by-name exclusion this replaced was applied before the .npmrc
        // reference was ever consulted; the derivation keeps that order, so
        // one pathological manifest behaves exactly as it did.
        expect(await groupsInBootstrap(root)).toEqual([
          { name: 'NX_REMOTE_CACHE_TOKEN', group: 'nx-cache', derivedGroup: 'nx-cache' },
        ]);
      },
    );
  });

  it('a repository declaring nothing has no groups', async () => {
    await withFixture({ secrets: {} }, async (root) => {
      expect(await groupsInBootstrap(root)).toEqual([]);
    });
  });
});

describe('resolveSecretEnvironment', () => {
  it('an existing nonempty environment value wins and no command runs', async () => {
    await withFixture(
      { secrets: { SMOO_TOKEN: { command: ['definitely-not-a-real-smoo-binary-xyz'] } } },
      async (root) => {
        // The declared command cannot run at all, so a resolution attempt would
        // fail loudly; an empty outcome proves the existing value preempted it.
        expect(await resolveInBootstrap({ root, env: { SMOO_TOKEN: 'already-set' } })).toEqual({
          resolved: {},
          deferred: [],
        });
      },
    );
  });

  it('an empty environment value does not win; the provider supplies the value', async () => {
    await withFixture({ secrets: { SMOO_TOKEN: { command: emit('tok-from-provider') } } }, async (root) => {
      expect(await resolveInBootstrap({ root, env: { SMOO_TOKEN: '' } })).toEqual({
        resolved: { SMOO_TOKEN: 'tok-from-provider' },
        deferred: [],
      });
    });
  });

  it('trims one terminal newline, LF or CRLF, preserving interior content', async () => {
    await withFixture({ secrets: { SMOO_TOKEN: { command: emit('tok-from-provider\n') } } }, async (root) => {
      expect(await resolveInBootstrap({ root, env: {} })).toEqual({
        resolved: { SMOO_TOKEN: 'tok-from-provider' },
        deferred: [],
      });
    });
    await withFixture({ secrets: { SMOO_TOKEN: { command: emit('tok\r\nmid\r\n') } } }, async (root) => {
      expect(await resolveInBootstrap({ root, env: {} })).toEqual({
        resolved: { SMOO_TOKEN: 'tok\r\nmid' },
        deferred: [],
      });
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
        // The cache token is not excluded by name any more — it is in group
        // `nx-cache`, which shell entry defers, so its provider is never
        // reached to be unreachable.
        expect(await resolveInBootstrap({ root, env: {} })).toEqual({
          resolved: { SMOO_TOKEN: 'tok-from-provider' },
          deferred: [
            { name: 'NX_REMOTE_CACHE_TOKEN', group: 'nx-cache', guidance: expect.stringContaining('nx-cache') },
          ],
        });
        const withoutCacheDeclaration = await resolveInBootstrap({ root, env: { CI: 'true' } });
        expect(withoutCacheDeclaration.error).toContain('SMOO_TOKEN');
        // CI refuses what shell entry needs, and the cache token is not that:
        // nothing shell entry runs reads `smoo.remoteCache`.
        expect(withoutCacheDeclaration.error).not.toContain('NX_REMOTE_CACHE_TOKEN');
      },
    );
  });

  it('the declared cache token is deferred at shell entry and its provider never runs', async () => {
    await withProviderLedger(async (ledger) => {
      await withFixture(
        {
          secrets: { NX_REMOTE_CACHE_TOKEN: { command: emitAndRecord('cache-tok', ledger.path) } },
          remoteCache: { server: 'https://nx-cache.example.net', tokenSecret: 'NX_REMOTE_CACHE_TOKEN' },
        },
        async (root) => {
          // Named in the result rather than silently omitted, which is what
          // the by-name exclusion this replaces used to do.
          expect(await resolveInBootstrap({ root, env: {} })).toEqual({
            resolved: {},
            deferred: [
              {
                name: 'NX_REMOTE_CACHE_TOKEN',
                group: 'nx-cache',
                guidance: expect.stringContaining('smoo secrets run nx-cache'),
              },
            ],
          });
          expect(ledger.invocations()).toBe(0);

          // And a cowshed workspace does not refuse it either: the gateway
          // rule is about registry credentials, and this is not one.
          expect(await resolveInBootstrap({ root, env: { COWSHED_WORKSPACE_TOKEN: 'gateway-session' } })).toEqual({
            resolved: {},
            deferred: [
              {
                name: 'NX_REMOTE_CACHE_TOKEN',
                group: 'nx-cache',
                guidance: expect.stringContaining('smoo secrets run nx-cache'),
              },
            ],
          });
          expect(ledger.invocations()).toBe(0);
        },
      );
    });
  });

  it('a deliberate nx-cache run resolves the cache token nothing else resolves', async () => {
    await withProviderLedger(async (ledger) => {
      await withFixture(
        {
          secrets: {
            NX_REMOTE_CACHE_TOKEN: { command: emitAndRecord('cache-tok', ledger.path) },
            SMOO_TOKEN: { command: emitAndRecord('shell-tok', ledger.path) },
          },
          remoteCache: { server: 'https://nx-cache.example.net', tokenSecret: 'NX_REMOTE_CACHE_TOKEN' },
        },
        async (root) => {
          const outcome = await resolveInBootstrap({ root, env: {}, group: 'nx-cache' });

          expect(outcome.resolved).toEqual({ NX_REMOTE_CACHE_TOKEN: 'cache-tok' });
          expect(outcome.deferred).toEqual([
            { name: 'SMOO_TOKEN', group: 'shell', guidance: expect.stringContaining('smoo secrets run shell') },
          ]);
          // One group, one provider invocation: the shell secret's command
          // did not run for a cache-token request.
          expect(ledger.invocations()).toBe(1);
        },
      );
    });
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

  it('a cowshed workspace refuses a registry credential rather than deferring it', async () => {
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
          deferred: [],
        });
      },
    );
  });

  // A registry credential is the second instance of the rule the resolver's
  // header states: shell entry never runs a provider command for a credential
  // that only a network operation needs. The reference is what makes it one.
  const REGISTRY_NPMRC = '@acme:registry=https://npm.example.net\n//npm.example.net/:_authToken=${SMOO_NPM_TOKEN}\n';

  it('shell entry defers a .npmrc-referenced variable and never runs its provider', async () => {
    await withProviderLedger(async (ledger) => {
      await withFixture(
        {
          npmrc: REGISTRY_NPMRC,
          secrets: { SMOO_NPM_TOKEN: { command: emitAndRecord('SHELL-ENTRY-RESOLVED-VALUE', ledger.path) } },
        },
        async (root) => {
          const outcome = await resolveInBootstrap({ root, env: {}, group: 'shell' });

          expect(outcome.resolved).toEqual({});
          expect(outcome.error).toBeUndefined();
          expect(ledger.invocations()).toBe(0);
          // A failing install prints `- <name>: <guidance>`, so both halves
          // have to be here: which variable, and the one command that
          // supplies it.
          expect(outcome.deferred).toEqual([
            {
              name: 'SMOO_NPM_TOKEN',
              group: 'registry',
              guidance: expect.stringContaining('smoo secrets run registry <command>'),
            },
          ]);
        },
      );
    });
  });

  it('a declared variable .npmrc does not reference still resolves at shell entry', async () => {
    await withProviderLedger(async (ledger) => {
      await withFixture(
        { npmrc: REGISTRY_NPMRC, secrets: { SMOO_GENERIC: { command: emitAndRecord('generic-tok', ledger.path) } } },
        async (root) => {
          // The same .npmrc, a variable it does not name: the deferral keys on
          // the reference, not on a registry existing somewhere in the repo.
          expect(await resolveInBootstrap({ root, env: {}, group: 'shell' })).toEqual({
            resolved: { SMOO_GENERIC: 'generic-tok' },
            deferred: [],
          });
          expect(ledger.invocations()).toBe(1);
        },
      );
    });
  });

  it('a deliberate registry run resolves the variable shell entry deferred, and nothing else', async () => {
    await withProviderLedger(async (ledger) => {
      await withFixture(
        {
          npmrc: REGISTRY_NPMRC,
          secrets: {
            SMOO_NPM_TOKEN: { command: emitAndRecord('registry-tok', ledger.path) },
            SMOO_TOKEN: { command: emitAndRecord('shell-tok', ledger.path) },
          },
        },
        async (root) => {
          const outcome = await resolveInBootstrap({ root, env: {}, group: 'registry' });

          expect(outcome.resolved).toEqual({ SMOO_NPM_TOKEN: 'registry-tok' });
          // The shell secret is deferred, not resolved: one group, one
          // provider invocation. A wrapper that resolved every declared
          // secret would have run two.
          expect(outcome.deferred).toEqual([
            { name: 'SMOO_TOKEN', group: 'shell', guidance: expect.stringContaining('smoo secrets run shell') },
          ]);
          expect(ledger.invocations()).toBe(1);
        },
      );
    });
  });

  it('a declared group is what shell entry resolves, derivation notwithstanding', async () => {
    await withProviderLedger(async (ledger) => {
      await withFixture(
        {
          npmrc: REGISTRY_NPMRC,
          secrets: { SMOO_NPM_TOKEN: { command: emitAndRecord('registry-tok', ledger.path), group: 'shell' } },
        },
        async (root) => {
          // The repository says this credential belongs to the shell, and
          // that is the whole of the rule: the override is not a hint.
          expect(await resolveInBootstrap({ root, env: {}, group: 'shell' })).toEqual({
            resolved: { SMOO_NPM_TOKEN: 'registry-tok' },
            deferred: [],
          });
          expect(ledger.invocations()).toBe(1);
        },
      );
    });
  });

  it('an exported registry credential wins over the deferral', async () => {
    await withProviderLedger(async (ledger) => {
      await withFixture(
        { npmrc: REGISTRY_NPMRC, secrets: { SMOO_NPM_TOKEN: { command: emitAndRecord('x', ledger.path) } } },
        async (root) => {
          // Nothing to defer and nothing to run: the developer who exported it
          // for this terminal already answered the question.
          expect(await resolveInBootstrap({ root, env: { SMOO_NPM_TOKEN: 'exported' }, group: 'shell' })).toEqual({
            resolved: {},
            deferred: [],
          });
          expect(ledger.invocations()).toBe(0);
        },
      );
    });
  });

  it('CI refuses a missing registry credential instead of deferring it', async () => {
    await withProviderLedger(async (ledger) => {
      await withFixture(
        { npmrc: REGISTRY_NPMRC, secrets: { SMOO_NPM_TOKEN: { command: emitAndRecord('x', ledger.path) } } },
        async (root) => {
          // CI outranks the registry rule and is unchanged by it: a job that
          // needs the credential says so at setup rather than discovering it
          // at the first fetch, and there is no wrapper to run there.
          const outcome = await resolveInBootstrap({ root, env: { CI: 'true' }, group: 'shell' });

          expect(outcome.error).toContain('SMOO_NPM_TOKEN');
          expect(outcome.error).toContain('inject');
          expect(outcome.error).not.toContain('smoo secrets run');
          expect(ledger.invocations()).toBe(0);
        },
      );
    });
  });

  it('a cowshed workspace refuses a registry credential for a network operation too', async () => {
    await withProviderLedger(async (ledger) => {
      await withFixture(
        {
          npmrc: REGISTRY_NPMRC,
          secrets: { SMOO_NPM_TOKEN: { command: emitAndRecord('LOCAL-RESOLVED-VALUE', ledger.path) } },
        },
        async (root) => {
          // The gateway is the enrollment path there, so the wrapper does not
          // become a way around it.
          const outcome = await resolveInBootstrap({
            root,
            env: { COWSHED_WORKSPACE_TOKEN: 'gateway-session' },
            group: 'registry',
          });

          expect(outcome.error).toContain('SMOO_NPM_TOKEN');
          expect(outcome.error).toContain('cowshed gateway');
          expect(outcome.error).not.toContain('LOCAL-RESOLVED-VALUE');
          expect(ledger.invocations()).toBe(0);
        },
      );
    });
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
