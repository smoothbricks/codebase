import { describe, expect, it } from 'bun:test';
import { existsSync, readFileSync } from 'node:fs';
import { mkdtemp, readdir, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import typia from 'typia';
import { runCli } from '../cli.js';
import { secretsRun } from './run.js';

/**
 * `smoo secrets run` is tested against real child processes and a real
 * repository on disk, because the two things it promises are exactly the
 * things a stub cannot show: that ONE group's provider commands run, and that
 * argv reaches the child the way it was typed.
 */

/** A provider command that records every invocation, so "no prompt" is a count and not an adjective. */
function provider(name: string, vault: string, ledger: string): [string, ...string[]] {
  // The value lives outside the repository under test, so "no secret value
  // on disk here" is a statement about what the command did, not about what
  // the fixture happened to write into its own manifest.
  return [
    process.execPath,
    '-e',
    `const fs = require('node:fs');fs.appendFileSync(${JSON.stringify(ledger)}, '1\\n');` +
      `process.stdout.write(fs.readFileSync(${JSON.stringify(join(vault, name))}, 'utf8'))`,
  ];
}

/** What a recorded child wrote down about itself: how it was called, and which declared variables it could see. */
interface ChildRecord {
  argv: string[];
  env: Record<string, string>;
}

const isChildRecord = typia.createIs<ChildRecord>();

interface Fixture {
  root: string;
  /** How many provider commands have run in this repository so far. */
  invocations: () => number;
  /** Whatever the last recorded child wrote down about itself. */
  record: () => ChildRecord;
  /** Every file the command left behind, as text. */
  files: () => Promise<string[]>;
}

/**
 * A repository declaring one secret in each derived group: a `.npmrc`
 * reference (`registry`), the declared cache token (`nx-cache`), and one that
 * is neither (`shell`).
 */
async function withRepo(
  run: (fixture: Fixture) => Promise<void>,
  options: { group?: Record<string, string> } = {},
): Promise<void> {
  const root = await mkdtemp(join(tmpdir(), 'smoo-secrets-run-'));
  const vault = await mkdtemp(join(tmpdir(), 'smoo-secrets-vault-'));
  const ledger = join(vault, 'provider-ledger');
  const recordPath = join(root, 'child-record.json');
  // The context under test is a developer machine, the only one that runs a
  // provider command at all. CI and a cowshed workspace are their own tests
  // below, and the suite's own environment — bun test runs under CI=true —
  // must not decide which of the three this is.
  const ambient = { CI: process.env.CI, COWSHED_WORKSPACE_TOKEN: process.env.COWSHED_WORKSPACE_TOKEN };
  delete process.env.CI;
  delete process.env.COWSHED_WORKSPACE_TOKEN;
  try {
    await writeFile(join(vault, 'REGISTRY_TOKEN'), 'registry-value');
    await writeFile(join(vault, 'NX_CACHE_TOKEN'), 'cache-value');
    await writeFile(join(vault, 'SHELL_TOKEN'), 'shell-value');
    await writeFile(
      join(root, 'package.json'),
      JSON.stringify({
        name: 'fixture',
        version: '0.0.0',
        smoo: {
          remoteCache: { server: 'https://nx-cache.example.net', tokenSecret: 'NX_CACHE_TOKEN' },
          secrets: {
            REGISTRY_TOKEN: {
              command: provider('REGISTRY_TOKEN', vault, ledger),
              ...groupOf('REGISTRY_TOKEN', options),
            },
            NX_CACHE_TOKEN: {
              command: provider('NX_CACHE_TOKEN', vault, ledger),
              ...groupOf('NX_CACHE_TOKEN', options),
            },
            SHELL_TOKEN: { command: provider('SHELL_TOKEN', vault, ledger), ...groupOf('SHELL_TOKEN', options) },
          },
        },
      }),
    );
    await writeFile(
      join(root, '.npmrc'),
      '@acme:registry=https://npm.example.net\n//npm.example.net/:_authToken=${REGISTRY_TOKEN}\n',
    );
    // A child that writes down exactly what it was given: its own argv, and
    // which of THIS fixture's three declared variables it can see. Named
    // one by one on purpose — a pattern over the whole environment would
    // copy whatever the developer running the suite happens to export.
    await writeFile(
      join(root, 'recorder.mjs'),
      `import { writeFileSync } from 'node:fs';
const declared = ['REGISTRY_TOKEN', 'NX_CACHE_TOKEN', 'SHELL_TOKEN'];
writeFileSync(${JSON.stringify(recordPath)}, JSON.stringify({
  argv: process.argv.slice(2),
  env: Object.fromEntries(
    declared.filter((name) => process.env[name] !== undefined).map((name) => [name, process.env[name]]),
  ),
}));
writeFileSync(${JSON.stringify(join(root, 'child-output.txt'))}, 'the command wrote this\\n');
`,
    );
    await run({
      root,
      invocations: () =>
        existsSync(ledger)
          ? readFileSync(ledger, 'utf8')
              .split('\n')
              .filter((line) => line.length > 0).length
          : 0,
      record: () => {
        const parsed: unknown = JSON.parse(readFileSync(recordPath, 'utf8'));
        if (!isChildRecord(parsed)) {
          throw new Error(`the recorded child wrote an unreadable record: ${readFileSync(recordPath, 'utf8')}`);
        }
        return parsed;
      },
      // Everything in the repository except the record the child was asked
      // to write: that one exists to prove the value arrived in the child's
      // environment, so it is the instrument, not evidence against it.
      files: async () => {
        const names = await readdir(root);
        return names
          .filter((name) => name !== 'node_modules' && name !== 'child-record.json')
          .map((name) => readFileSync(join(root, name), 'utf8'));
      },
    });
  } finally {
    for (const [name, value] of Object.entries(ambient)) {
      if (value === undefined) {
        delete process.env[name];
        continue;
      }
      process.env[name] = value;
    }
    await rm(root, { recursive: true, force: true });
    await rm(vault, { recursive: true, force: true });
  }
}

function groupOf(name: string, options: { group?: Record<string, string> }): { group?: string } {
  const group = options.group?.[name];
  return group === undefined ? {} : { group };
}

/** Everything the command said, and the code it returned. */
async function capture(run: () => Promise<number>): Promise<{ code: number; output: string }> {
  const lines: string[] = [];
  const { log, error } = console;
  console.log = (...args: unknown[]) => lines.push(args.join(' '));
  console.error = (...args: unknown[]) => lines.push(args.join(' '));
  try {
    return { code: await run(), output: lines.join('\n') };
  } finally {
    console.log = log;
    console.error = error;
  }
}

const recorder = (root: string, ...args: string[]): string[] => [process.execPath, join(root, 'recorder.mjs'), ...args];

describe('smoo secrets run', () => {
  it('with no group, lists the groups this repository declares and runs nothing', async () => {
    await withRepo(async ({ root, invocations }) => {
      const { code, output } = await capture(async () => secretsRun(root, undefined, []));

      expect(code).toBe(2);
      // The refusal IS the answer: which groups exist here, and what is in
      // each. Most people meet this command once, at a 401.
      expect(output).toContain('nx-cache');
      expect(output).toContain('NX_CACHE_TOKEN');
      expect(output).toContain('registry');
      expect(output).toContain('REGISTRY_TOKEN');
      expect(output).toContain('shell');
      expect(output).toContain('SHELL_TOKEN');
      expect(invocations()).toBe(0);
    });
  });

  it('names an unknown group and still lists what exists', async () => {
    await withRepo(async ({ root, invocations }) => {
      const { code, output } = await capture(async () => secretsRun(root, 'deploy', ['echo', 'hi']));

      expect(code).toBe(2);
      expect(output).toContain('deploy');
      expect(output).toContain('registry');
      expect(output).toContain('REGISTRY_TOKEN');
      expect(invocations()).toBe(0);
    });
  });

  it('refuses a group with no command, naming what it would have supplied', async () => {
    await withRepo(async ({ root, invocations }) => {
      const { code, output } = await capture(async () => secretsRun(root, 'registry', []));

      expect(code).toBe(2);
      expect(output).toContain('REGISTRY_TOKEN');
      expect(invocations()).toBe(0);
    });
  });

  it('resolves exactly the named group, and neither group resolves the other', async () => {
    await withRepo(async ({ root, invocations, record }) => {
      expect(await secretsRun(root, 'registry', recorder(root))).toBe(0);

      // One declared secret in the group, one provider invocation: a wrapper
      // that resolved everything declared would have run three.
      expect(invocations()).toBe(1);
      expect(record().env).toEqual({ REGISTRY_TOKEN: 'registry-value' });

      expect(await secretsRun(root, 'nx-cache', recorder(root))).toBe(0);

      expect(invocations()).toBe(2);
      expect(record().env).toEqual({ NX_CACHE_TOKEN: 'cache-value' });
    });
  });

  it('puts no secret value in the child argv or on disk', async () => {
    await withRepo(async ({ root, record, files }) => {
      expect(await secretsRun(root, 'registry', recorder(root, '--flag', 'positional'))).toBe(0);

      const { argv, env } = record();
      expect(env.REGISTRY_TOKEN).toBe('registry-value');
      // argv is world-readable through `ps`; the value travels in the
      // environment or nowhere.
      expect(argv).toEqual(['--flag', 'positional']);
      for (const contents of await files()) {
        expect(contents).not.toContain('registry-value');
      }
    });
  });

  it('reproduces the child exit status, including death by signal as 128+signum', async () => {
    await withRepo(async ({ root }) => {
      expect(await secretsRun(root, 'registry', [process.execPath, '-e', 'process.exit(17)'])).toBe(17);
      expect(await secretsRun(root, 'registry', [process.execPath, '-e', 'process.kill(process.pid, "SIGTERM")'])).toBe(
        143,
      );
    });
  });

  it('refuses a command it cannot start the way a shell does', async () => {
    await withRepo(async ({ root }) => {
      const { code, output } = await capture(async () =>
        secretsRun(root, 'registry', ['definitely-not-a-real-smoo-binary-xyz']),
      );

      expect(code).toBe(127);
      expect(output).toContain('definitely-not-a-real-smoo-binary-xyz');
    });
  });

  it('states a declared group that overrides the derivation', async () => {
    await withRepo(
      async ({ root }) => {
        const { output } = await capture(async () => secretsRun(root, undefined, []));

        // The reference in .npmrc derives `registry`; the entry says `shell`.
        // A silent override is how the next reader loses an hour.
        expect(output).toContain('REGISTRY_TOKEN declares group `shell`');
        expect(output).toContain('overriding the `registry`');
      },
      { group: { REGISTRY_TOKEN: 'shell' } },
    );
  });

  it('resolves an overridden group instead of the derived one', async () => {
    await withRepo(
      async ({ root, record, invocations }) => {
        expect(await secretsRun(root, 'shell', recorder(root))).toBe(0);

        expect(record().env).toEqual({ REGISTRY_TOKEN: 'registry-value', SHELL_TOKEN: 'shell-value' });
        expect(invocations()).toBe(2);
      },
      { group: { REGISTRY_TOKEN: 'shell' } },
    );
  });

  it('in CI, refuses with injected-secret guidance and runs no provider command', async () => {
    await withRepo(async ({ root, invocations }) => {
      process.env.CI = 'true';

      const { code, output } = await capture(async () => secretsRun(root, 'registry', recorder(root)));

      expect(code).toBe(1);
      expect(output).toContain('REGISTRY_TOKEN');
      expect(output).toContain('inject');
      // The wrapper is not a way around an injected-secret store, so it is
      // not offered as one.
      expect(output).not.toContain('smoo secrets run');
      expect(invocations()).toBe(0);
    });
  });

  it('in a cowshed workspace, refuses a registry credential with gateway-enrollment guidance', async () => {
    await withRepo(async ({ root, invocations }) => {
      process.env.COWSHED_WORKSPACE_TOKEN = 'gateway-session';

      const { code, output } = await capture(async () => secretsRun(root, 'registry', recorder(root)));

      expect(code).toBe(1);
      expect(output).toContain('REGISTRY_TOKEN');
      expect(output).toContain('cowshed gateway');
      expect(invocations()).toBe(0);
      // The gateway rule is about registry credentials. A group the install
      // does not read still resolves through its provider in the same
      // workspace — `shell` would not, because it carries the same
      // `registry` dependency shell entry does.
      expect(await secretsRun(root, 'nx-cache', recorder(root))).toBe(0);
      expect(invocations()).toBe(1);
    });
  });
});

/**
 * Commander is the part of this that can silently misbehave: an option after
 * the group belongs to the child, and a parser that ate `-d` would only show
 * up in someone else's afternoon. This drives the real program, not
 * `secretsRun`, because the parser is what is under test.
 */
describe('argv passthrough through the real command line', () => {
  it('hands every argument after the group to the child verbatim, flags included', async () => {
    await withRepo(async ({ root, record }) => {
      const cwd = process.cwd();
      const exitCode = process.exitCode;
      process.chdir(root);
      try {
        await runCli([
          'secrets',
          'run',
          'registry',
          process.execPath,
          join(root, 'recorder.mjs'),
          'add',
          '-d',
          '@acme/x',
        ]);
        expect(process.exitCode).toBe(0);
      } finally {
        process.chdir(cwd);
        process.exitCode = exitCode;
      }

      expect(record().argv).toEqual(['add', '-d', '@acme/x']);
      expect(record().env).toEqual({ REGISTRY_TOKEN: 'registry-value' });
    });
  });
});
