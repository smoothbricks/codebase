import { afterAll, beforeAll, describe, expect, it } from 'bun:test';
import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { secretsMissingInEnvironment, secretsStatus } from './commands.js';
import { parseSecretsStatusDocument, reconcileSecrets } from './status.js';

const sources = {
  workerSecrets: { 'targets/billing': ['PAYMENTS_PUBLISHABLE_KEY', 'PAYMENTS_SECRET_KEY'] },
  workflowSecrets: ['PAYMENTS_PUBLISHABLE_KEY', 'PAYMENTS_SECRET_KEY'],
  localSecrets: [],
  secretNames: {
    PAYMENTS_PUBLISHABLE_KEY: 'PAYMENTS_PUBLISHABLE_KEY',
    PAYMENTS_SECRET_KEY: 'PAYMENTS_SECRET_KEY',
  },
  repositorySecrets: ['PAYMENTS_PUBLISHABLE_KEY', 'PAYMENTS_SECRET_KEY', 'RETIRED_TOKEN'],
  environmentSecrets: { production: ['PAYMENTS_PUBLISHABLE_KEY'] },
};

describe('what `smoo secrets set --env` walks', () => {
  it('offers a name the repository holds and the environment does not, and no undeclared one', () => {
    // Overriding one stage's credentials is the reason to write at environment
    // scope: skipping names the repository already holds would leave a live
    // value unsettable. RETIRED_TOKEN is absent from the environment too, but
    // nothing declares it - prompting for it would ask an operator to copy a
    // stale value forward.
    const walked = secretsMissingInEnvironment(reconcileSecrets(sources), 'production');

    expect(walked.map((row) => row.name)).toEqual(['PAYMENTS_SECRET_KEY']);
  });
});

/**
 * A whole `smoo secrets status` run against a checkout on disk and a `gh` that
 * answers from a script rather than the network. Anything less would test the
 * projection twice and the command not at all: whether `--json` writes one
 * document and nothing else, and whether it still refuses, are properties of
 * this function's own output.
 */
describe('a real `smoo secrets status` run', () => {
  let root = '';
  let path = '';

  beforeAll(async () => {
    root = await mkdtemp(join(tmpdir(), 'smoo-secrets-status-'));
    await writeFile(
      join(root, 'package.json'),
      `${JSON.stringify(
        {
          name: 'acme-app',
          private: true,
          workspaces: ['targets/*'],
          repository: { type: 'git', url: 'https://github.com/acme/app.git' },
          smoo: {
            github: {
              deploySecrets: {
                MAIL_CAPTURE_CONTROL_TOKEN: 'MAIL_CAPTURE_CONTROL_TOKEN',
                STRIPE_PUBLISHABLE_KEY: 'STRIPE_PUBLISHABLE_KEY',
                STRIPE_SECRET_KEY: 'STRIPE_SECRET_KEY',
              },
            },
            secrets: { NPM_READ_TOKEN: { command: ['printf', 'unused'] } },
          },
        },
        null,
        2,
      )}\n`,
    );
    await mkdir(join(root, '.github', 'workflows'), { recursive: true });
    await writeFile(
      join(root, '.github', 'workflows', 'ci.yml'),
      `name: CI
on: push
jobs:
  validate:
    runs-on: ubuntu-latest
    environment: staging
    steps:
      - run: echo build
  release:
    runs-on: ubuntu-latest
    environment: production
    steps:
      - run: echo deploy
`,
    );
    await mkdir(join(root, 'targets', 'billing'), { recursive: true });
    await writeFile(
      join(root, 'targets', 'billing', 'package.json'),
      `${JSON.stringify(
        {
          name: '@acme/billing',
          version: '0.0.0',
          private: true,
          smoo: { wrangler: { secretStages: { STRIPE_SECRET_KEY: ['production'], E2E_CONTROL_TOKEN: [] } } },
        },
        null,
        2,
      )}\n`,
    );
    await writeFile(
      join(root, 'targets', 'billing', '.dev.vars.example'),
      'STRIPE_SECRET_KEY=""\nSTRIPE_PUBLISHABLE_KEY=""\nE2E_CONTROL_TOKEN=""\n',
    );
    // `gh` from a script: the repository holds two secrets, staging holds one,
    // and production refuses - the three answers the command has to tell apart.
    const bin = join(root, 'bin');
    await mkdir(bin, { recursive: true });
    await writeFile(
      join(bin, 'gh'),
      `#!/bin/sh
case "$*" in
  *"--env production"*) echo "HTTP 403: Resource not accessible" >&2; exit 1 ;;
  *"--env staging"*) echo '[{"name":"MAIL_CAPTURE_CONTROL_TOKEN"}]' ;;
  *) echo '[{"name":"NPM_READ_TOKEN"},{"name":"RETIRED_TOKEN"}]' ;;
esac
`,
      { mode: 0o755 },
    );
    path = process.env.PATH ?? '';
    process.env.PATH = `${bin}:${path}`;
  });

  afterAll(async () => {
    process.env.PATH = path;
    await rm(root, { recursive: true, force: true });
  });

  async function run(options: { repo?: string; env?: string; json?: boolean }): Promise<{
    code: number;
    out: string[];
    errors: string[];
  }> {
    const out: string[] = [];
    const errors: string[] = [];
    const [log, error] = [console.log, console.error];
    console.log = (...args: unknown[]) => out.push(args.map(String).join(' '));
    console.error = (...args: unknown[]) => errors.push(args.map(String).join(' '));
    try {
      return { code: await secretsStatus(root, options), out, errors };
    } finally {
      [console.log, console.error] = [log, error];
    }
  }

  it('writes one document to stdout and nothing else, and still refuses', async () => {
    const { code, out } = await run({ repo: 'acme/app', json: true });

    expect(out).toHaveLength(1);
    const validated = parseSecretsStatusDocument(out[0] ?? '');
    expect(validated.success).toBe(true);
    if (!validated.success) throw new Error('`--json` wrote a document its own validator rejects');
    expect(validated.data.repository).toEqual({ repo: 'acme/app', source: 'requested', secretCount: 2 });
    expect(validated.data.environments).toEqual([
      {
        name: 'production',
        bound: true,
        readable: false,
        reason:
          'gh secret list --json name --repo acme/app --env production exited 1: HTTP 403: Resource not accessible',
      },
      { name: 'staging', bound: true, readable: true, secretCount: 1 },
    ]);
    expect(validated.data.unsatisfied).toEqual([
      { name: 'STRIPE_PUBLISHABLE_KEY', repositorySecret: 'STRIPE_PUBLISHABLE_KEY', missingIn: ['staging'] },
      { name: 'STRIPE_SECRET_KEY', repositorySecret: 'STRIPE_SECRET_KEY', missingIn: ['staging'] },
    ]);
    // A machine-readable status nobody can gate on would be worse than none.
    expect(code).toBe(1);
  });

  it('carries the stages `smoo.wrangler.secretStages` declares, per name', async () => {
    const { out } = await run({ repo: 'acme/app', json: true });
    const validated = parseSecretsStatusDocument(out[0] ?? '');
    if (!validated.success) throw new Error('`--json` wrote a document its own validator rejects');
    const stages = Object.fromEntries(validated.data.secrets.map((row) => [row.name, row.requiredByStages]));

    expect(stages).toEqual({
      // Scoped to production by the wrangler project.
      STRIPE_SECRET_KEY: ['production'],
      // Declared and absent from the map, so every stage requires it.
      STRIPE_PUBLISHABLE_KEY: ['preview', 'production', 'staging'],
      // Declared and mapped to []: a local-only value no stage requires.
      E2E_CONTROL_TOKEN: [],
      // Names no wrangler project declares have no stages to require them.
      MAIL_CAPTURE_CONTROL_TOKEN: [],
      NPM_READ_TOKEN: [],
      RETIRED_TOKEN: [],
    });
  });

  it('prints the same facts as a table without the flag', async () => {
    const { code, out, errors } = await run({ repo: 'acme/app' });

    expect(out[0]).toBe('repository acme/app (requested), holding 2 secrets');
    expect(out[1]).toBe('environment staging, holding 1 secrets');
    expect(out[2]).toBe(
      'environment production could not be read, so nothing below claims what it holds: ' +
        'gh secret list --json name --repo acme/app --env production exited 1: HTTP 403: Resource not accessible',
    );
    expect(out.find((line) => line.startsWith('ABSENT') && line.includes('STRIPE_SECRET_KEY'))).toMatch(
      /^ABSENT +STRIPE_SECRET_KEY +workflow +targets\/billing$/,
    );
    expect(errors).toContain(
      'missing: STRIPE_SECRET_KEY — a workflow passes secrets.STRIPE_SECRET_KEY and no value exists in staging.',
    );
    expect(errors).toContain('  smoo secrets set STRIPE_SECRET_KEY -R acme/app --env staging');
    expect(code).toBe(1);
  });
});
