import { describe, expect, it } from 'bun:test';
import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { repositorySecretMapping } from '../lib/secret-names.js';
import { workflowEnvironments } from './index.js';
import { environmentsMissing, reconcileSecrets, type SecretRow, unsatisfiedSecrets, unwiredSecrets } from './status.js';

const sources = {
  workerSecrets: {
    'targets/billing': ['STRIPE_PUBLISHABLE_KEY', 'STRIPE_SECRET_KEY'],
    'targets/mail': ['MAIL_CAPTURE_CONTROL_TOKEN'],
  },
  workflowSecrets: ['MAIL_CAPTURE_CONTROL_TOKEN', 'STRIPE_PUBLISHABLE_KEY', 'STRIPE_SECRET_KEY'],
  localCommands: ['NPM_READ_TOKEN'],
  secretNames: {
    STRIPE_PUBLISHABLE_KEY: 'STRIPE_PUBLISHABLE_KEY',
    STRIPE_SECRET_KEY: 'STRIPE_SECRET_KEY',
    MAIL_CAPTURE_CONTROL_TOKEN: 'MAIL_CAPTURE_CONTROL_TOKEN',
    NPM_READ_TOKEN: 'NPM_READ_TOKEN',
  },
  repositorySecrets: ['MAIL_CAPTURE_CONTROL_TOKEN', 'NPM_READ_TOKEN'],
};

describe('secret reconciliation', () => {
  it('names the secret a workflow passes and the repository does not hold', () => {
    // A preview stage refuses with `publishable_key_mismatch` when `ci.yml`
    // passes `secrets.STRIPE_PUBLISHABLE_KEY` and the repository holds no such
    // secret: the empty value reaches the payment library's validator, which
    // talks about the key instead of the missing declaration.
    const unsatisfied = unsatisfiedSecrets(reconcileSecrets(sources)).map((row) => row.name);

    expect(unsatisfied).toEqual(['STRIPE_PUBLISHABLE_KEY', 'STRIPE_SECRET_KEY']);
  });

  it('keeps "declared but no workflow passes it" separate from "no value"', () => {
    // The remedies differ: one is `smoo secrets set`, the other is a
    // smoo.github.deploySecrets entry. Reporting them as one list sends an
    // operator to the wrong fix.
    const rows = reconcileSecrets({
      ...sources,
      workflowSecrets: ['MAIL_CAPTURE_CONTROL_TOKEN'],
    });

    expect(unwiredSecrets(rows).map((row) => row.name)).toEqual(['STRIPE_PUBLISHABLE_KEY', 'STRIPE_SECRET_KEY']);
    expect(unsatisfiedSecrets(rows)).toEqual([]);
  });

  it('reports a repository secret no source declares, so a stale one is visible', () => {
    const rows = reconcileSecrets({ ...sources, repositorySecrets: [...sources.repositorySecrets, 'RETIRED_TOKEN'] });
    const retired = rows.find((row) => row.name === 'RETIRED_TOKEN');

    expect(retired).toEqual({
      name: 'RETIRED_TOKEN',
      repositorySecret: 'RETIRED_TOKEN',
      declaredByWorkers: [],
      suppliedByWorkflow: false,
      fetchableLocally: false,
      onRepository: true,
      heldByEnvironment: [],
    });
  });

  it('attributes a shared secret to every worker that declares it', () => {
    const rows = reconcileSecrets({
      ...sources,
      workerSecrets: { 'targets/a': ['SHARED'], 'targets/b': ['SHARED'] },
    });

    expect(rows.find((row) => row.name === 'SHARED')?.declaredByWorkers).toEqual(['targets/a', 'targets/b']);
  });

  it('reads a reserved env name from its owner-prefixed secret, with no declaration', () => {
    // GitHub rejects `gh secret set GITHUB_*`, so the value cannot live under
    // its own name. Operators write the owner-prefixed name by hand in the
    // workflow; the convention derives exactly that from the repository owner.
    const rows = reconcileSecrets({
      workerSecrets: { 'targets/backend': ['GITHUB_CLIENT_SECRET'] },
      workflowSecrets: ['GITHUB_CLIENT_SECRET'],
      secretNames: repositorySecretMapping(['GITHUB_CLIENT_SECRET'], 'acme'),
      localCommands: [],
      repositorySecrets: ['ACME_GITHUB_CLIENT_SECRET'],
    });

    expect(rows).toEqual([
      {
        name: 'GITHUB_CLIENT_SECRET',
        repositorySecret: 'ACME_GITHUB_CLIENT_SECRET',
        declaredByWorkers: ['targets/backend'],
        suppliedByWorkflow: true,
        fetchableLocally: false,
        onRepository: true,
        heldByEnvironment: [],
      },
    ]);
    expect(unsatisfiedSecrets(rows)).toEqual([]);
  });
});

describe('environment-scoped values', () => {
  // Two jobs bind an environment each: what `secrets.X` resolves to in them is
  // the environment's value, and only then the repository's.
  const bound = ['preview', 'production'];
  const rowFor = (rows: readonly SecretRow[], name: string): SecretRow => {
    const row = rows.find((candidate) => candidate.name === name);
    if (!row) throw new Error(`no row for ${name}`);
    return row;
  };

  it('does not call a name missing when every environment a job binds holds it', () => {
    // The false refusal this fixes: the values are set, in the scope the job
    // actually reads, and reading repository scope alone reported them absent.
    const rows = reconcileSecrets({
      ...sources,
      environmentSecrets: {
        preview: ['STRIPE_PUBLISHABLE_KEY', 'STRIPE_SECRET_KEY'],
        production: ['STRIPE_PUBLISHABLE_KEY', 'STRIPE_SECRET_KEY'],
      },
    });

    expect(unsatisfiedSecrets(rows, bound)).toEqual([]);
    expect(rowFor(rows, 'STRIPE_SECRET_KEY').heldByEnvironment).toEqual(['preview', 'production']);
    expect(rowFor(rows, 'STRIPE_SECRET_KEY').onRepository).toBe(false);
  });

  it('lets a repository value satisfy every bound environment, because a job falls back to it', () => {
    const rows = reconcileSecrets({
      ...sources,
      repositorySecrets: [...sources.repositorySecrets, 'STRIPE_PUBLISHABLE_KEY', 'STRIPE_SECRET_KEY'],
    });

    expect(unsatisfiedSecrets(rows, bound)).toEqual([]);
    expect(environmentsMissing(rowFor(rows, 'STRIPE_SECRET_KEY'), bound)).toEqual([]);
  });

  it('names exactly the scopes that lack a value, so each gets its own command', () => {
    const rows = reconcileSecrets({ ...sources, environmentSecrets: { preview: ['STRIPE_SECRET_KEY'] } });

    expect(unsatisfiedSecrets(rows, bound).map((row) => row.name)).toEqual([
      'STRIPE_PUBLISHABLE_KEY',
      'STRIPE_SECRET_KEY',
    ]);
    expect(environmentsMissing(rowFor(rows, 'STRIPE_SECRET_KEY'), bound)).toEqual(['production']);
    expect(environmentsMissing(rowFor(rows, 'STRIPE_PUBLISHABLE_KEY'), bound)).toEqual(['preview', 'production']);
  });

  it('shows a value only an environment holds, even when nothing declares the name', () => {
    const rows = reconcileSecrets({ ...sources, environmentSecrets: { production: ['RETIRED_TOKEN'] } });

    expect(rowFor(rows, 'RETIRED_TOKEN').heldByEnvironment).toEqual(['production']);
    expect(rowFor(rows, 'RETIRED_TOKEN').onRepository).toBe(false);
  });
});

describe('workflow environment bindings', () => {
  it('reads the scopes a workflow binds, and nothing that only looks like one', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoo-secrets-environments-'));
    try {
      await mkdir(join(root, '.github', 'workflows'), { recursive: true });
      await writeFile(
        join(root, '.github', 'workflows', 'ci.yml'),
        `name: CI
on: push
jobs:
  main:
    name: Validate
    runs-on: ubuntu-latest
    environment: staging
    steps:
      - name: Build
        run: echo build
  review:
    runs-on: ubuntu-latest
    environment: "review env"
    steps:
      - name: Deploy
        run: echo deploy
  preview:
    runs-on: ubuntu-latest
    environment: \${{ github.event.inputs.stage }}
    steps:
      - name: Deploy
        run: echo deploy
  production:
    runs-on: ubuntu-latest
    environment:
      name: production
      url: \${{ steps.deploy.outputs.url }}
    steps:
      - name: Deploy
        run: echo deploy
`,
      );

      // The workflow's own name, each job's, and each step's are not scopes: a
      // secret cannot be set in "Validate", and offering it would be a command
      // that fails. The computed one names no fixed scope either.
      expect(workflowEnvironments(root)).toEqual(['production', 'review env', 'staging']);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });
});
