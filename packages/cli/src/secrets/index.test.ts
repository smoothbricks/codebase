import { describe, expect, it } from 'bun:test';
import { repositorySecretMapping } from '../lib/secret-names.js';
import { reconcileSecrets, unsatisfiedSecrets, unwiredSecrets } from './index.js';

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
      },
    ]);
    expect(unsatisfiedSecrets(rows)).toEqual([]);
  });
});
