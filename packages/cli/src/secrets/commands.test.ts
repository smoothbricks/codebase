import { describe, expect, it } from 'bun:test';
import { secretsMissingInEnvironment } from './commands.js';
import { reconcileSecrets } from './index.js';

const sources = {
  workerSecrets: { 'targets/billing': ['PAYMENTS_PUBLISHABLE_KEY', 'PAYMENTS_SECRET_KEY'] },
  workflowSecrets: ['PAYMENTS_PUBLISHABLE_KEY', 'PAYMENTS_SECRET_KEY'],
  localCommands: [],
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
