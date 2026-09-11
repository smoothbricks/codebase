import { describe, expect, it } from 'bun:test';
import {
  parseSecretsStatusDocument,
  projectSecretsStatus,
  reconcileSecrets,
  type SecretsStatusFacts,
  stringifySecretsStatusDocument,
} from './status.js';

const rows = reconcileSecrets({
  workerSecrets: { 'targets/billing': ['STRIPE_SECRET_KEY', 'STRIPE_PUBLISHABLE_KEY', 'E2E_CONTROL_TOKEN'] },
  workflowSecrets: ['STRIPE_SECRET_KEY', 'STRIPE_PUBLISHABLE_KEY'],
  secretNames: {
    STRIPE_SECRET_KEY: 'STRIPE_SECRET_KEY',
    STRIPE_PUBLISHABLE_KEY: 'STRIPE_PUBLISHABLE_KEY',
    E2E_CONTROL_TOKEN: 'E2E_CONTROL_TOKEN',
  },
  localCommands: [],
  repositorySecrets: ['RETIRED_TOKEN'],
  environmentSecrets: { staging: ['STRIPE_SECRET_KEY'] },
});

const facts: SecretsStatusFacts = {
  repository: { repo: 'acme/app', source: 'upstream of the current branch (private)', secretCount: 1 },
  environments: [
    { name: 'staging', bound: true, readable: true, secretCount: 1 },
    { name: 'production', bound: true, readable: false, reason: 'gh secret list --env production exited 1: HTTP 403' },
  ],
  rows,
  stageScopes: {
    'targets/billing': { STRIPE_SECRET_KEY: ['production'], E2E_CONTROL_TOKEN: [] },
  },
};

/** The stages one name ends up requiring, which is the whole point of the projection. */
function stagesFor(name: string): string[] {
  const row = projectSecretsStatus(facts).secrets.find((candidate) => candidate.name === name);
  if (!row) throw new Error(`no row for ${name}`);
  return row.requiredByStages;
}

describe('which stages a secret is required by', () => {
  it('takes the stages a wrangler project scoped it to', () => {
    expect(stagesFor('STRIPE_SECRET_KEY')).toEqual(['production']);
  });

  it('requires a declared name absent from secretStages everywhere, because the map is an exception list', () => {
    // The dangerous direction: reading "unscoped" as "needed nowhere" would
    // let a production deploy sail past a secret it cannot run without.
    expect(stagesFor('STRIPE_PUBLISHABLE_KEY')).toEqual(['preview', 'production', 'staging']);
  });

  it('requires a name mapped to [] by no stage at all', () => {
    // A local-development-only value: declared, deliberately scoped to
    // nothing, and no stage may refuse to deploy over it.
    expect(stagesFor('E2E_CONTROL_TOKEN')).toEqual([]);
  });

  it('scopes nothing to a name no wrangler project declares', () => {
    // Empty here means "not a Worker secret at all", which is what
    // `declaredByWorkers` tells apart from "declared and scoped to nothing".
    const row = projectSecretsStatus(facts).secrets.find((candidate) => candidate.name === 'RETIRED_TOKEN');

    expect(row?.requiredByStages).toEqual([]);
    expect(row?.declaredByWorkers).toEqual([]);
  });

  it('unions the scopes of every project declaring the same name', () => {
    // One name, two Workers, different scopes: a value must exist wherever
    // either of them deploys, so the requirement is the union and not
    // whichever declaration was read last.
    const shared = projectSecretsStatus({
      ...facts,
      stageScopes: {
        'targets/billing': { STRIPE_SECRET_KEY: ['production'] },
        'targets/mail': { STRIPE_SECRET_KEY: ['staging'] },
      },
      rows: reconcileSecrets({
        workerSecrets: { 'targets/billing': ['STRIPE_SECRET_KEY'], 'targets/mail': ['STRIPE_SECRET_KEY'] },
        workflowSecrets: [],
        secretNames: { STRIPE_SECRET_KEY: 'STRIPE_SECRET_KEY' },
        localCommands: [],
        repositorySecrets: [],
      }),
    });

    expect(shared.secrets[0]?.requiredByStages).toEqual(['production', 'staging']);
  });
});

describe('what the document says about satisfaction', () => {
  it('counts only a bound environment that could be read as a scope a job reads', () => {
    // production is bound and unreadable. Counting it as empty would refuse
    // over STRIPE_SECRET_KEY, whose value may well be set there; counting it
    // as holding a value would hide a real gap. It counts as neither, so
    // staging alone decides - and staging does hold STRIPE_SECRET_KEY.
    const document = projectSecretsStatus(facts);

    expect(document.unsatisfied).toEqual([
      { name: 'STRIPE_PUBLISHABLE_KEY', repositorySecret: 'STRIPE_PUBLISHABLE_KEY', missingIn: ['staging'] },
    ]);
  });

  it('leaves missingIn empty when the repository itself is the scope to set the value in', () => {
    // No environment bound: the repository is the only scope a job reads, and
    // naming environments here would print commands that write nowhere useful.
    const noEnvironments = projectSecretsStatus({ ...facts, environments: [] });

    expect(noEnvironments.unsatisfied.map((row) => [row.name, row.missingIn])).toEqual([
      ['STRIPE_PUBLISHABLE_KEY', []],
      ['STRIPE_SECRET_KEY', []],
    ]);
  });
});

describe('the document as a wire format', () => {
  it('round-trips through its own validator, stages and all', () => {
    // What the CLI writes is exactly what `parseSecretsStatusDocument`
    // accepts, so a repository's own operations tool validates rather than
    // trusts a shape it scraped.
    const validated = parseSecretsStatusDocument(stringifySecretsStatusDocument(projectSecretsStatus(facts)));

    expect(validated.success).toBe(true);
    if (!validated.success) throw new Error('the document its own emitter produced did not validate');
    expect(validated.data.version).toBe(1);
    expect(validated.data.repository).toEqual({
      repo: 'acme/app',
      source: 'upstream of the current branch (private)',
      secretCount: 1,
    });
    expect(validated.data.environments).toEqual([
      { name: 'staging', bound: true, readable: true, secretCount: 1 },
      {
        name: 'production',
        bound: true,
        readable: false,
        reason: 'gh secret list --env production exited 1: HTTP 403',
      },
    ]);
    expect(validated.data.secrets.find((row) => row.name === 'STRIPE_SECRET_KEY')).toEqual({
      name: 'STRIPE_SECRET_KEY',
      repositorySecret: 'STRIPE_SECRET_KEY',
      declaredByWorkers: ['targets/billing'],
      suppliedByWorkflow: true,
      fetchableLocally: false,
      onRepository: false,
      heldByEnvironment: ['staging'],
      requiredByStages: ['production'],
    });
  });

  it('refuses to serialise a document missing a field, naming the field', () => {
    // The emitter's boundary earns its place here: a projection that dropped a
    // field would otherwise reach a consumer as an unexplained validation
    // failure in someone else's repository, with nothing pointing back here.
    const dropped = JSON.parse(
      JSON.stringify(projectSecretsStatus(facts), (key, value: unknown) =>
        key === 'requiredByStages' ? undefined : value,
      ),
    );

    expect(() => stringifySecretsStatusDocument(dropped)).toThrow(/requiredByStages/);
  });
});
