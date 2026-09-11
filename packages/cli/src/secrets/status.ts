/**
 * The value model behind `smoo secrets`: one row per secret name, reconciled
 * from every source that has an opinion about it, plus the machine-readable
 * document `smoo secrets status --json` emits.
 *
 * Everything here is pure and depends on nothing but typia, which is what lets
 * `@smoothbricks/cli/secrets` be imported for the types alone — a consumer
 * driving a UI or a repository's own operations CLI gets the document type and
 * a validator without pulling in `gh`, the filesystem readers, or commander.
 * The readers that produce these facts live in ./index.ts; the operator
 * commands that print them live in ./commands.ts.
 */

import typia from 'typia';
import type { SecretStageMap, SecretStageScope } from '../wrangler/stage-secrets.js';

export type { SecretStageScope };

/** Where an env name is described, which repository secret carries it, and whether that exists. */
export interface SecretRow {
  name: string;
  /** The repository secret this env name reads from, by convention or declaration. */
  repositorySecret: string;
  /** Wrangler projects whose `.dev.vars.example` declares it. */
  declaredByWorkers: string[];
  /** True when a managed workflow renders `secrets.<name>` into a job. */
  suppliedByWorkflow: boolean;
  /** True when `smoo.secrets` can fetch it for a developer shell; equivalently, `localGroup` is set. */
  fetchableLocally: boolean;
  /**
   * How `smoo.secrets` resolves it locally: the group that resolves it, and
   * the group this repository's own declarations derive. They differ exactly
   * when the entry overrides the derivation, which is the one thing a reader
   * must never have to infer. Absent when nothing fetches the name locally.
   */
  localGroup?: { resolves: string; derived: string };
  /** True when the repository holds a secret of this name. */
  onRepository: boolean;
  /**
   * GitHub Environments holding their own value for this name. A job bound to
   * an environment reads that value in preference to the repository's, which
   * is how one name carries test credentials on a preview stage and live ones
   * in production.
   */
  heldByEnvironment: string[];
}

/**
 * One `smoo.secrets` entry as the reconciliation needs it: the name, the
 * group that resolves it, and the group this repository's declarations
 * derive. ../secrets/resolver.ts produces these from the same file shell
 * entry routes with.
 */
export interface LocalSecret {
  name: string;
  group: string;
  derivedGroup: string;
}

export interface SecretSources {
  /** Worker label -> env names it declares. */
  workerSecrets: Record<string, readonly string[]>;
  /** Env names a managed workflow passes into a job. */
  workflowSecrets: readonly string[];
  /** Env name -> repository secret, by convention with declared exceptions. */
  secretNames: Readonly<Record<string, string>>;
  /** Every `smoo.secrets` entry with the group that resolves it. */
  localSecrets: readonly LocalSecret[];
  /** Repository secret names GitHub currently holds. */
  repositorySecrets: readonly string[];
  /** Environment name -> secret names that environment holds. */
  environmentSecrets?: Readonly<Record<string, readonly string[]>>;
}

/**
 * One row per name known to any source, sorted, so a reader sees the whole
 * picture rather than one source's view of it.
 */
export function reconcileSecrets(sources: SecretSources): SecretRow[] {
  const declaredByAnyWorker: string[] = Object.values(sources.workerSecrets).flatMap((names) => [...names]);
  const localSecrets = new Map(sources.localSecrets.map((secret) => [secret.name, secret]));
  const names = new Set<string>([...declaredByAnyWorker, ...sources.workflowSecrets, ...localSecrets.keys()]);
  // A repository secret that already carries a known env name is that name's
  // row, not a row of its own: listing ACME_GITHUB_CLIENT_SECRET beside
  // GITHUB_CLIENT_SECRET would report one value as two secrets, one of them
  // permanently "declared by nothing".
  const carriesKnownEnvName = new Set(
    names.size > 0 ? [...names].map((name) => sources.secretNames[name] ?? name) : [],
  );
  // A value only an environment holds is still a value: leaving it out of the
  // rows is how an operator ends up hunting for a secret that is already set.
  const addUndeclared = (secret: string): void => {
    if (!carriesKnownEnvName.has(secret)) names.add(secret);
  };
  for (const secret of sources.repositorySecrets) addUndeclared(secret);
  for (const held of Object.values(sources.environmentSecrets ?? {})) {
    for (const secret of held) addUndeclared(secret);
  }
  return [...names]
    .sort((left, right) => left.localeCompare(right))
    .map((name) => {
      const local = localSecrets.get(name);
      return {
        name,
        repositorySecret: sources.secretNames[name] ?? name,
        declaredByWorkers: Object.entries(sources.workerSecrets)
          .filter(([, declared]) => declared.includes(name))
          .map(([label]) => label)
          .sort((left, right) => left.localeCompare(right)),
        suppliedByWorkflow: sources.workflowSecrets.includes(name),
        fetchableLocally: local !== undefined,
        ...(local === undefined ? {} : { localGroup: { resolves: local.group, derived: local.derivedGroup } }),
        onRepository: sources.repositorySecrets.includes(sources.secretNames[name] ?? name),
        heldByEnvironment: Object.entries(sources.environmentSecrets ?? {})
          .filter(([, held]) => held.includes(sources.secretNames[name] ?? name))
          .map(([environment]) => environment)
          .sort((left, right) => left.localeCompare(right)),
      };
    });
}

/**
 * The rows a CI deploy cannot satisfy: a Worker declares the name, a workflow
 * promises to pass it, and the repository has no value to pass. Reported
 * separately from "declared but not wired into any workflow", because the
 * remedies differ — set a secret, versus declare it in `smoo.github`.
 */
export function unsatisfiedSecrets(rows: readonly SecretRow[], boundEnvironments: readonly string[] = []): SecretRow[] {
  return rows.filter((row) => {
    if (!row.suppliedByWorkflow) return false;
    if (row.onRepository) return false;
    // With no environment bound, the repository is the only scope a job reads.
    // With environments bound, each one can carry the value instead - so the
    // name is satisfied only when every bound environment holds it.
    if (boundEnvironments.length === 0) return true;
    return !boundEnvironments.every((environment) => row.heldByEnvironment.includes(environment));
  });
}

/** Bound environments that lack their own value and cannot fall back to the repository. */
export function environmentsMissing(row: SecretRow, boundEnvironments: readonly string[]): string[] {
  if (row.onRepository) return [];
  return boundEnvironments.filter((environment) => !row.heldByEnvironment.includes(environment));
}

/** Worker-declared names no managed workflow passes: a CI deploy will run without them. */
export function unwiredSecrets(rows: readonly SecretRow[]): SecretRow[] {
  return rows.filter((row) => row.declaredByWorkers.length > 0 && !row.suppliedByWorkflow);
}

/**
 * Every stage a scope can name, in the order the document lists them. A
 * declared secret absent from `smoo.wrangler.secretStages` belongs to all of
 * them, which is the rule this constant spells out exactly once.
 */
const EVERY_STAGE: readonly SecretStageScope[] = ['preview', 'production', 'staging'];

/** The repository the status was read from, and how that repository was chosen. */
export interface SecretsStatusRepository {
  /** `owner/name`, the form `gh --repo` takes. */
  repo: string;
  /** Why this repository — the checkout fact that decided it, verbatim from the resolver. */
  source: string;
  /** How many secrets it holds at repository scope. */
  secretCount: number;
}

/**
 * One GitHub Environment the status looked at. Discriminated on `readable`
 * because an environment `gh` refused to list is not an empty one: reporting
 * it as empty would call values missing that may well be set, and the reason
 * is the only thing that lets an operator tell the two apart.
 */
export type SecretsStatusEnvironment =
  | {
      name: string;
      /** True when a managed workflow binds a job to it, so a job reads it before the repository. */
      bound: boolean;
      readable: true;
      secretCount: number;
    }
  | {
      name: string;
      bound: boolean;
      readable: false;
      /** What `gh` said, so nothing below claims what this environment holds. */
      reason: string;
    };

/** A reconciled row, plus which deployment stages require the name to have a value. */
export interface SecretsStatusRow extends SecretRow {
  /**
   * Stages that refuse to deploy without a value, from `smoo.wrangler.secretStages`
   * unioned over every wrangler project declaring the name: absent from the map
   * means every stage, mapped to `[]` means none. A name no wrangler project
   * declares is `[]` too — `declaredByWorkers` tells those two apart.
   */
  requiredByStages: SecretStageScope[];
}

/** A name a workflow passes that no scope the job reads can supply: this is what fails a deploy. */
export interface UnsatisfiedSecret {
  name: string;
  repositorySecret: string;
  /**
   * Bound environments holding no value for it. Empty means no environment is
   * bound and the repository itself is the scope to set it in.
   */
  missingIn: string[];
}

/**
 * Everything `smoo secrets status` learned, in one document.
 *
 * `unsatisfied` is the exit code's reason and not a convenience: the rule that
 * decides whether a value exists in a scope a bound job reads is smoo's, and a
 * consumer re-deriving it from `secrets` would be maintaining a second copy of
 * it.
 */
export interface SecretsStatusDocument {
  /** Document shape, so a consumer refuses a document it does not understand instead of misreading it. */
  version: 1;
  repository: SecretsStatusRepository;
  /** Every environment looked at: those the workflows bind, then any asked for with `--env`. */
  environments: SecretsStatusEnvironment[];
  /** One row per name any source knows, sorted by name. */
  secrets: SecretsStatusRow[];
  /** Empty exactly when the command exits 0. */
  unsatisfied: UnsatisfiedSecret[];
}

/** The facts a document is projected from, all gathered before anything is printed. */
export interface SecretsStatusFacts {
  repository: SecretsStatusRepository;
  environments: readonly SecretsStatusEnvironment[];
  rows: readonly SecretRow[];
  /** Worker label -> its `smoo.wrangler.secretStages` declaration; a worker with no block is absent. */
  stageScopes: Readonly<Record<string, SecretStageMap>>;
}

/**
 * The stages that require a value for one name: the union over every worker
 * declaring it, since a name two projects share must exist wherever either of
 * them deploys.
 */
function requiredByStages(
  name: string,
  workers: readonly string[],
  stageScopes: Readonly<Record<string, SecretStageMap>>,
): SecretStageScope[] {
  const required = new Set<SecretStageScope>();
  for (const worker of workers) {
    const scoped = stageScopes[worker]?.[name];
    // No scope declared for a name the worker declares is "every stage": the
    // map is an exception list, not a registry.
    for (const stage of scoped ?? EVERY_STAGE) required.add(stage);
  }
  return EVERY_STAGE.filter((stage) => required.has(stage));
}

/** The document, from facts. Pure: every read happened before this was called. */
export function projectSecretsStatus(facts: SecretsStatusFacts): SecretsStatusDocument {
  const secrets: SecretsStatusRow[] = facts.rows.map((row) => ({
    ...row,
    requiredByStages: requiredByStages(row.name, row.declaredByWorkers, facts.stageScopes),
  }));
  // Only an environment a workflow binds *and* `gh` could list is a scope a job
  // is known to read: an unreadable one is unknown, not empty.
  const bound = facts.environments
    .filter((environment) => environment.bound && environment.readable)
    .map((environment) => environment.name);
  return {
    version: 1,
    repository: facts.repository,
    environments: [...facts.environments],
    secrets,
    unsatisfied: unsatisfiedSecrets(secrets, bound).map((row) => ({
      name: row.name,
      repositorySecret: row.repositorySecret,
      missingIn: environmentsMissing(row, bound),
    })),
  };
}

/**
 * The consumer's boundary: text off a `smoo secrets status --json` run into a
 * validated document, or typia's own account of every field that disagreed.
 */
export const parseSecretsStatusDocument = typia.json.createValidateParse<SecretsStatusDocument>();

/**
 * The emitter's boundary: the projection is checked against its declared type
 * before it becomes text. A shape bug then fails here, naming the field, rather
 * than reaching a consumer as a document its validator rejects with no idea who
 * produced it.
 */
export const stringifySecretsStatusDocument = typia.json.createAssertStringify<SecretsStatusDocument>();
