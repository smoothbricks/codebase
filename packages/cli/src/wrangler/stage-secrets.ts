import { existsSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import typia from 'typia';
import { formatValidationErrors, parseJsonFileText } from '../lib/json.js';
import { parseDevVarsExample } from './prepare-env.js';
import { type DeploymentStage, isPullRequestStage } from './stage.js';

/**
 * How a declaration names a stage. The fixed stages go by their own name; every `prN` stage is
 * `preview`, because a scope is written once and pull-request numbers are not knowable in advance.
 */
export type SecretStageScope = 'staging' | 'production' | 'preview';

/**
 * `smoo.wrangler.secretStages`: a declared secret name mapped to the stages it belongs to. The map
 * answers two questions with one declaration, and both directions matter:
 *
 * - requirement — a stage the secret belongs to refuses to deploy without a value for it;
 * - permission — a stage the secret does *not* belong to never receives it, however loudly the
 *   deploying shell exports it. A test-only capability exported by CI for preview stages must not
 *   ride along into production just because the variable happens to be set.
 *
 * A declared name absent from the map belongs to every stage. A name mapped to `[]` belongs to no
 * stage, which is how a value that exists only for local development is declared.
 */
export type SecretStageMap = Record<string, SecretStageScope[]>;

/** The one block of a project's package.json this reader needs; every other field is ignored. */
interface WranglerPackageManifest {
  smoo?: {
    wrangler?: {
      secretStages?: SecretStageMap;
    };
  };
}

const validateWranglerManifest = typia.json.createValidateParse<WranglerPackageManifest>();

/** Secret NAMES the project declares. Values live on the Worker; the repo only ever holds the keys. */
export function readDeclaredSecretNames(cwd: string): string[] {
  const path = join(cwd, '.dev.vars.example');
  return existsSync(path) ? parseDevVarsExample(readFileSync(path, 'utf8')) : [];
}

/** `smoo.wrangler.secretStages` from the project's package.json; no file and no block scope nothing. */
export function readSecretStageMap(cwd: string): SecretStageMap {
  const path = join(cwd, 'package.json');
  if (!existsSync(path)) return {};
  const result = parseJsonFileText(path, readFileSync(path, 'utf8'), validateWranglerManifest);
  if (!result.success) {
    throw new Error(`${path} declares an invalid smoo.wrangler block: ${formatValidationErrors(result.errors)}`);
  }
  return result.data.smoo?.wrangler?.secretStages ?? {};
}

/** Which of a project's declared secrets one stage may see, and which belong to other stages. */
export interface StageSecretPlan {
  stage: DeploymentStage;
  /** Declared names this stage requires — and the only ones a deploy of it may carry. */
  required: string[];
  /** Declared names scoped to other stages: withheld from this deploy even when a value is exported. */
  withheld: string[];
  /** The declaration itself, so a refusal can say why each name is where it is. */
  scopes: SecretStageMap;
}

/**
 * Splits the declared secrets by whether `stage` is in scope for each.
 *
 * A scope on an undeclared name is refused rather than ignored: it is almost always a typo of a
 * real secret's name, and its effect is the dangerous direction — the misspelt entry scopes
 * nothing while the real secret, still absent from the map, reaches every stage.
 */
export function planStageSecrets(declared: string[], stage: DeploymentStage, scopes: SecretStageMap): StageSecretPlan {
  const declaredNames = new Set(declared);
  const undeclared = Object.keys(scopes).filter((name) => !declaredNames.has(name));
  if (undeclared.length > 0) {
    throw new Error(
      `smoo.wrangler.secretStages scopes ${undeclared.join(', ')}, which .dev.vars.example does not declare. ` +
        'A scope on a name no secret has leaves the secret it was meant for unscoped, so that secret reaches every stage.',
    );
  }
  // Every `prN` stage answers to one written token: a scope cannot name pull requests in advance.
  const scope: SecretStageScope = isPullRequestStage(stage) ? 'preview' : stage;
  const required: string[] = [];
  const withheld: string[] = [];
  for (const name of declared) {
    const declaredScope = scopes[name];
    if (declaredScope === undefined || declaredScope.includes(scope)) {
      required.push(name);
    } else {
      withheld.push(name);
    }
  }
  return { stage, required, withheld, scopes };
}

/**
 * Why this deploy must not proceed, or nothing when it may.
 *
 * Two refusals, reported together so one run names every problem:
 *
 * - a required secret with no value anywhere. `--secrets-file` applies additively, so a deploy
 *   that never mentions a secret leaves whatever the Worker already holds — silence that reads as
 *   success while a secret introduced after the first deploy never arrives, and the code that
 *   needs it fails at runtime instead of here.
 * - a withheld secret the Worker already holds. Filtering it out of this deploy's payload cannot
 *   remove it, so the scope would be nominal rather than enforced until someone deletes it.
 *
 * A value is never read, never formatted, and never named beyond its key.
 */
export function stageSecretRefusal(
  plan: StageSecretPlan,
  exported: ReadonlySet<string>,
  held: ReadonlySet<string>,
  workerName: string,
): string | undefined {
  const unavailable = plan.required.filter((name) => !exported.has(name) && !held.has(name));
  const installed = plan.withheld.filter((name) => held.has(name));
  if (unavailable.length === 0 && installed.length === 0) return undefined;
  const lines = [`Refusing to deploy ${workerName} to ${plan.stage}.`];
  if (unavailable.length > 0) {
    lines.push(
      `Stage ${plan.stage} requires these secrets and no value exists for them, neither in this environment nor on the Worker:`,
      ...unavailable.map((name) => `  ${name} — ${scopeDescription(name, plan.scopes)}`),
      'Export each one in the deploying shell before the deploy. Once the Worker exists,',
      `\`wrangler secret put <NAME> --name ${workerName}\` supplies it too.`,
    );
  }
  if (installed.length > 0) {
    lines.push(
      `The Worker holds these secrets, which smoo.wrangler.secretStages keeps out of ${plan.stage}:`,
      ...installed.map((name) => `  ${name} — ${scopeDescription(name, plan.scopes)}`),
      `Delete each one with \`wrangler secret delete <NAME> --name ${workerName}\`. A deploy of this stage`,
      'never sends them, so leaving them installed would keep the scope nominal.',
    );
  }
  return lines.join('\n');
}

function scopeDescription(name: string, scopes: SecretStageMap): string {
  const scope = scopes[name];
  if (scope === undefined) return 'unscoped, so every stage requires it';
  if (scope.length === 0) return 'scoped to no stage (local development only)';
  return `scoped to ${scope.join(', ')}`;
}
