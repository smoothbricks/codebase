// The Nx tags that drive stage deploys. CI selection (github-ci), workflow generation (monorepo)
// and the graph readers must agree on these, so they live here rather than with any one of them.

export type DeploymentStage = 'staging' | 'production' | `pr${number}`;

/** Deployed on every stage: one command that deploys whichever stage it is given. */
const STAGE_DEPLOY_TAG = 'stage-deploy-target';

/** Deployed on staging only: infrastructure the pull-request stages share. */
export const STAGING_DEPLOY_TAG = 'staging-deploy-target';

/** Excluded from every stage deploy; deployed outside the stage flow. */
export const PERMANENT_DEPLOY_TAG = 'permanent-deploy-target';

/** Opts a project into the generated production-on-push deploy job. */
export const PRODUCTION_PUSH_DEPLOY_TAG = 'production-push-deploy-target';

/**
 * Whether the stage flow deploys a project, from its TAGS alone — the one rule
 * CI selection and workflow generation both read, so a generated deploy job and
 * the projects that job deploys cannot disagree.
 *
 * Having a `deploy` target is not that answer. A deploy target says a project
 * CAN be deployed; only a tag says CI does it. A published library ships a
 * wrangler manifest to document a Durable Object binding for its consumers, and
 * that manifest is structurally identical to a deployable worker's — so the
 * plugin infers `deploy` from one, and a repository that deploys nothing from CI
 * would otherwise generate a deploy job, with cloud credentials, for it.
 *
 * The three tags compose as precedence, not as a set: `permanent` excludes a
 * project from every stage, `staging` narrows it to one, `stage` generalizes it
 * to all. `production-push` selects WITHIN this answer (see `--select-tag`) and
 * never widens it.
 */
export function stageDeploysProject(tags: string[] | undefined, stage: DeploymentStage): boolean {
  if (tags === undefined) return false;
  if (tags.includes(PERMANENT_DEPLOY_TAG)) return false;
  if (tags.includes(STAGING_DEPLOY_TAG)) return stage === 'staging';
  return tags.includes(STAGE_DEPLOY_TAG);
}
