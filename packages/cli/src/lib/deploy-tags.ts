// The Nx tags that drive stage deploys. CI selection (github-ci), workflow generation (monorepo)
// and the graph readers must agree on these, so they live here rather than with any one of them.

/** Deployed on every stage: one command that deploys whichever stage it is given. */
const STAGE_DEPLOY_TAG = 'stage-deploy-target';

/** Deployed on staging only: infrastructure the pull-request stages share. */
export const STAGING_DEPLOY_TAG = 'staging-deploy-target';

/** Excluded from every stage deploy; deployed outside the stage flow. */
export const PERMANENT_DEPLOY_TAG = 'permanent-deploy-target';

/** Opts a project into the generated production-on-push deploy job. */
export const PRODUCTION_PUSH_DEPLOY_TAG = 'production-push-deploy-target';

/**
 * Deployed after every other project the same run selected: its deploy calls into what they deploy (a site that
 * signs in to its stage's backend). It orders; it never selects.
 */
export const LATE_DEPLOY_TAG = 'late-deploy-target';

/**
 * Whether a deploy target is stage-derived (the tag, or the `smoo wrangler deploy-stage` command) rather than a set of
 * per-stage configurations; CI selection and workflow generation must agree on this.
 */
export function isStageDerivedDeploy(tags: string[] | undefined, command: string | undefined): boolean {
  return tags?.includes(STAGE_DEPLOY_TAG) === true || command?.includes('smoo wrangler deploy-stage') === true;
}
