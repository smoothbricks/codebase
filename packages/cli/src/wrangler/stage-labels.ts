// How a staging value becomes a pull-request stage's: the label rewrites shared by the TOML
// derivation (stage.ts) and the flat-config derivation (flat-config.ts). Internal to the
// package; the published `./wrangler/stage` entry does not re-export it.

// The predicate and the rewriter share one regex so they cannot drift:
// `hasStageLabel(v)` is true exactly when `replaceHostnameLabel(v, stage)` would change `v`.
const STAGING_HOSTNAME_LABEL = /(^|[.@/])staging(?=\.)/g;

/** True when a hostname-bearing value carries `staging` as a label, i.e. it derives to a PR stage. */
export function hasStageLabel(value: string): boolean {
  // A global regex's `.test()` resumes from the previous match, so rewind it first.
  STAGING_HOSTNAME_LABEL.lastIndex = 0;
  return STAGING_HOSTNAME_LABEL.test(value);
}

export function replaceHostnameLabel(value: string, stage: `pr${number}`): string {
  return value.replace(STAGING_HOSTNAME_LABEL, `$1${stage}`);
}

export function replaceExactToken(value: string, from: string, to: string): string {
  const escaped = from.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  return value.replace(new RegExp(`(^|[-.])${escaped}(?=$|[-.])`, 'g'), `$1${to}`);
}

/**
 * A pull-request name derived from its staging counterpart. A value with no exact
 * `staging` segment cannot be derived, and reusing it verbatim would silently share
 * staging's namespace, bucket or database with the pull request.
 */
export function derivedStagingName(value: string, stage: `pr${number}`, what: string): string {
  const derived = replaceExactToken(value, 'staging', stage);
  if (derived === value) {
    throw new Error(`${what} ${value} has no exact staging segment.`);
  }
  return derived;
}
