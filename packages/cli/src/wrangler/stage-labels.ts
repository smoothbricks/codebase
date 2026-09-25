// How a staging value becomes a pull-request stage's: the label rewrites shared by the TOML
// derivation (stage.ts) and the flat-config derivation (flat-config.ts), and the hostnames a
// stage's routes serve. Internal to the package; the published `./wrangler/stage` entry does
// not re-export it.

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

/**
 * The lowercase host a route pattern serves, minus a leading `*.` and any path: `*.pr7.example.com/*`
 * and the pathless `*.pr7.example.com` both give `pr7.example.com`.
 */
export function routeHostname(pattern: string): string {
  return pattern.split('/', 1)[0].replace(/^\*\./, '').toLowerCase();
}

/**
 * The proxied CNAME `*.<host>` → `<host>` that a `*.` route with a declared zone needs, and nothing
 * for any other route. Reconcile creates the record from this, so there is one derivation of it.
 */
export function wildcardDnsRecord(route: {
  pattern: string;
  zoneName?: string;
}): { zoneName: string; name: string; content: string } | undefined {
  if (!route.pattern.startsWith('*.') || !route.zoneName) return undefined;
  const hostname = routeHostname(route.pattern);
  return { zoneName: route.zoneName, name: `*.${hostname}`, content: hostname };
}
