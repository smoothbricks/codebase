import type { PackageJson, PackagePrivateNpmConfig } from '../workspace-manifest.js';
import { isPublishablePackage } from '../workspace-package-policy.js';

/** Committed npmrc values only; workflow generation must not depend on a user's home. */
export function npmrcValueFromText(text: string, key: string): string | undefined {
  for (const line of text.split(/\r?\n/)) {
    const trimmed = line.trim();
    if (trimmed.startsWith('#') || trimmed.startsWith(';')) continue;
    const separator = trimmed.indexOf('=');
    if (separator > 0 && trimmed.slice(0, separator).trim() === key) return trimmed.slice(separator + 1).trim();
  }
  return undefined;
}

export function npmrcTokenEnvFromText(text: string, scope: string): string | undefined {
  const registry = npmrcValueFromText(text, `${scope}:registry`);
  if (!registry) return undefined;
  let url: URL;
  try {
    url = new URL(registry);
  } catch {
    return undefined;
  }
  const pathname = url.pathname.endsWith('/') ? url.pathname : `${url.pathname}/`;
  return /^\$\{([A-Za-z_][A-Za-z0-9_]*)\}$/.exec(
    npmrcValueFromText(text, `//${url.host}${pathname}:_authToken`) ?? '',
  )?.[1];
}

/** Resolve workflow credential NAMES without obtaining, interpolating, or persisting any secret. */
export function privateNpmWorkflowConfig(
  declared: PackagePrivateNpmConfig | undefined,
  root: PackageJson,
  packages: readonly PackageJson[],
  npmrc: string,
): PackagePrivateNpmConfig | undefined {
  if (!declared) return undefined;
  const inScope = (name: string) => name === declared.scope || name.startsWith(`${declared.scope}/`);
  const consumes = [root, ...packages].some((pkg) =>
    [pkg.dependencies, pkg.devDependencies, pkg.peerDependencies, pkg.optionalDependencies].some((deps) =>
      Object.entries(deps ?? {}).some(([name, spec]) => inScope(name) && !/^(workspace|link|file):/.test(spec)),
    ),
  );
  const publishes = packages.some(
    (pkg) =>
      pkg.name &&
      pkg.version &&
      inScope(pkg.name) &&
      isPublishablePackage({ private: pkg.private === true, tags: pkg.nx?.tags ?? [] }) &&
      pkg.nx?.tags?.includes('npm:private'),
  );
  if (!consumes && !publishes) return undefined;
  const npmrcEnv = npmrcTokenEnvFromText(npmrc, declared.scope);
  const readTokenEnv = declared.readTokenEnv ?? (consumes ? npmrcEnv : undefined);
  const publishTokenEnv = publishes ? (declared.publishTokenEnv ?? npmrcEnv) : undefined;
  if (!readTokenEnv && !publishTokenEnv) return undefined;
  return {
    scope: declared.scope,
    ...(readTokenEnv ? { readTokenEnv } : {}),
    ...(publishTokenEnv ? { publishTokenEnv } : {}),
  };
}
