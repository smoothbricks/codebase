/**
 * The bootstrap secret resolver, as the CLI sees it.
 *
 * `managed/raw/tooling/direnv/secret-references.ts` holds the one
 * implementation of THE RULE — which group resolves where, and what CI and a
 * cowshed workspace refuse instead. A managed repository runs the copy smoo
 * writes into its own `tooling/direnv/secret-references.ts` at shell entry;
 * `smoo secrets run` runs THIS package's copy of the same file. One
 * implementation, so the command and the shell cannot disagree about a group.
 *
 * The Nx plugin ships this raw bootstrap asset. Resolve it from the installed
 * plugin, not a checkout-relative path, so source and published layouts agree.
 * It remains outside compiled src: bootstrap runs before the Typia preload.
 */

import { join } from 'node:path';
import { pathToFileURL } from 'node:url';
import { managedAssetsRoot } from '@smoothbricks/nx-plugin/managed-assets';
import typia from 'typia';

/** A declared `smoo.secrets` entry with its group settled. */
export interface GroupedSecret {
  name: string;
  /** The group that resolves it: the declared one when stated, `derivedGroup` otherwise. */
  group: string;
  /**
   * What the repository's own declarations imply: `registry` for a variable
   * `.npmrc` interpolates, `nx-cache` for the declared cache token, `shell`
   * for everything else. Differs from `group` exactly when an entry overrides
   * the derivation.
   */
  derivedGroup: string;
}

/** A declared secret a request declined to resolve, and how to supply it. */
export interface DeferredSecret {
  name: string;
  group: string;
  guidance: string;
}

/** What belongs in a child's environment, and what was deliberately left out of it. */
export interface SecretResolution {
  values: Record<string, string>;
  deferred: DeferredSecret[];
}

interface SecretReferencesModule {
  readSecretGroups?: unknown;
  resolveSecretEnvironment?: unknown;
}

const isSecretReferencesModule = typia.createIs<SecretReferencesModule>();
const isGroupedSecrets = typia.createIs<GroupedSecret[]>();
const isSecretResolution = typia.createIs<SecretResolution>();

const RESOLVER_PATH = join(managedAssetsRoot, 'raw/tooling/direnv/secret-references.ts');

interface SecretReferences {
  readSecretGroups: (root: string) => unknown;
  resolveSecretEnvironment: (options: { root: string; group: string }) => unknown;
}

/**
 * A loaded export, narrowed to the signature this file calls it with. typia
 * validates data, not functions — it reports a module missing both of these
 * as valid — so presence is checked here, and what each one RETURNS is
 * validated below, where it is data again.
 */
function isSecretGroupsReader(value: unknown): value is SecretReferences['readSecretGroups'] {
  return typeof value === 'function';
}

function isSecretGroupResolver(value: unknown): value is SecretReferences['resolveSecretEnvironment'] {
  return typeof value === 'function';
}

let loaded: SecretReferences | undefined;

async function secretReferences(): Promise<SecretReferences> {
  if (loaded !== undefined) {
    return loaded;
  }
  // Dynamic by necessity: the target is a managed raw script outside this
  // package's compiled rootDir, so no static import can name it.
  const imported: unknown = await import(pathToFileURL(RESOLVER_PATH).href);
  if (!isSecretReferencesModule(imported)) {
    throw new Error(`${RESOLVER_PATH} did not load as a module`);
  }
  const { readSecretGroups: read, resolveSecretEnvironment: resolve } = imported;
  if (!isSecretGroupsReader(read) || !isSecretGroupResolver(resolve)) {
    throw new Error(`${RESOLVER_PATH} does not expose the expected secret resolver API`);
  }
  loaded = { readSecretGroups: read, resolveSecretEnvironment: resolve };
  return loaded;
}

/**
 * Every `smoo.secrets` entry of the repository at `root`, each with the group
 * that resolves it. Rejects with the offending declaration named — never a
 * value — when the manifest or `.npmrc` cannot be read.
 */
export async function readSecretGroups(root: string): Promise<GroupedSecret[]> {
  const references = await secretReferences();
  const groups: unknown = references.readSecretGroups(root);
  if (!isGroupedSecrets(groups)) {
    throw new Error(`${RESOLVER_PATH} returned an unexpected group listing`);
  }
  return groups;
}

/**
 * Resolve exactly one group's secrets for one child process. Everything
 * outside the group comes back deferred rather than resolved, so a run for
 * one group never triggers another group's provider command. Rejects with the
 * resolver's aggregated refusal, which names variables and groups only.
 */
export async function resolveSecretGroup(root: string, group: string): Promise<SecretResolution> {
  const references = await secretReferences();
  const resolution: unknown = await references.resolveSecretEnvironment({ root, group });
  if (!isSecretResolution(resolution)) {
    throw new Error(`${RESOLVER_PATH} returned an unexpected resolution`);
  }
  return resolution;
}
