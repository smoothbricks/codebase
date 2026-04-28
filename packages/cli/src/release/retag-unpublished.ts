import { type ReleasePackageInfo, releasePackageForTag } from './core.js';

export interface RetagUnpublishedOptions {
  tags: string[];
  toRef: string;
  push: boolean;
  dispatch: boolean;
  dryRun: boolean;
  branch: string;
  workflow: string;
}

export interface RetagUnpublishedTagUpdate<Package extends ReleasePackageInfo = ReleasePackageInfo> {
  tag: string;
  pkg: Package;
  expectedRemoteObject: string | null;
}

export interface RetagUnpublishedShell<Package extends Omit<ReleasePackageInfo, 'version'> = ReleasePackageInfo> {
  listReleasePackages(): Package[];
  resolveRef(ref: string): Promise<string>;
  resolveDispatchRef(branch: string): Promise<string | null>;
  packageVersionAtRef(packagePath: string, ref: string): Promise<string | null>;
  npmVersionExists(name: string, version: string): Promise<boolean>;
  githubReleaseExists(tag: string): Promise<boolean>;
  remoteTagObject(tag: string): Promise<string | null>;
  createOrMoveTag(tag: string, ref: string): Promise<void>;
  pushTags(updates: Array<RetagUnpublishedTagUpdate<Package & { version: string }>>): Promise<void>;
  dispatchPublishWorkflow(workflow: string, branch: string): Promise<void>;
  log(message: string): void;
}

export async function retagUnpublished<Package extends Omit<ReleasePackageInfo, 'version'>>(
  shell: RetagUnpublishedShell<Package>,
  options: RetagUnpublishedOptions,
): Promise<Array<RetagUnpublishedTagUpdate<Package & { version: string }>>> {
  if (options.tags.length === 0) {
    throw new Error('retag-unpublished requires at least one release tag.');
  }
  assertUniqueTags(options.tags);

  const targetSha = await shell.resolveRef(options.toRef);
  if (options.dispatch) {
    const dispatchSha = await shell.resolveDispatchRef(options.branch);
    if (!dispatchSha) {
      throw new Error(`Cannot dispatch ${options.workflow}: remote branch ${options.branch} was not found.`);
    }
    if (dispatchSha !== targetSha) {
      throw new Error(
        `Cannot dispatch ${options.workflow}: ${options.toRef} resolves to ${targetSha.slice(0, 12)}, but ${
          options.branch
        } resolves to ${dispatchSha.slice(0, 12)}. Push the branch first or choose a ref already at the remote branch HEAD.`,
      );
    }
  }

  const packages = shell.listReleasePackages();
  const updates: Array<RetagUnpublishedTagUpdate<Package & { version: string }>> = [];
  for (const tag of options.tags) {
    const match = releasePackageForTag(packages, tag);
    if (!match) {
      throw new Error(`Release tag ${tag} does not match an owned release package.`);
    }
    const versionAtRef = await shell.packageVersionAtRef(match.pkg.path, options.toRef);
    if (versionAtRef !== match.version) {
      throw new Error(
        `Release tag ${tag} cannot move to ${options.toRef}: ${match.pkg.path}/package.json has version ${
          versionAtRef ?? 'missing'
        }, expected ${match.version}.`,
      );
    }
    if (await shell.npmVersionExists(match.pkg.name, match.version)) {
      throw new Error(`Cannot retag ${tag}: ${match.pkg.name}@${match.version} already exists on npm.`);
    }
    if (await shell.githubReleaseExists(tag)) {
      throw new Error(`Cannot retag ${tag}: GitHub Release ${tag} already exists.`);
    }

    updates.push({
      tag,
      pkg: { ...match.pkg, version: match.version },
      expectedRemoteObject: options.push ? await shell.remoteTagObject(tag) : null,
    });
  }

  for (const update of updates) {
    if (options.dryRun) {
      shell.log(`Would move ${update.tag} to ${options.toRef} (${targetSha.slice(0, 12)}).`);
    } else {
      await shell.createOrMoveTag(update.tag, options.toRef);
      shell.log(`Moved ${update.tag} to ${options.toRef} (${targetSha.slice(0, 12)}).`);
    }
  }
  if (options.push) {
    if (options.dryRun) {
      shell.log(`Would push ${updates.length} retagged release tag${updates.length === 1 ? '' : 's'}.`);
    } else {
      await shell.pushTags(updates);
      shell.log(`Pushed ${updates.length} retagged release tag${updates.length === 1 ? '' : 's'}.`);
    }
  }
  if (options.dispatch) {
    if (options.dryRun) {
      shell.log(`Would dispatch ${options.workflow} on ${options.branch} with bump=auto.`);
    } else {
      await shell.dispatchPublishWorkflow(options.workflow, options.branch);
      shell.log(`Dispatched ${options.workflow} on ${options.branch} with bump=auto.`);
    }
  }

  return updates;
}

function assertUniqueTags(tags: string[]): void {
  const seen = new Set<string>();
  for (const tag of tags) {
    if (seen.has(tag)) {
      throw new Error(`Release tag ${tag} was provided more than once.`);
    }
    seen.add(tag);
  }
}
