import { readFileSync, writeFileSync } from 'node:fs';
import { run, runStatus } from '../lib/run.js';
import { listReleasePackages, type PackageInfo } from '../lib/workspace.js';
import { syncBunLockfileVersions } from '../monorepo/lockfile.js';
import { releaseCandidateShell } from './candidate-shell.js';
import { releaseCommitParagraphs, releasedByCommit } from './core.js';

/**
 * The one release commit of a release that has first releases in it.
 *
 * Nx cannot version a first release: there are no commits since a release tag for it to derive a bump from. smoo does,
 * from `firstReleases`, whose `version` is the version each one publishes (`firstReleaseVersion`): it writes that version
 * into the manifest, on top of what Nx staged for the packages it versioned, and commits everything once, with the
 * message Nx writes for a release. That message lists every package the commit releases, because a first release at the
 * version its manifest already declares changes no file, and the list is then the commit's only record of it
 * (`releasedByCommit`).
 */
export async function commitReleaseWithFirstReleases(
  root: string,
  firstReleases: readonly PackageInfo[],
): Promise<void> {
  for (const pkg of firstReleases) {
    const text = readFileSync(pkg.packageJsonPath, 'utf8');
    const versioned = manifestWithVersion(text, pkg.version);
    if (versioned === null) {
      throw new Error(
        `${pkg.packageJsonPath} has no top-level "version" string to set for the first release of ${pkg.projectName}.`,
      );
    }
    if (versioned !== text) {
      writeFileSync(pkg.packageJsonPath, versioned);
    }
  }
  // Nx versions a package whose dependency it bumps from the version on disk, a first release included, and writes that
  // version into bun.lock too. The manifests are final now, and the lockfile follows them.
  syncBunLockfileVersions(root, { log: false, stage: true });
  await run('git', ['add', '--', ...firstReleases.map((pkg) => pkg.packageJsonPath)], root);

  const shell = await releaseCandidateShell(root);
  const listed = new Map(firstReleases.map((pkg) => [pkg.projectName, pkg.version]));
  const released: PackageInfo[] = [];
  for (const pkg of listReleasePackages(root)) {
    const versionBefore = (await shell.packageJsonAtRef('HEAD', pkg.path))?.version ?? null;
    if (releasedByCommit(pkg, versionBefore, listed)) {
      released.push(pkg);
    }
  }
  const nothingStaged = (await runStatus('git', ['diff', '--cached', '--quiet'], root, true)) === 0;
  await run(
    'git',
    [
      'commit',
      ...(nothingStaged ? ['--allow-empty'] : []),
      ...releaseCommitParagraphs(released).flatMap((paragraph) => ['--message', paragraph]),
    ],
    root,
  );
}

/**
 * `manifest` with the value of its top-level `"version"` replaced by `version`, every other byte as it was: the release
 * commit changes one line of a first release's manifest, in whatever format the repository keeps it. Null when the
 * manifest has no top-level `"version"` string.
 */
export function manifestWithVersion(manifest: string, version: string): string | null {
  let depth = 0;
  let at = 0;
  while (at < manifest.length) {
    const char = manifest[at];
    if (char === '"') {
      const end = stringEnd(manifest, at);
      if (depth === 1 && manifest.slice(at, end) === '"version"') {
        const matched = /^\s*:\s*"(?:[^"\\]|\\.)*"/.exec(manifest.slice(end))?.[0];
        if (matched !== undefined) {
          const valueStart = end + matched.indexOf('"');
          return `${manifest.slice(0, valueStart)}${JSON.stringify(version)}${manifest.slice(end + matched.length)}`;
        }
      }
      at = end;
      continue;
    }
    if (char === '{' || char === '[') {
      depth++;
    } else if (char === '}' || char === ']') {
      depth--;
    }
    at++;
  }
  return null;
}

/** The index just past the string literal that opens at `start`. */
function stringEnd(text: string, start: number): number {
  let at = start + 1;
  while (at < text.length && text[at] !== '"') {
    at += text[at] === '\\' ? 2 : 1;
  }
  return Math.min(at + 1, text.length);
}
