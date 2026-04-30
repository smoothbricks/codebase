import { existsSync, readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';

import type { NxPolicyIssue } from './workspace-config-policy.js';

// ---------------------------------------------------------------------------
// Public constants
// ---------------------------------------------------------------------------

export const SMOO_NX_VERSION_ACTIONS = '@smoothbricks/nx-plugin/version-actions';
export const SMOO_NX_RELEASE_TAG_PATTERN = '{projectName}@{version}';

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/**
 * Check nx.json release config policy, returns issues found.
 * Only checks release-specific config — workspace config policy is checked separately.
 */
export function checkReleaseConfigPolicy(root: string): NxPolicyIssue[] {
  const nxJsonPath = join(root, 'nx.json');
  const nxJson = readJsonObject(nxJsonPath);
  if (!nxJson) {
    return [{ path: nxJsonPath, message: 'nx.json not found or invalid' }];
  }
  const issues: NxPolicyIssue[] = [];

  const release = recordProperty(nxJson, 'release');
  if (!release) {
    issues.push({ path: nxJsonPath, message: 'release config is missing' });
    return issues;
  }

  if (stringProperty(release, 'projectsRelationship') !== 'independent') {
    issues.push({ path: nxJsonPath, message: 'release.projectsRelationship must be independent' });
  }

  const version = recordProperty(release, 'version');
  if (!version) {
    issues.push({ path: nxJsonPath, message: 'release.version config is missing' });
  }
  if (version && stringProperty(version, 'specifierSource') !== 'conventional-commits') {
    issues.push({ path: nxJsonPath, message: 'release.version.specifierSource must be conventional-commits' });
  }
  if (version && stringProperty(version, 'currentVersionResolver') !== 'git-tag') {
    issues.push({ path: nxJsonPath, message: 'release.version.currentVersionResolver must be git-tag' });
  }
  if (version && stringProperty(version, 'fallbackCurrentVersionResolver') !== 'disk') {
    issues.push({ path: nxJsonPath, message: 'release.version.fallbackCurrentVersionResolver must be disk' });
  }
  if (version && stringProperty(version, 'versionActions') !== SMOO_NX_VERSION_ACTIONS) {
    issues.push({
      path: nxJsonPath,
      message: `release.version.versionActions must be ${SMOO_NX_VERSION_ACTIONS}`,
    });
  }
  if (version && stringProperty(version, 'preVersionCommand')) {
    issues.push({
      path: nxJsonPath,
      message: 'release.version.preVersionCommand must not be defined; smoo builds npm-missing packages before publish',
    });
  }

  const releaseTag = recordProperty(release, 'releaseTag');
  if (!releaseTag) {
    issues.push({ path: nxJsonPath, message: 'release.releaseTag config is missing' });
  }
  if (releaseTag && stringProperty(releaseTag, 'pattern') !== SMOO_NX_RELEASE_TAG_PATTERN) {
    issues.push({
      path: nxJsonPath,
      message: `release.releaseTag.pattern must be ${SMOO_NX_RELEASE_TAG_PATTERN}`,
    });
  }

  const changelog = recordProperty(release, 'changelog');
  if (!changelog) {
    issues.push({ path: nxJsonPath, message: 'release.changelog config is missing' });
  }
  if (changelog && changelog.workspaceChangelog !== false) {
    issues.push({ path: nxJsonPath, message: 'release.changelog.workspaceChangelog must be false' });
  }

  const projectChangelogs = changelog ? recordProperty(changelog, 'projectChangelogs') : null;
  if (!projectChangelogs) {
    issues.push({ path: nxJsonPath, message: 'release.changelog.projectChangelogs config is missing' });
  }
  if (projectChangelogs && projectChangelogs.createRelease !== false) {
    issues.push({
      path: nxJsonPath,
      message: 'release.changelog.projectChangelogs.createRelease must be false',
    });
  }
  if (projectChangelogs && projectChangelogs.file !== false) {
    issues.push({
      path: nxJsonPath,
      message: 'release.changelog.projectChangelogs.file must be false',
    });
  }

  const renderOptions = projectChangelogs ? recordProperty(projectChangelogs, 'renderOptions') : null;
  if (!renderOptions) {
    issues.push({
      path: nxJsonPath,
      message: 'release.changelog.projectChangelogs.renderOptions config is missing',
    });
  }

  return issues;
}

/**
 * Fix nx.json release config policy. Returns whether anything changed.
 */
export function applyReleaseConfigPolicy(root: string): boolean {
  const nxJsonPath = join(root, 'nx.json');
  const nxJson = readJsonObject(nxJsonPath);
  if (!nxJson) {
    return false;
  }
  let changed = false;

  const release = getOrCreateRecord(nxJson, 'release');
  changed = setStringProperty(release, 'projectsRelationship', 'independent') || changed;

  const version = getOrCreateRecord(release, 'version');
  changed = setStringProperty(version, 'specifierSource', 'conventional-commits') || changed;
  changed = setStringProperty(version, 'currentVersionResolver', 'git-tag') || changed;
  changed = setStringProperty(version, 'fallbackCurrentVersionResolver', 'disk') || changed;
  changed = setStringProperty(version, 'versionActions', SMOO_NX_VERSION_ACTIONS) || changed;

  if ('preVersionCommand' in version) {
    delete version.preVersionCommand;
    changed = true;
  }

  const releaseTag = getOrCreateRecord(release, 'releaseTag');
  changed = setStringProperty(releaseTag, 'pattern', SMOO_NX_RELEASE_TAG_PATTERN) || changed;

  const changelog = getOrCreateRecord(release, 'changelog');
  changed = setBooleanProperty(changelog, 'workspaceChangelog', false) || changed;

  const projectChangelogs = getOrCreateRecord(changelog, 'projectChangelogs');
  changed = setBooleanProperty(projectChangelogs, 'createRelease', false) || changed;
  changed = setBooleanProperty(projectChangelogs, 'file', false) || changed;

  const renderOptions = getOrCreateRecord(projectChangelogs, 'renderOptions');
  if (typeof renderOptions.authors !== 'boolean') {
    renderOptions.authors = true;
    changed = true;
  }
  if (typeof renderOptions.applyUsernameToAuthors !== 'boolean') {
    renderOptions.applyUsernameToAuthors = true;
    changed = true;
  }

  if (changed) {
    writeJsonObject(nxJsonPath, nxJson);
  }
  return changed;
}

// ---------------------------------------------------------------------------
// JSON helpers (self-contained, following bounded-test-policy.ts pattern)
// ---------------------------------------------------------------------------

function readJsonObject(path: string): Record<string, unknown> | null {
  if (!existsSync(path)) {
    return null;
  }
  try {
    const parsed: unknown = JSON.parse(readFileSync(path, 'utf8'));
    return isRecord(parsed) ? parsed : null;
  } catch {
    return null;
  }
}

function writeJsonObject(path: string, value: Record<string, unknown>): void {
  writeFileSync(path, `${JSON.stringify(value, null, 2)}\n`);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function stringProperty(record: Record<string, unknown>, key: string): string | null {
  const value = record[key];
  return typeof value === 'string' && value.length > 0 ? value : null;
}

function recordProperty(record: Record<string, unknown>, key: string): Record<string, unknown> | null {
  const value = record[key];
  return isRecord(value) ? value : null;
}

function getOrCreateRecord(record: Record<string, unknown>, key: string): Record<string, unknown> {
  const value = record[key];
  if (isRecord(value)) {
    return value;
  }
  const next: Record<string, unknown> = {};
  record[key] = next;
  return next;
}

function setStringProperty(record: Record<string, unknown>, key: string, value: string): boolean {
  if (record[key] === value) {
    return false;
  }
  record[key] = value;
  return true;
}

function setBooleanProperty(record: Record<string, unknown>, key: string, value: boolean): boolean {
  if (record[key] === value) {
    return false;
  }
  record[key] = value;
  return true;
}
