import { existsSync, readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import type {
  NxJson,
  NxProjectJson,
  NxTargetConfig,
  PackageJson,
  PackageNxConfig,
  PackagePublishConfig,
  PackageRepository,
  PackageSmooGithub,
  StringMap,
} from '@smoothbricks/nx-plugin/workspace-manifest';
import typia, { type IValidation } from 'typia';

export * from '@smoothbricks/nx-plugin/workspace-manifest';

/** Parse package.json text. Invalid JSON throws; wrong shape returns null. */
export const parsePackageJsonText = typia.json.createIsParse<PackageJson>();

/** Parse `nx show project --json` output. Invalid JSON throws; wrong shape returns null. */
export const parseNxProjectJsonText = typia.json.createIsParse<NxProjectJson>();

/** Parse nx.json text. Invalid JSON throws; wrong shape returns null. */
export const parseNxJsonText = typia.json.createIsParse<NxJson>();

/** Parse a JSON string array. Invalid JSON throws; non-arrays return null. */
export const parseStringArrayText = typia.json.createIsParse<string[]>();

/** typia's failures as `path: expected type` items, the `$input` root stripped so paths read as the document's own. */
export function formatValidationErrors(errors: IValidation.IError[]): string {
  return errors.map((error) => `${error.path.replace(/^\$input\.?/, '')}: expected ${error.expected}`).join(', ');
}

/**
 * A file's text through a typia JSON parser. Text that is not JSON at all makes the parser throw a bare
 * SyntaxError that names no file; the rethrow names the one the text came from.
 */
export function parseJsonFileText<T>(path: string, text: string, parse: (text: string) => T): T {
  try {
    return parse(text);
  } catch (error) {
    if (error instanceof SyntaxError) throw new Error(`${path} is not valid JSON: ${error.message}`);
    throw error;
  }
}

const validatePackageJsonText = typia.json.createValidateParse<PackageJson>();

const isPackageJsonValue = typia.createIs<PackageJson>();

export function readJsonObject(path: string): PackageJson | null {
  if (!existsSync(path)) {
    return null;
  }
  try {
    return parsePackageJsonText(readFileSync(path, 'utf8'));
  } catch {
    return null;
  }
}

export function requiredJsonObject(path: string): PackageJson {
  const json = readJsonObject(path);
  if (!json) {
    throw new Error(`${path} not found or invalid`);
  }
  return json;
}

export function writeJsonObject(path: string, value: object): void {
  writeFileSync(path, jsonObjectText(value));
}

export function jsonObjectText(value: object): string {
  return `${JSON.stringify(value, null, 2)}\n`;
}

export function readJson(path: string): unknown {
  if (!existsSync(path)) {
    return null;
  }
  return JSON.parse(readFileSync(path, 'utf8'));
}

export function isPackageJson(value: unknown): value is PackageJson {
  return isPackageJsonValue(value);
}

/**
 * package.json validated as a whole, every wrong value named by its path; null when the file is absent. The
 * is-parser behind `readJsonObject` answers a wrong value with null for the whole manifest instead, which for the
 * `smoo.github` block would mean silently falling back to the defaults (branch `main`, no environments or secrets).
 */
export function readValidatedPackageJson(path: string): PackageJson | null {
  if (!existsSync(path)) {
    return null;
  }
  const result = parseJsonFileText(path, readFileSync(path, 'utf8'), validatePackageJsonText);
  if (result.success) {
    return result.data;
  }
  throw new Error(`package.json is invalid: ${formatValidationErrors(result.errors)}`);
}

/** The root manifest's `smoo.github` block (see `readValidatedPackageJson`); nothing without a manifest or a block. */
export function readSmooGithub(root: string): PackageSmooGithub | undefined {
  return readValidatedPackageJson(join(root, 'package.json'))?.smoo?.github;
}

/** The branches whose pushes drive the managed CI; the first one maps to the staging stage. */

/** Ensure package.json.scripts exists. */
export function ensureScripts(pkg: PackageJson): StringMap {
  if (isStringMap(pkg.scripts)) {
    return pkg.scripts;
  }
  const next: StringMap = {};
  pkg.scripts = next;
  return next;
}

/** Ensure package.json.dependencies (or other string-map dep field) exists. */
export function ensureDependencyMap(
  pkg: PackageJson,
  key: 'dependencies' | 'devDependencies' | 'peerDependencies' | 'optionalDependencies' | 'patchedDependencies',
): StringMap {
  const current = pkg[key];
  if (isStringMap(current)) {
    return current;
  }
  const next: StringMap = {};
  pkg[key] = next;
  return next;
}

/** Ensure package.json.engines exists. */
export function ensureEngines(pkg: PackageJson): StringMap {
  if (isStringMap(pkg.engines)) {
    return pkg.engines;
  }
  const next: StringMap = {};
  pkg.engines = next;
  return next;
}

/** Ensure package.json.nx exists. */
export function ensureNx(pkg: PackageJson): PackageNxConfig {
  if (isPackageNxConfig(pkg.nx)) {
    return pkg.nx;
  }
  const next: PackageNxConfig = {};
  pkg.nx = next;
  return next;
}

/** Ensure package.json.nx.targets exists. */
export function ensureNxTargets(nx: PackageNxConfig): Record<string, NxTargetConfig> {
  if (isNxTargets(nx.targets)) {
    return nx.targets;
  }
  const next: Record<string, NxTargetConfig> = {};
  nx.targets = next;
  return next;
}

/** Ensure package.json.publishConfig exists. */
export function ensurePublishConfig(pkg: PackageJson): PackagePublishConfig {
  if (isPublishConfig(pkg.publishConfig)) {
    return pkg.publishConfig;
  }
  const next: PackagePublishConfig = {};
  pkg.publishConfig = next;
  return next;
}

/** Ensure package.json.repository is an object (not a string URL). */
export function ensureRepositoryObject(pkg: PackageJson): PackageRepository {
  if (isRepositoryObject(pkg.repository)) {
    return pkg.repository;
  }
  const next: PackageRepository = {};
  pkg.repository = next;
  return next;
}

export function setStringProperty(record: StringMap, key: string, value: string): boolean {
  if (record[key] === value) {
    return false;
  }
  record[key] = value;
  return true;
}

export function setMissingStringProperty(record: StringMap, key: string, value: string): boolean {
  if (typeof record[key] === 'string') {
    return false;
  }
  record[key] = value;
  return true;
}

export function setPackageStringField(
  pkg: PackageJson,
  key: 'name' | 'version' | 'license' | 'types' | 'packageManager',
  value: string,
): boolean {
  if (pkg[key] === value) {
    return false;
  }
  pkg[key] = value;
  return true;
}

export function setMissingPackageStringField(
  pkg: PackageJson,
  key: 'name' | 'version' | 'license' | 'types' | 'packageManager',
  value: string,
): boolean {
  if (typeof pkg[key] === 'string') {
    return false;
  }
  pkg[key] = value;
  return true;
}

export function setNxName(nx: PackageNxConfig, value: string): boolean {
  if (nx.name === value) {
    return false;
  }
  nx.name = value;
  return true;
}

export function setPublishAccess(publishConfig: PackagePublishConfig, value: string): boolean {
  if (publishConfig.access === value) {
    return false;
  }
  publishConfig.access = value;
  return true;
}

export function setRepositoryField(
  repository: PackageRepository,
  key: 'type' | 'url' | 'directory',
  value: string,
): boolean {
  if (repository[key] === value) {
    return false;
  }
  repository[key] = value;
  return true;
}

export function setMissingRepositoryField(
  repository: PackageRepository,
  key: 'type' | 'url' | 'directory',
  value: string,
): boolean {
  if (typeof repository[key] === 'string') {
    return false;
  }
  repository[key] = value;
  return true;
}

function isStringMap(value: unknown): value is StringMap {
  return value !== null && value !== undefined && typeof value === 'object' && !Array.isArray(value);
}

function isPackageNxConfig(value: unknown): value is PackageNxConfig {
  return value !== null && value !== undefined && typeof value === 'object' && !Array.isArray(value);
}

function isNxTargets(value: unknown): value is Record<string, NxTargetConfig> {
  return value !== null && value !== undefined && typeof value === 'object' && !Array.isArray(value);
}

function isPublishConfig(value: unknown): value is PackagePublishConfig {
  return value !== null && value !== undefined && typeof value === 'object' && !Array.isArray(value);
}

function isRepositoryObject(value: unknown): value is PackageRepository {
  return value !== null && value !== undefined && typeof value === 'object' && !Array.isArray(value);
}
