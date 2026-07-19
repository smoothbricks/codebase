import { existsSync, readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { ensureObjectField, type NxTargetConfig, type PackageJson, writeJsonObject } from '../lib/json.js';
import { getWorkspacePackageManifests } from '../lib/workspace.js';

interface WranglerProject {
  label: string;
  dir: string;
  packageJsonPath: string;
  json: PackageJson;
  tomlPath: string;
}

/** First `[env.<name>]` header in a wrangler.toml, or `null` when the config declares no envs (top-level bindings only). */
export function firstWranglerEnv(tomlText: string): string | null {
  const match = tomlText.match(/^\s*\[env\.([A-Za-z0-9_-]+)/m);
  return match ? match[1] : null;
}

function wranglerProjects(root: string): WranglerProject[] {
  const projects: WranglerProject[] = [];
  for (const pkg of getWorkspacePackageManifests(root)) {
    const dir = join(root, pkg.path);
    const tomlPath = join(dir, 'wrangler.toml');
    if (!existsSync(tomlPath)) {
      continue;
    }
    projects.push({
      label: pkg.path || '.',
      dir,
      packageJsonPath: pkg.packageJsonPath,
      json: pkg.json,
      tomlPath,
    });
  }
  return projects;
}

/** Ensure the root .gitignore ignores the local secret file + generated types (once). */
function ensureGitignore(root: string): void {
  const path = join(root, '.gitignore');
  const text = existsSync(path) ? readFileSync(path, 'utf8') : '';
  const lines = text.split('\n').map((line) => line.trim());
  const missing = ['.dev.vars', 'worker-configuration.d.ts'].filter((entry) => !lines.includes(entry));
  if (missing.length === 0) {
    return;
  }
  const block = `${text.replace(/\s*$/, '')}\n\n# wrangler: local secret values + generated types (names live in .dev.vars.example)\n${missing.join('\n')}\n`;
  writeFileSync(path, block);
  console.log(`updated        .gitignore (${missing.join(', ')})`);
}

export function applyWranglerDefaults(root: string): void {
  const projects = wranglerProjects(root);
  if (projects.length > 0) {
    ensureGitignore(root);
  }
  for (const project of projects) {
    const env = firstWranglerEnv(readFileSync(project.tomlPath, 'utf8'));
    const nx = ensureObjectField(project.json, 'nx', () => ({}));
    const targets = ensureObjectField(nx, 'targets', () => ({}));
    const desired: NxTargetConfig = {
      executor: 'nx:run-commands',
      cache: true,
      inputs: ['{projectRoot}/wrangler.toml', '{projectRoot}/.dev.vars.example'],
      outputs: ['{projectRoot}/worker-configuration.d.ts'],
      options: {
        command: `wrangler types${env ? ` --env ${env}` : ''} --env-file .dev.vars.example --include-runtime false`,
        cwd: '{projectRoot}',
      },
    };
    let changed = false;
    const existing = targets['wrangler-types'];
    if (!existing || JSON.stringify(existing) !== JSON.stringify(desired)) {
      targets['wrangler-types'] = desired;
      changed = true;
    }
    const typecheck = targets.typecheck;
    if (typecheck) {
      const dependsOn = Array.isArray(typecheck.dependsOn) ? [...typecheck.dependsOn] : [];
      if (!dependsOn.includes('wrangler-types')) {
        dependsOn.push('wrangler-types');
        typecheck.dependsOn = dependsOn;
        changed = true;
      }
    }
    if (changed) {
      writeJsonObject(project.packageJsonPath, project.json);
      console.log(`updated        ${project.label}/package.json wrangler-types target`);
    } else {
      console.log(`unchanged      ${project.label}/package.json wrangler-types target`);
    }
    // Empty .dev.vars.example = "this worker declares no secrets". Bootstrap it
    // when missing; humans add SECRET_NAME= lines if the worker reads any (the
    // wrangler-types + tsc gate fails on an undeclared secret until they do).
    const examplePath = join(project.dir, '.dev.vars.example');
    if (!existsSync(examplePath)) {
      writeFileSync(examplePath, '');
      console.log(`created        ${project.label}/.dev.vars.example (no secrets)`);
    }
  }
}

export function validateWrangler(root: string): number {
  const gitignorePath = join(root, '.gitignore');
  const gitignoreLines = existsSync(gitignorePath)
    ? readFileSync(gitignorePath, 'utf8')
        .split('\n')
        .map((line) => line.trim())
    : [];
  const gitignoreCoversSecrets =
    gitignoreLines.includes('.dev.vars') && gitignoreLines.includes('worker-configuration.d.ts');
  let problems = 0;
  for (const project of wranglerProjects(root)) {
    let projectProblems = 0;
    if (!existsSync(join(project.dir, '.dev.vars.example'))) {
      console.log(`⨯ ${project.label}: missing .dev.vars.example`);
      projectProblems++;
    }
    const targets = project.json.nx?.targets;
    if (!targets?.['wrangler-types']) {
      console.log(`⨯ ${project.label}: missing wrangler-types nx target`);
      projectProblems++;
    }
    if (!gitignoreCoversSecrets) {
      console.log(`⨯ ${project.label}: root .gitignore must ignore .dev.vars and worker-configuration.d.ts`);
      projectProblems++;
    }
    if (projectProblems === 0) {
      console.log(`✓ ${project.label}: wrangler config`);
    }
    problems += projectProblems;
  }
  return problems;
}
