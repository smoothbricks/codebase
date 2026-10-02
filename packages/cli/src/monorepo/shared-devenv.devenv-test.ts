/**
 * shared-devenv.ts against real devenv, the way the managed .envrc drives it: `enter`, then `export` as
 * `use devenv`'s DEVENV_BIN for `devenv direnv-export`, then the export evaluated the way direnv imports it.
 * Every evaluation is real, so this suite runs in its own `devenv-test` target with its own deadline.
 */
import { describe, expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import {
  existsSync,
  mkdirSync,
  mkdtempSync,
  readdirSync,
  readFileSync,
  realpathSync,
  rmSync,
  writeFileSync,
} from 'node:fs';
import { join } from 'node:path';
import { managedAssetsRoot } from '@smoothbricks/nx-plugin/managed-assets';
import { printCommandOutput } from '../lib/run.js';

const program = join(managedAssetsRoot, 'raw/tooling/direnv/shared-devenv.ts');
/** This repository's own lock, cut to the inputs the fixture names, so the fixture fetches nothing new. */
const repositoryLock = join(managedAssetsRoot, '..', '..', '..', 'tooling', 'direnv', 'devenv.lock');
const EVALUATION_TIMEOUT_MS = 1_200_000;

interface Workspace {
  readonly root: string;
  readonly env: Record<string, string>;
}

function fixtureLock(): string {
  const lock: unknown = JSON.parse(readFileSync(repositoryLock, 'utf8'));
  if (
    typeof lock !== 'object' ||
    lock === null ||
    !('nodes' in lock) ||
    typeof lock.nodes !== 'object' ||
    lock.nodes === null
  ) {
    throw new Error(`${repositoryLock} has no nodes`);
  }
  const nodes: Record<string, unknown> = { ...lock.nodes };
  const version = 'version' in lock ? lock.version : undefined;
  return `${JSON.stringify({
    nodes: {
      devenv: nodes.devenv,
      nixpkgs: nodes.nixpkgs,
      'nixpkgs-src': nodes['nixpkgs-src'],
      root: { inputs: { devenv: 'devenv', nixpkgs: 'nixpkgs' } },
    },
    root: 'root',
    version,
  })}\n`;
}

/** A workspace at its own path with its own HOME, TMPDIR and runtime directory, as a sandbox gives it. */
function workspace(scratch: string, cache: string, name: string, devenvNix: string): Workspace {
  const root = join(scratch, name);
  const devenv = join(root, 'tooling', 'direnv');
  mkdirSync(devenv, { recursive: true });
  writeFileSync(join(devenv, 'devenv.nix'), devenvNix);
  writeFileSync(join(devenv, 'devenv.yaml'), 'inputs:\n  nixpkgs:\n    url: github:cachix/devenv-nixpkgs/rolling\n');
  writeFileSync(join(devenv, 'devenv.lock'), fixtureLock());
  const env = {
    PATH: process.env.PATH ?? '',
    COWSHED_DEVENV_CACHE: cache,
    HOME: join(root, '.home'),
    TMPDIR: join(scratch, `tmp-${name}`),
    XDG_RUNTIME_DIR: join(scratch, `run-${name}`),
    XDG_DATA_HOME: join(root, '.data'),
  };
  for (const directory of [env.HOME, env.TMPDIR, env.XDG_RUNTIME_DIR, join(env.XDG_DATA_HOME, 'devenv')]) {
    mkdirSync(directory, { recursive: true });
  }
  // What the managed .envrc records before devenv starts, so devenv asks no Cachix API for its key.
  writeFileSync(
    join(env.XDG_DATA_HOME, 'devenv', 'cachix_trusted_keys.json'),
    '{\n  "devenv": "devenv.cachix.org-1:w1cLUi8dv3hnoSPGAuibQv+f9TZLr6cv/Hm9XgU50cw="\n}\n',
  );
  return { root, env };
}

function enter(ws: Workspace): { copy: string | undefined; stderr: string } {
  const result = spawnSync('bun', [program, 'enter', ws.root], { cwd: ws.root, encoding: 'utf8', env: ws.env });
  return { copy: result.status === 0 ? result.stdout.trim() : undefined, stderr: result.stderr };
}

interface Exported {
  readonly script: string;
  readonly stderr: string;
  /** The environment the export leaves after direnv-style evaluation, shell hook included. */
  readonly imported: Record<string, string>;
}

/** `export` for `devenv direnv-export`, then the export evaluated from tooling/direnv the way direnv does. */
function exportShell(ws: Workspace, copy: string): Exported {
  const devenvDir = join(ws.root, 'tooling', 'direnv');
  const exported = spawnSync('bun', [program, 'export', ws.root, copy, 'direnv-export'], {
    cwd: devenvDir,
    encoding: 'utf8',
    env: ws.env,
    maxBuffer: 64 * 1024 * 1024,
  });
  if (exported.status !== 0) printCommandOutput(exported.stdout ?? '', exported.stderr ?? '');
  expect(exported.status).toBe(0);
  const scriptFile = join(ws.env.TMPDIR ?? '', 'export.sh');
  writeFileSync(scriptFile, exported.stdout);
  const evaluated = spawnSync(
    'bash',
    ['-c', 'eval "$(cat "$1")" >/dev/null 2>&1 </dev/null; env -0', 'import', scriptFile],
    {
      cwd: devenvDir,
      encoding: 'utf8',
      env: ws.env,
      maxBuffer: 64 * 1024 * 1024,
    },
  );
  expect(evaluated.status).toBe(0);
  const imported: Record<string, string> = {};
  for (const entry of evaluated.stdout.split('\0')) {
    const separator = entry.indexOf('=');
    if (separator > 0) imported[entry.slice(0, separator)] = entry.slice(separator + 1);
  }
  return { script: exported.stdout, stderr: exported.stderr, imported };
}

/** The runtime directory devenv itself creates for a checkout: it makes it on every command, `print-paths` included. */
function devenvRuntime(ws: Workspace, scratch: string, name: string): string {
  const probe = join(scratch, `probe-run-${name}`);
  mkdirSync(probe, { recursive: true });
  const printed = spawnSync('devenv', ['print-paths'], {
    cwd: join(ws.root, 'tooling', 'direnv'),
    encoding: 'utf8',
    env: { ...ws.env, XDG_RUNTIME_DIR: probe },
  });
  expect(printed.status).toBe(0);
  const created = readdirSync(probe);
  expect(created).toHaveLength(1);
  return join(ws.env.XDG_RUNTIME_DIR ?? '', created[0] ?? '');
}

/** Nothing devenv exported, after relocation, names the shared cache; the sandbox's own pointer to it is cowshed's. */
function expectNoCachePath(exported: Exported, cache: string): void {
  expect(exported.script.includes(cache)).toBe(false);
  for (const [name, value] of Object.entries(exported.imported)) {
    if (name !== 'COWSHED_DEVENV_CACHE' && value.includes(cache)) {
      throw new Error(`${name} names the shared cache: ${value}`);
    }
  }
}

describe('shared-devenv.ts with devenv', () => {
  it(
    'gives a second workspace with identical inputs a cache hit, and moves every path onto each workspace',
    () => {
      // Short, so devenv's runtime sockets stay inside sun_path.
      const scratch = realpathSync(mkdtempSync('/tmp/smoo-sdv-'));
      try {
        const cache = join(scratch, 'cache');
        mkdirSync(cache);
        const devenvNix = '{ ... }: { env.SHARED_DEVENV_PROBE = "first"; }\n';
        const main = workspace(scratch, cache, 'main', devenvNix);
        const clone = workspace(scratch, cache, 'clone', devenvNix);

        const mainCopy = enter(main).copy ?? '';
        expect(mainCopy.startsWith(`${cache}/`)).toBe(true);
        const first = exportShell(main, mainCopy);
        expectNoCachePath(first, cache);
        expect(first.imported.SHARED_DEVENV_PROBE).toBe('first');
        expect(first.imported.DEVENV_ROOT).toBe(join(main.root, 'tooling', 'direnv'));
        expect(first.imported.DEVENV_STATE).toBe(join(main.root, 'tooling', 'direnv', '.devenv', 'state'));
        expect(first.imported.DEVENV_RUNTIME).toBe(devenvRuntime(main, scratch, 'main'));
        expect(first.imported.DEVENV_TASK_FILE).toBeUndefined();
        // What `use devenv` watches, and the GC root, are the workspace's own.
        const watched = readFileSync(join(main.root, 'tooling', 'direnv', '.devenv', 'input-paths.txt'), 'utf8');
        expect(watched).toContain(join(main.root, 'tooling', 'direnv', 'devenv.nix'));
        expect(watched.includes(cache)).toBe(false);
        expect(existsSync(join(main.root, 'tooling', 'direnv', '.devenv', 'gc', 'shell'))).toBe(true);

        expect(enter(clone).copy).toBe(mainCopy);
        const second = exportShell(clone, mainCopy);
        expect(second.stderr).toMatch(/Evaluating shell in [^\n]*\(cached\)/);
        expectNoCachePath(second, cache);
        expect(second.script.includes(main.root)).toBe(false);
        expect(second.imported.DEVENV_ROOT).toBe(join(clone.root, 'tooling', 'direnv'));
        expect(second.imported.DEVENV_RUNTIME).toBe(devenvRuntime(clone, scratch, 'clone'));

        // A workspace whose devenv.nix differs evaluates its own.
        const changed = workspace(scratch, cache, 'changed', '{ ... }: { env.SHARED_DEVENV_PROBE = "changed"; }\n');
        const changedCopy = enter(changed).copy ?? '';
        expect(changedCopy).not.toBe(mainCopy);
        expect(exportShell(changed, changedCopy).imported.SHARED_DEVENV_PROBE).toBe('changed');
      } finally {
        rmSync(scratch, { recursive: true, force: true });
      }
    },
    EVALUATION_TIMEOUT_MS,
  );

  it(
    'evaluates in place a project whose enterShell tasks write into it, and remembers why',
    () => {
      const scratch = realpathSync(mkdtempSync('/tmp/smoo-sdv-'));
      try {
        const cache = join(scratch, 'cache');
        mkdirSync(cache);
        const project = workspace(
          scratch,
          cache,
          'tasks',
          '{ ... }: { tasks."probe:setup" = { exec = "touch \\"$DEVENV_ROOT/ran-here\\""; before = [ "devenv:enterShell" ]; }; }\n',
        );
        const copy = enter(project).copy ?? '';
        // Devenv's task listing evaluates the task graph without running enterShell.
        // This is the preflight boundary before the shared copy can execute a task.
        const listed = spawnSync('devenv', ['tasks', 'list', '--json'], {
          cwd: join(copy, 'tooling', 'direnv'),
          encoding: 'utf8',
          env: {
            ...project.env,
            HOME: join(cache, 'home'),
            TMPDIR: join(cache, 'tmp'),
            XDG_RUNTIME_DIR: join(cache, 'run'),
          },
        });
        if (listed.status !== 0) printCommandOutput(listed.stdout ?? '', listed.stderr ?? '');
        expect(listed.status).toBe(0);
        expect(existsSync(join(copy, 'tooling', 'direnv', 'ran-here'))).toBe(false);
        expect(
          existsSync(join(copy, 'tooling', 'direnv', '.devenv', 'gc', 'task-config-devenv-config-task-config')),
        ).toBe(true);
        const exported = exportShell(project, copy);
        expect(exported.stderr).toContain('enterShell runs probe:setup, which write into the project they run in');
        expect(existsSync(join(copy, 'tooling', 'direnv', 'ran-here'))).toBe(false);
        expect(exported.imported.DEVENV_ROOT).toBe(join(project.root, 'tooling', 'direnv'));
        expect(existsSync(join(project.root, 'tooling', 'direnv', 'ran-here'))).toBe(true);

        // The inherited in-place graph describes the old inputs. A new,
        // side-effect-free configuration must use its own current graph.
        writeFileSync(
          join(project.root, 'tooling', 'direnv', 'devenv.nix'),
          '{ ... }: { env.SHARED_DEVENV_PROBE = "safe"; }\n',
        );
        const safe = enter(project);
        expect(safe.copy).toBeDefined();
        expect(safe.copy).not.toBe(copy);
        const shared = exportShell(project, safe.copy ?? '');
        expect(shared.imported.SHARED_DEVENV_PROBE).toBe('safe');
        expectNoCachePath(shared, cache);

        const custom = workspace(
          scratch,
          cache,
          'custom-hook',
          `{ ... }: { enterShell = ''\n  echo entered >> "$DEVENV_ROOT/hook-runs"\n''; }\n`,
        );
        const customCopy = enter(custom).copy ?? '';
        const customExport = exportShell(custom, customCopy);
        expect(existsSync(join(customCopy, 'tooling', 'direnv', 'hook-runs'))).toBe(false);
        expect(readFileSync(join(custom.root, 'tooling', 'direnv', 'hook-runs'), 'utf8')).toBe('entered\n');
        expect(customExport.imported.DEVENV_ROOT).toBe(join(custom.root, 'tooling', 'direnv'));
      } finally {
        rmSync(scratch, { recursive: true, force: true });
      }
    },
    EVALUATION_TIMEOUT_MS,
  );
});
