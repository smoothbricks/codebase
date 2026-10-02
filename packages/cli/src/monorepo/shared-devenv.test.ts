import { describe, expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import {
  chmodSync,
  existsSync,
  lstatSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  realpathSync,
  rmSync,
  symlinkSync,
  utimesSync,
  writeFileSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { managedAssetsRoot } from '@smoothbricks/nx-plugin/managed-assets';
import { printCommandOutput } from '../lib/run.js';

const program = join(managedAssetsRoot, 'raw/tooling/direnv/shared-devenv.ts');

interface Scratch {
  readonly dir: string;
  readonly cache: string;
}

function withScratch(run: (scratch: Scratch) => void): void {
  const dir = realpathSync(mkdtempSync(join(tmpdir(), 'smoo-shared-devenv-')));
  try {
    const cache = join(dir, 'caches', 'devenv');
    mkdirSync(cache, { recursive: true });
    run({ dir, cache });
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
}

/** A checkout whose devenv reads tooling/direnv and one `path:` input beside it. */
function checkout(scratch: Scratch, name: string): string {
  const root = join(scratch.dir, name);
  const devenv = join(root, 'tooling', 'direnv');
  const overlay = join(root, 'tooling', 'overlay');
  mkdirSync(join(devenv, '.devenv', 'state'), { recursive: true });
  mkdirSync(overlay, { recursive: true });
  writeFileSync(join(devenv, 'devenv.nix'), '{ ... }: { imports = [./module.nix]; }\n');
  writeFileSync(join(devenv, 'module.nix'), '{ ... }: { }\n');
  writeFileSync(join(devenv, 'devenv.lock'), '{}\n');
  writeFileSync(
    join(devenv, 'devenv.yaml'),
    'inputs:\n  overlay:\n    url: path:../../tooling/overlay\n    flake: true\n  nixpkgs:\n    url: github:cachix/devenv-nixpkgs/rolling\n',
  );
  writeFileSync(join(devenv, '.devenv', 'state', 'checkout-only'), name);
  writeFileSync(join(overlay, 'flake.nix'), '{ outputs = _: { }; }\n');
  return root;
}

function sandbox(scratch: Scratch, root: string): Record<string, string> {
  return { COWSHED_DEVENV_CACHE: scratch.cache, HOME: join(root, '.cowshed', 'home') };
}

interface Entered {
  /** The copy devenv evaluates, or undefined when the shell evaluates in place. */
  readonly copy: string | undefined;
  readonly stderr: string;
}

/** `shared-devenv.ts enter`, run from the checkout root the way the managed .envrc runs it. */
function enter(root: string, env: Record<string, string>): Entered {
  const result = spawnSync('bun', [program, 'enter', root], {
    cwd: root,
    encoding: 'utf8',
    env: { PATH: process.env.PATH ?? '', ...env },
  });
  if (result.status !== 0 && result.status !== 1) printCommandOutput(result.stdout ?? '', result.stderr ?? '');
  expect([0, 1]).toContain(result.status);
  return { copy: result.status === 0 ? result.stdout.trim() : undefined, stderr: result.stderr };
}

describe('shared-devenv.ts', () => {
  it('evaluates in place, silently, outside a cowshed sandbox', () => {
    withScratch((scratch) => {
      const root = checkout(scratch, 'host');
      const entered = enter(root, { HOME: join(scratch.dir, 'host-home') });
      expect(entered.copy).toBeUndefined();
      expect(entered.stderr).toBe('');
    });
  });

  it('enters one copy for identical inputs at different paths, and another once an input changes', () => {
    withScratch((scratch) => {
      const main = checkout(scratch, 'main');
      const clone = checkout(scratch, 'clone');
      const first = enter(main, sandbox(scratch, main)).copy;
      expect(first).toBeDefined();
      expect(enter(clone, sandbox(scratch, clone)).copy).toBe(first);

      // An evaluation input in tooling/direnv, a `path:` input, and an executable bit Nix
      // hashes into a narHash each make a new copy.
      writeFileSync(join(clone, 'tooling', 'direnv', 'module.nix'), '{ ... }: { env.CHANGED = "1"; }\n');
      const edited = enter(clone, sandbox(scratch, clone)).copy;
      expect(edited).not.toBe(first);
      writeFileSync(join(clone, 'tooling', 'overlay', 'flake.nix'), '{ outputs = _: { changed = 1; }; }\n');
      const overlayEdited = enter(clone, sandbox(scratch, clone)).copy;
      expect(overlayEdited).not.toBe(edited);
      chmodSync(join(clone, 'tooling', 'overlay', 'flake.nix'), 0o755);
      expect(enter(clone, sandbox(scratch, clone)).copy).not.toBe(overlayEdited);

      // The checkout's own .devenv never decides or enters the copy.
      writeFileSync(join(main, 'tooling', 'direnv', '.devenv', 'state', 'checkout-only'), 'changed');
      expect(enter(main, sandbox(scratch, main)).copy).toBe(first);
    });
  });

  it('copies what devenv reads in the repository layout, through symlinks, without any .devenv', () => {
    withScratch((scratch) => {
      const root = checkout(scratch, 'linked');
      const managed = join(root, 'packages', 'managed');
      mkdirSync(managed, { recursive: true });
      writeFileSync(join(managed, 'module.nix'), '{ ... }: { env.LINKED = "1"; }\n');
      rmSync(join(root, 'tooling', 'direnv', 'module.nix'));
      symlinkSync('../../packages/managed/module.nix', join(root, 'tooling', 'direnv', 'module.nix'));

      const copy = enter(root, sandbox(scratch, root)).copy ?? '';
      expect(lstatSync(join(copy, 'tooling', 'direnv', 'module.nix')).isFile()).toBe(true);
      expect(existsSync(join(copy, 'tooling', 'overlay', 'flake.nix'))).toBe(true);
      expect(existsSync(join(copy, 'tooling', 'direnv', '.devenv'))).toBe(false);

      // The link's target is what the digest covers.
      writeFileSync(join(managed, 'module.nix'), '{ ... }: { env.LINKED = "2"; }\n');
      expect(enter(root, sandbox(scratch, root)).copy).not.toBe(copy);
    });
  });

  it('keeps an entered copy from the prune that drops copies unused for 30 days', () => {
    withScratch((scratch) => {
      const kept = checkout(scratch, 'kept');
      const keptCopy = enter(kept, sandbox(scratch, kept)).copy ?? '';
      const old = new Date(Date.now() - 31 * 24 * 60 * 60 * 1000);
      utimesSync(join(keptCopy, '.used'), old, old);
      // Entering refreshes the copy before devenv runs in it.
      expect(enter(kept, sandbox(scratch, kept)).copy).toBe(keptCopy);

      const stale = checkout(scratch, 'stale');
      writeFileSync(join(stale, 'tooling', 'direnv', 'module.nix'), '{ ... }: { env.STALE = "1"; }\n');
      const staleCopy = enter(stale, sandbox(scratch, stale)).copy ?? '';
      utimesSync(join(staleCopy, '.used'), old, old);

      // Publishing a new copy prunes the stale one and keeps the one entered since.
      const fresh = checkout(scratch, 'fresh');
      writeFileSync(join(fresh, 'tooling', 'direnv', 'module.nix'), '{ ... }: { env.FRESH = "1"; }\n');
      expect(enter(fresh, sandbox(scratch, fresh)).copy).toBeDefined();
      expect(existsSync(staleCopy)).toBe(false);
      expect(existsSync(keptCopy)).toBe(true);
    });
  });

  it('keeps checkout-local configuration, HOME nixpkgs configuration and imports out of the shared copy', () => {
    withScratch((scratch) => {
      const local = checkout(scratch, 'local');
      writeFileSync(join(local, 'tooling', 'direnv', 'devenv.local.nix'), '{ ... }: { }\n');
      const withLocal = enter(local, sandbox(scratch, local));
      expect(withLocal.copy).toBeUndefined();
      expect(withLocal.stderr).toContain('devenv.local.nix exists and is not shared; evaluating in place');

      const home = checkout(scratch, 'home');
      const env = sandbox(scratch, home);
      mkdirSync(join(env.HOME ?? '', '.config', 'nixpkgs'), { recursive: true });
      const withHome = enter(home, env);
      expect(withHome.copy).toBeUndefined();
      expect(withHome.stderr).toContain('.config/nixpkgs exists and is not shared; evaluating in place');

      const imports = checkout(scratch, 'imports');
      const yaml = join(imports, 'tooling', 'direnv', 'devenv.yaml');
      writeFileSync(yaml, `imports:\n  - ../../elsewhere\n${readFileSync(yaml, 'utf8')}`);
      const withImports = enter(imports, sandbox(scratch, imports));
      expect(withImports.copy).toBeUndefined();
      expect(withImports.stderr).toContain('has imports, which are not copied; evaluating in place');
    });
  });
});
