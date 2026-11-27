import { describe, expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { join, resolve } from 'node:path';
import typia from 'typia';
import { printCommandOutput } from '../lib/run.js';

const REPO_ROOT = resolve(import.meta.dir, '..', '..', '..', '..');
const DEVENV_LOCK = join(REPO_ROOT, 'tooling/direnv/devenv.lock');
const MANAGED_DEVENV_MODULE = join(REPO_ROOT, 'packages/nx-plugin/managed/raw/tooling/direnv/devenv.smoo.nix');

interface ManagedBunOverlayEval {
  readonly plainBunIsSmooBun: boolean;
  readonly smooBunVersion: string;
  readonly ownBunIsItsOwn: boolean;
  readonly ownSmooBunIsTheManagedOne: boolean;
}

type EvalOutcome =
  | { readonly ok: true; readonly value: ManagedBunOverlayEval }
  | { readonly ok: false; readonly error: Error };

function nixString(value: string): string {
  return JSON.stringify(value).replaceAll('${', '\\${');
}

function resolveNix(): string {
  const nix = Bun.which('nix');
  if (nix === null) {
    const path = process.env.PATH ?? '<unset>';
    throw new Error(
      `nix must be on PATH (the devenv shell provides it) to evaluate the managed Bun overlay; PATH=${path}`,
    );
  }
  return nix;
}

function readPinnedSmooBunVersion(): string {
  const source = readFileSync(MANAGED_DEVENV_MODULE, 'utf8');
  const match = /\bsmooBun\s*=\s*[^;]*?\bversion\s*=\s*"([^"]+)";/.exec(source);
  if (match?.[1] === undefined) {
    throw new Error(`could not read the pinned smooBun version from ${MANAGED_DEVENV_MODULE}`);
  }
  return match[1];
}

function runManagedBunOverlayEval(): ManagedBunOverlayEval {
  const nix = resolveNix();
  const expr = `
let
  lock = builtins.fromJSON (builtins.readFile ${nixString(DEVENV_LOCK)});
  src = lock.nodes.\${lock.nodes.root.inputs.nixpkgs}.inputs."nixpkgs-src";
  nixpkgs = builtins.fetchTree lock.nodes.\${src}.locked;
  lib = import (nixpkgs + "/lib");
  managed = (import ${nixString(MANAGED_DEVENV_MODULE)} {
    config = {}; inputs = {}; inherit lib; pkgs = null;
  }).overlays;
  plain = import nixpkgs { overlays = managed; };
  own = import nixpkgs { overlays = [ (_: prev: { bun = prev.hello; }) ] ++ managed; };
in {
  plainBunIsSmooBun = plain.bun.drvPath == plain.smooBun.drvPath;
  smooBunVersion = plain.smooBun.version;
  ownBunIsItsOwn = own.bun.drvPath == own.hello.drvPath;
  ownSmooBunIsTheManagedOne = own.smooBun.drvPath == plain.smooBun.drvPath;
}
`;
  const result = spawnSync(nix, ['eval', '--json', '--impure', '--expr', expr], {
    encoding: 'utf8',
    timeout: 120_000,
  });
  if (result.error) {
    printCommandOutput(result.stdout ?? '', result.stderr ?? '');
    throw new Error(`nix eval failed: ${result.error.message}`);
  }
  if (result.status !== 0) {
    printCommandOutput(result.stdout ?? '', result.stderr ?? '');
    throw new Error(`nix eval exited with status ${String(result.status)} (signal ${String(result.signal)})`);
  }
  return typia.assertEquals<ManagedBunOverlayEval>(JSON.parse(result.stdout));
}

let cachedEval: EvalOutcome | undefined;

function evaluateManagedBunOverlay(): ManagedBunOverlayEval {
  if (cachedEval === undefined) {
    try {
      cachedEval = { ok: true, value: runManagedBunOverlayEval() };
    } catch (error) {
      cachedEval = { ok: false, error: error instanceof Error ? error : new Error(String(error)) };
    }
  }
  if (!cachedEval.ok) {
    throw cachedEval.error;
  }
  return cachedEval.value;
}

describe('managed Bun overlay (devenv.smoo.nix)', () => {
  it("a repository that keeps nixpkgs' bun gets the managed canary as its bun, one store path with smooBun", () => {
    const evaluated = evaluateManagedBunOverlay();
    expect(evaluated.plainBunIsSmooBun).toBe(true);
    expect(evaluated.smooBunVersion).toBe(readPinnedSmooBunVersion());
  });

  it('a repository whose earlier overlay replaced bun keeps its own bun', () => {
    const evaluated = evaluateManagedBunOverlay();
    expect(evaluated.ownBunIsItsOwn).toBe(true);
  });

  it('smooBun is the managed canary whatever an earlier overlay made of bun', () => {
    const evaluated = evaluateManagedBunOverlay();
    expect(evaluated.ownSmooBunIsTheManagedOne).toBe(true);
  });
});
