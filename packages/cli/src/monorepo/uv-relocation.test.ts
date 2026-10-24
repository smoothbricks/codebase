import { afterEach, describe, expect, it } from 'bun:test';
import {
  cpSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  realpathSync,
  rmSync,
  symlinkSync,
  writeFileSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { join, relative, resolve } from 'node:path';
import {
  relativizeCheckoutPaths,
  uvEnvironmentRelocatable,
} from '../../../nx-plugin/managed/raw/tooling/direnv/uv-relocation.ts';

/**
 * What keeps a uv environment location-independent, measured by calling the two functions that decide it on
 * environments planted on disk. Shell entry runs them around `uv sync` (the uv installer of setup-environment.ts);
 * whether an entry creates, syncs, replaces or leaves an environment alone is that wiring's, measured through the real
 * entry in setup-environment-uv.test.ts. What is measured here is each verdict: which environments an install admits
 * and which name an environment or a checkout that is not this one, and how each `.pth` line is rewritten. `uv` never
 * runs: what it leaves, an environment and the editable member's `.pth` line, is written out.
 */

/** The workspace member `uv sync` installs editable: its sources, relative to the checkout. */
const MEMBER = 'python/tool/src';
/** Where devenv puts the environment in a checkout, and its python's site-packages, directory by directory. */
const VENV = ['tooling', 'direnv', '.devenv', 'state', 'venv'];
const SITE = [...VENV, 'lib', 'python3.14', 'site-packages'];
/** The member's `.pth` line once rewritten: up from site-packages to the checkout, then down to the sources. */
const RELATIVE_MEMBER = [...SITE.map(() => '..'), MEMBER].join('/');

interface Layout {
  /** The scratch directory: the checkout, and beside it what an old checkout or environment left behind. */
  readonly scratch: string;
  readonly root: string;
  readonly venv: string;
  readonly site: string;
}

const created: string[] = [];
afterEach(() => {
  for (const directory of created.splice(0)) rmSync(directory, { recursive: true, force: true });
});

/**
 * A checkout whose environment `uv venv --relocatable` and `uv sync` have just left: a relocatable config, an empty
 * `bin`, `_virtualenv.pth`, and the editable member as the absolute path of its sources.
 */
function synced(): Layout {
  const scratch = realpathSync(mkdtempSync(join(tmpdir(), 'uv-relocation-')));
  created.push(scratch);
  const root = join(scratch, 'checkout');
  const venv = join(root, ...VENV);
  const site = join(root, ...SITE);
  mkdirSync(join(root, MEMBER, 'tool'), { recursive: true });
  mkdirSync(join(venv, 'bin'), { recursive: true });
  mkdirSync(site, { recursive: true });
  writeFileSync(join(venv, 'pyvenv.cfg'), 'home = /nix/store/python/bin\nrelocatable = true\n');
  writeFileSync(join(site, '_virtualenv.pth'), 'import _virtualenv\n');
  writeFileSync(join(site, 'tool.pth'), `${join(root, MEMBER)}\n`);
  return { scratch, root, venv, site };
}

/** What an install leaves: the synced environment after the rewrite, the member's line relative to site-packages. */
function installed(): Layout {
  const layout = synced();
  writeFileSync(join(layout.site, 'tool.pth'), `${RELATIVE_MEMBER}\n`);
  return layout;
}

/** The interpreter of a virtualenv another checkout left behind; its pyvenv.cfg is what marks the directory as one. */
function oldEnvironment(directory: string): string {
  mkdirSync(join(directory, 'bin'), { recursive: true });
  writeFileSync(join(directory, 'pyvenv.cfg'), 'home = /nix/store/python/bin\n');
  writeFileSync(join(directory, 'bin', 'python'), '');
  return join(directory, 'bin', 'python');
}

const foreign = (scratch: string): string => oldEnvironment(join(scratch, 'foreign-venv'));
const spaced = (scratch: string): string => oldEnvironment(join(scratch, 'old checkout', 'foreign venv'));

/** The interpreter of an old checkout's devenv profile. */
function oldProfile(scratch: string): string {
  const bin = join(scratch, 'old-checkout', '.devenv', 'profile', 'bin');
  mkdirSync(bin, { recursive: true });
  writeFileSync(join(bin, 'python'), '');
  return join(bin, 'python');
}

/** A console script in the environment's `bin`. */
const launcher = (venv: string, content: string): void => writeFileSync(join(venv, 'bin', 'pytest'), content);

interface Case {
  readonly name: string;
  /** Turns the installed environment into the layout under test. */
  readonly plant: (layout: Layout) => void;
}

/**
 * Layouts the witness must reject, each planted alone on an environment that is admitted until then, so the verdict is
 * the escape's: a launcher, an interpreter link or a `.pth` line naming an environment or a checkout other than this
 * one, with the config valid throughout, and an environment that is not the relocatable one an install leaves.
 */
const ESCAPES: readonly Case[] = [
  {
    name: 'a launcher naming a foreign old root',
    plant: ({ scratch, venv }) => launcher(venv, `#!${foreign(scratch)}\n`),
  },
  {
    name: 'a launcher naming this environment absolutely',
    plant: ({ venv }) => launcher(venv, `#!${join(venv, 'bin', 'python')}\n`),
  },
  {
    name: 'an absolute .pth line',
    plant: ({ root, site }) => writeFileSync(join(site, 'tool.pth'), `${join(root, MEMBER)}\n`),
  },
  {
    name: 'a launcher naming an old checkout devenv profile interpreter',
    plant: ({ scratch, venv }) => launcher(venv, `#!${oldProfile(scratch)}\n`),
  },
  {
    name: 'a shell wrapper execing an old venv',
    plant: ({ scratch, venv }) => launcher(venv, `#!/bin/sh\nexec "${foreign(scratch)}" "$@"\n`),
  },
  {
    name: 'a whitespace-shebang launcher naming an old venv',
    plant: ({ scratch, venv }) => launcher(venv, `#! ${foreign(scratch)}\n`),
  },
  {
    name: 'a shell wrapper execing a spaced old venv path',
    plant: ({ scratch, venv }) => launcher(venv, `#!/bin/sh\nexec "${spaced(scratch)}" "$@"\n`),
  },
  {
    name: 'an env -S launcher naming a spaced old venv path',
    plant: ({ scratch, venv }) => launcher(venv, `#!/usr/bin/env -S "${spaced(scratch)}"\n`),
  },
  {
    name: 'a polyglot wrapper execing a spaced old venv path',
    plant: ({ scratch, venv }) => launcher(venv, `#!/bin/sh\n'''exec' "${spaced(scratch)}" "$0" "$@"\n' '''\n`),
  },
  {
    name: 'a relative interpreter alias resolving into an old checkout profile',
    plant: ({ scratch, venv }) =>
      symlinkSync(relative(join(venv, 'bin'), oldProfile(scratch)), join(venv, 'bin', 'python')),
  },
  {
    name: 'an environment interpreter that is a symlink into an old checkout profile',
    plant: ({ scratch, venv }) => symlinkSync(oldProfile(scratch), join(venv, 'bin', 'python')),
  },
  {
    name: 'a config that does not say relocatable',
    plant: ({ venv }) => writeFileSync(join(venv, 'pyvenv.cfg'), 'home = /nix/store/python/bin\n'),
  },
  {
    name: 'a legacy environment: no relocatable config, a launcher and a .pth line naming the old absolute home',
    plant: ({ root, venv, site }) => {
      writeFileSync(join(venv, 'pyvenv.cfg'), 'home = /nix/store/python/bin\n');
      launcher(venv, `#!${join(venv, 'bin', 'python')}\n`);
      writeFileSync(join(site, 'tool.pth'), `${join(root, MEMBER)}\n`);
    },
  },
  {
    name: 'an environment that does not exist',
    plant: ({ venv }) => rmSync(venv, { recursive: true }),
  },
];

/**
 * Layouts that hold links and launchers and are relocatable all the same: relative aliases stay in the environment, a
 * store interpreter names no environment or checkout, and a wrapper that locates itself names no path at all.
 */
const ADMITTED: readonly Case[] = [
  {
    name: 'a constructed relocatable layout — relative interpreter aliases, a store interpreter, a self-locating wrapper',
    plant: ({ scratch, venv }) => {
      const store = join(scratch, 'store', 'bin');
      mkdirSync(store, { recursive: true });
      writeFileSync(join(store, 'python'), '');
      symlinkSync(join(store, 'python'), join(venv, 'bin', 'python'));
      symlinkSync('python', join(venv, 'bin', 'python3'));
      symlinkSync('python', join(venv, 'bin', 'python3.14'));
      launcher(venv, '#!/bin/sh\nexec "$(dirname "$0")/python" "$@"\n');
    },
  },
];

describe('uvEnvironmentRelocatable', () => {
  it('admits what an install leaves', () => {
    expect(uvEnvironmentRelocatable(installed().venv)).toBe(true);
  });

  it.each(ESCAPES)('rejects $name', ({ plant }) => {
    const layout = installed();
    expect(uvEnvironmentRelocatable(layout.venv)).toBe(true);
    plant(layout);
    expect(uvEnvironmentRelocatable(layout.venv)).toBe(false);
  });

  it.each(ADMITTED)('admits $name', ({ plant }) => {
    const layout = installed();
    plant(layout);
    expect(uvEnvironmentRelocatable(layout.venv)).toBe(true);
  });
});

interface Rewrite {
  readonly name: string;
  /** Plants what the lines name, and returns the `.pth` file as `uv sync` wrote it and as the rewrite must leave it. */
  readonly pth: (layout: Layout) => { readonly before: string; readonly after: string };
}

const REWRITES: readonly Rewrite[] = [
  {
    name: 'a line naming a directory in the checkout becomes a path relative to site-packages',
    pth: ({ root }) => ({ before: `${join(root, MEMBER)}\n`, after: `${RELATIVE_MEMBER}\n` }),
  },
  {
    name: 'an import line stays as uv wrote it',
    pth: () => ({ before: 'import _virtualenv\n', after: 'import _virtualenv\n' }),
  },
  {
    name: "a directory outside the checkout stays absolute, even one whose path begins with the checkout's",
    pth: ({ root }) => {
      const sibling = join(`${root}-old`, MEMBER);
      mkdirSync(sibling, { recursive: true });
      return { before: `${sibling}\n`, after: `${sibling}\n` };
    },
  },
  {
    name: 'a directory that does not exist stays as uv wrote it',
    pth: ({ root }) => {
      const missing = join(root, 'python', 'gone', 'src');
      return { before: `${missing}\n`, after: `${missing}\n` };
    },
  },
  {
    name: 'each line of a file is judged alone',
    pth: ({ root }) => {
      const missing = join(root, 'python', 'gone', 'src');
      return {
        before: `import _virtualenv\n${join(root, MEMBER)}\n${missing}\n`,
        after: `import _virtualenv\n${RELATIVE_MEMBER}\n${missing}\n`,
      };
    },
  },
];

describe('relativizeCheckoutPaths', () => {
  it.each(REWRITES)('$name', ({ pth }) => {
    const layout = synced();
    const { before, after } = pth(layout);
    const file = join(layout.site, 'tool.pth');
    writeFileSync(file, before);
    relativizeCheckoutPaths(layout.venv, layout.root);
    expect(readFileSync(file, 'utf8')).toBe(after);
  });

  it('rewrites every .pth file of every python in lib, and nothing else', () => {
    const layout = synced();
    const absolute = `${join(layout.root, MEMBER)}\n`;
    const other = join(layout.venv, 'lib', 'python3.13', 'site-packages');
    mkdirSync(other, { recursive: true });
    writeFileSync(join(other, 'tool.pth'), absolute);
    writeFileSync(join(other, 'tool.txt'), absolute);
    relativizeCheckoutPaths(layout.venv, layout.root);
    expect(readFileSync(join(layout.site, 'tool.pth'), 'utf8')).toBe(`${RELATIVE_MEMBER}\n`);
    expect(readFileSync(join(other, 'tool.pth'), 'utf8')).toBe(`${RELATIVE_MEMBER}\n`);
    expect(readFileSync(join(other, 'tool.txt'), 'utf8')).toBe(absolute);
  });

  it('turns what uv sync wrote into an environment the witness admits, here and in a copy of the checkout', () => {
    const layout = synced();
    expect(uvEnvironmentRelocatable(layout.venv)).toBe(false);
    relativizeCheckoutPaths(layout.venv, layout.root);
    expect(uvEnvironmentRelocatable(layout.venv)).toBe(true);

    // A copy-on-write clone: every byte, at another path. Python's site resolves a relative .pth line against
    // site-packages, so the member's line resolves to the copy's sources.
    const clone = join(layout.scratch, 'clone');
    cpSync(layout.root, clone, { recursive: true, verbatimSymlinks: true });
    expect(uvEnvironmentRelocatable(join(clone, ...VENV))).toBe(true);
    const site = join(clone, ...SITE);
    expect(resolve(site, readFileSync(join(site, 'tool.pth'), 'utf8').trim())).toBe(join(clone, MEMBER));
    expect(readFileSync(join(site, '_virtualenv.pth'), 'utf8')).toBe('import _virtualenv\n');
  });
});
