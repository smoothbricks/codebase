import { afterAll, beforeAll, describe, expect, it, spyOn } from 'bun:test';
import { existsSync, mkdtempSync, readFileSync, readlinkSync, realpathSync, symlinkSync } from 'node:fs';
import { chmod, cp, mkdir, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join, relative } from 'node:path';
import {
  applyDeclaredLinks,
  type DeclaredLinksPlan,
  keepDeveloperLinks,
  planDeclaredLinks,
  type UnavailableDeclaredLink,
} from '../../../nx-plugin/managed/raw/tooling/direnv/developer-links.ts';
import { linkEntry } from './__tests__/helpers/shell-entry.js';

/**
 * What `smoo.developerLinks` and an install do to the node_modules trees of a checkout, called directly. Each tree is
 * what `bun install` leaves under the isolated linker, planted by hand: the lockfile naming the workspace members and
 * each member's dependencies linked by relative path. `--links` is `applyDeclaredLinks(planDeclaredLinks())` while no
 * Nx or store link is stale (setup-environment.ts), and the shell-entry suite (setup-environment-links.test.ts) runs
 * that entry, the root postinstall and a real install end to end.
 */

// Three workspace members: the install links two of them into `app`'s node_modules, `@fixture/lib` under a scope and
// `util` without one.
const APP = 'packages/app';
const LINKED = `${APP}/node_modules/@fixture/lib`;
const UNLINKED = `${APP}/node_modules/util`;

interface Workspace {
  /** Each member directory and its package name. */
  readonly members: Readonly<Record<string, string>>;
  /** What an install puts at each dependency entry: the workspace member, by relative path. */
  readonly lockfile: Readonly<Record<string, string>>;
}

const WORKSPACE: Workspace = {
  members: { [APP]: 'app', 'packages/lib': '@fixture/lib', 'packages/util': 'util' },
  lockfile: { [LINKED]: '../../../lib', [UNLINKED]: '../../util' },
};

/** A plan that changes nothing and says nothing: `--links --check` exits 0 on it without a word. */
const CURRENT: DeclaredLinksPlan = { changes: [], unavailable: [], reports: [] };

/** The root manifest, declaring `developerLinks` when given. */
function manifest(developerLinks: Readonly<Record<string, string>> | undefined): string {
  return JSON.stringify({
    name: 'fixture',
    version: '0.0.0',
    private: true,
    workspaces: ['packages/*'],
    ...(developerLinks === undefined ? {} : { smoo: { developerLinks } }),
  });
}

/** Changes the root's `smoo.developerLinks`, or drops it. */
async function declare(root: string, developerLinks: Readonly<Record<string, string>> | undefined): Promise<void> {
  await writeFile(join(root, 'package.json'), manifest(developerLinks));
}

/** A checkout `workspace` installed into, beside the directories its declarations may name. */
async function withInstalled(
  workspace: Workspace,
  developerLinks: Readonly<Record<string, string>> | undefined,
  run: (root: string) => Promise<void>,
): Promise<void> {
  const scratch = realpathSync(mkdtempSync(join(tmpdir(), 'developer-links-declared-')));
  const root = join(scratch, 'checkout');
  try {
    await mkdir(join(root, 'node_modules', '.bun'), { recursive: true });
    await writeFile(join(root, 'package.json'), manifest(developerLinks));
    const members = Object.keys(workspace.members);
    await writeFile(
      join(root, 'bun.lock'),
      JSON.stringify({ lockfileVersion: 1, workspaces: Object.fromEntries(['', ...members].map((m) => [m, {}])) }),
    );
    for (const [member, name] of Object.entries(workspace.members)) await checkout(join(root, member), name);
    for (const [entry, target] of Object.entries(workspace.lockfile)) await linkEntry(root, entry, target);
    await run(root);
  } finally {
    await rm(scratch, { recursive: true, force: true });
  }
}

/** A local checkout of a package: a directory holding its manifest. */
async function checkout(at: string, name: string): Promise<string> {
  await mkdir(at, { recursive: true });
  await writeFile(join(at, 'package.json'), JSON.stringify({ name, version: '9.9.9' }));
  return at;
}

/** What `run` says on stderr, a line per `console.error`. */
async function saying(run: () => Promise<unknown>): Promise<string> {
  let said = '';
  const error = spyOn(console, 'error').mockImplementation((...args: unknown[]) => {
    said += `${args.map(String).join(' ')}\n`;
  });
  try {
    await run();
  } finally {
    error.mockRestore();
  }
  return said;
}

/** The `--links` shell task over current Nx and store links, and the root postinstall: plan, then apply. */
function link(root: string): Promise<string> {
  return saying(async () => applyDeclaredLinks(root, await planDeclaredLinks(root)));
}

/**
 * A shell entry's install: `bun install` re-points every dependency entry at the lockfile's version, then runs the
 * root postinstall, inside `keepDeveloperLinks` as setup-environment.ts runs it.
 */
function install(root: string, workspace: Workspace = WORKSPACE): Promise<string> {
  return saying(() =>
    keepDeveloperLinks(
      root,
      async () => {
        for (const [entry, target] of Object.entries(workspace.lockfile)) await linkEntry(root, entry, target);
        applyDeclaredLinks(root, await planDeclaredLinks(root));
      },
      { relink: ['nx'] },
    ),
  );
}

/** The finding for `@fixture/lib` declared at `target`, which cannot be linked to for `reason`. */
function unavailable(target: string, reason: string): UnavailableDeclaredLink {
  return { kind: 'absent', name: '@fixture/lib', target, reason };
}

describe('as smoo.developerLinks declares them', () => {
  // A developer machine: a CI runner links nothing, and both keepDeveloperLinks and the postinstall read this.
  const runner = process.env.GITHUB_ACTIONS;
  beforeAll(() => {
    delete process.env.GITHUB_ACTIONS;
  });
  afterAll(() => {
    if (runner !== undefined) process.env.GITHUB_ACTIONS = runner;
  });

  it('links each installed declared package beside the main checkout a cowshed marker names', async () => {
    await withInstalled(WORKSPACE, { '@fixture/lib': '../local-lib' }, async (root) => {
      await install(root);
      // Genuine sibling declarations share the main anchor without reading its tree.
      const main = join(dirname(root), 'projects', 'main');
      await mkdir(main, { recursive: true });
      await mkdir(join(root, '.cowshed'), { recursive: true });
      await writeFile(join(root, '.cowshed', 'workspace.json'), JSON.stringify({ projectRoot: main }));
      const local = await checkout(join(dirname(root), 'projects', 'local-lib'), '@fixture/lib');

      expect(await planDeclaredLinks(root)).toEqual({
        ...CURRENT,
        changes: [{ path: LINKED, binding: { kind: 'checkout', target: local } }],
      });
      expect(await link(root)).toBe(`linked declared packages to local checkouts: @fixture/lib -> ${local}\n`);
      expect(readlinkSync(join(root, LINKED))).toBe(local);
      expect(readlinkSync(join(root, UNLINKED))).toBe('../../util');
      // The root depends on neither, so its tree gains nothing.
      expect(existsSync(join(root, 'node_modules', '@fixture', 'lib'))).toBe(false);
      expect(await planDeclaredLinks(root)).toEqual(CURRENT);

      // An install that reinstalls the package keeps the link.
      await install(root);
      expect(readlinkSync(join(root, LINKED))).toBe(local);
      expect(await planDeclaredLinks(root)).toEqual(CURRENT);
    });
  });

  it('keeps an inherited absolute sibling link even when the declaration cannot resolve at this checkout depth', async () => {
    await withInstalled(WORKSPACE, { '@fixture/lib': '../provider/lib' }, async (root) => {
      await install(root);
      const sibling = await checkout(join(dirname(root), 'donor-side/provider/lib'), '@fixture/lib');
      await linkEntry(root, LINKED, sibling);
      await link(root);
      expect(readlinkSync(join(root, LINKED))).toBe(sibling);
      await install(root);
      expect(readlinkSync(join(root, LINKED))).toBe(sibling);
    });
  });

  it('selects contained package directories in this checkout, not the main checkout', async () => {
    await withInstalled(WORKSPACE, { '@fixture/lib': '.cache/owned-package' }, async (root) => {
      await install(root);
      expect(readlinkSync(join(root, LINKED))).toBe('../../../lib');
      const main = join(dirname(root), 'projects', 'main');
      await checkout(join(main, '.cache', 'owned-package'), '@fixture/lib');
      const legacy = await checkout(join(main, '.cache', 'package'), '@fixture/lib');
      await mkdir(join(root, '.cowshed'), { recursive: true });
      await writeFile(join(root, '.cowshed', 'workspace.json'), JSON.stringify({ projectRoot: main }));

      await linkEntry(root, LINKED, legacy);
      // A missing local directory refuses an inherited Main binding, without rewriting it.
      await expect(planDeclaredLinks(root)).rejects.toThrow(
        'restore it in this checkout, never borrow the main checkout',
      );
      expect(readlinkSync(join(root, LINKED))).toBe(legacy);
      const local = await checkout(join(root, '.cache', 'owned-package'), '@fixture/lib');
      // The inherited Main payload need not share the declaration's new relative path.
      expect(await planDeclaredLinks(root)).toEqual({
        ...CURRENT,
        changes: [{ path: LINKED, binding: { kind: 'checkout', target: local } }],
      });
      await link(root);
      expect(readlinkSync(join(root, LINKED))).toBe(relative(dirname(join(root, LINKED)), local));
      expect(readFileSync(join(legacy, 'package.json'), 'utf8')).toContain('@fixture/lib');
      expect(await planDeclaredLinks(root)).toEqual(CURRENT);
      // A genuinely unrelated checkout remains the developer's independent choice.
      const independent = await checkout(join(dirname(root), 'independent-lib'), '@fixture/lib');
      await linkEntry(root, LINKED, independent);
      expect(await planDeclaredLinks(root)).toEqual(CURRENT);
      await link(root);
      expect(readlinkSync(join(root, LINKED))).toBe(independent);
    });
  });

  it('refuses a contained declaration whose existing ancestor escapes through a symlink', async () => {
    await withInstalled(WORKSPACE, { '@fixture/lib': '.cache/owned-package' }, async (root) => {
      await install(root);
      const foreign = join(dirname(root), 'foreign-packages');
      await mkdir(foreign);
      symlinkSync(foreign, join(root, '.cache'));
      // The final component does not exist: the existing ancestor still proves the escape.
      await expect(link(root)).rejects.toThrow('escapes its owning checkout');
      expect(readlinkSync(join(root, LINKED))).toBe('../../../lib');
    });
  });

  it("keeps a developer's own link and diagnoses an absent declared checkout on application", async () => {
    await withInstalled(WORKSPACE, { '@fixture/lib': '../absent-lib', util: '../declared-util' }, async (root) => {
      await install(root);
      await checkout(join(dirname(root), 'declared-util'), 'util');
      const mine = await checkout(join(dirname(root), 'my-util'), 'util');
      await linkEntry(root, UNLINKED, mine);
      const absent = {
        ...CURRENT,
        unavailable: [unavailable(join(dirname(root), 'absent-lib'), 'does not exist on this machine')],
      };
      expect(await planDeclaredLinks(root)).toEqual(absent);
      expect(await link(root)).toContain(
        `! smoo.developerLinks @fixture/lib: ${join(dirname(root), 'absent-lib')} does not exist on this machine; the installed version stays`,
      );
      expect(readlinkSync(join(root, LINKED))).toBe('../../../lib');
      expect(await planDeclaredLinks(root)).toEqual(absent);
      expect(readlinkSync(join(root, UNLINKED))).toBe(mine);
    });
  });

  it('replaces an old declared binding when the declaration changes to another checkout', async () => {
    await withInstalled(WORKSPACE, { '@fixture/lib': '../lib-a' }, async (root) => {
      await install(root);
      const a = await checkout(join(dirname(root), 'lib-a'), '@fixture/lib');
      const b = await checkout(join(dirname(root), 'lib-b'), '@fixture/lib');
      await link(root);
      expect(readlinkSync(join(root, LINKED))).toBe(a);

      await declare(root, { '@fixture/lib': '../lib-b' });
      expect(await planDeclaredLinks(root)).toEqual({
        ...CURRENT,
        changes: [{ path: LINKED, binding: { kind: 'checkout', target: b } }],
      });
      await link(root);
      expect(readlinkSync(join(root, LINKED))).toBe(b);
    });
  });

  it('keeps an absent declared checkout as an unavailable finding, leaving the existing absolute binding bytes unchanged', async () => {
    await withInstalled(WORKSPACE, { '@fixture/lib': '../absent-lib' }, async (root) => {
      await install(root);
      const sibling = await checkout(join(dirname(root), 'donor-side/lib'), '@fixture/lib');
      await linkEntry(root, LINKED, sibling);
      expect(await planDeclaredLinks(root)).toEqual({
        ...CURRENT,
        unavailable: [unavailable(join(dirname(root), 'absent-lib'), 'does not exist on this machine')],
      });
      expect(await link(root)).toContain('does not exist on this machine');
      expect(readlinkSync(join(root, LINKED))).toBe(sibling);
    });
  });

  it('reports a present checkout without a package.json as unavailable, never as an unhandled ENOENT', async () => {
    await withInstalled(WORKSPACE, { '@fixture/lib': '../bare-lib' }, async (root) => {
      await install(root);
      await mkdir(join(dirname(root), 'bare-lib'), { recursive: true });
      const said = await link(root);
      expect(said).toContain('has no package.json here');
      expect(said).not.toContain('ENOENT');
      expect(await planDeclaredLinks(root)).toEqual({
        ...CURRENT,
        unavailable: [unavailable(join(dirname(root), 'bare-lib'), 'has no package.json here')],
      });
    });
  });

  it('refuses a declaration whose directory is absolute', async () => {
    await withInstalled(WORKSPACE, { '@fixture/lib': '/opt/lib' }, async (root) => {
      await expect(planDeclaredLinks(root)).rejects.toThrow(
        'smoo.developerLinks @fixture/lib must be a directory relative to the main checkout, not "/opt/lib"',
      );
    });
  });

  it('refuses a declared Nx link and leaves both Nx bindings where the install put them', async () => {
    // `app` and the root both depend on the workspace member `nx`, bound by relative path as `util` is.
    const WITH_NX: Workspace = {
      members: { ...WORKSPACE.members, 'packages/nx': 'nx' },
      lockfile: { ...WORKSPACE.lockfile, 'node_modules/nx': '../packages/nx', [`${APP}/node_modules/nx`]: '../../nx' },
    };
    // A declared Nx checkout would repoint Bun's binding in postinstall, so smoo refuses the declaration by name.
    await withInstalled(WITH_NX, { nx: '.cache/retired-nx' }, async (root) => {
      const bindings = () => [
        readlinkSync(join(root, 'node_modules/nx')),
        readlinkSync(join(root, APP, 'node_modules/nx')),
      ];
      const obsolete = /smoo\.developerLinks\.nx .*smoo monorepo update/;
      await expect(planDeclaredLinks(root)).rejects.toThrow(obsolete);
      expect(bindings()).toEqual(['../packages/nx', '../../nx']);
      // The install's postinstall reads the same declaration; it must not repoint Nx either.
      await expect(install(root, WITH_NX)).rejects.toThrow(obsolete);
      expect(bindings()).toEqual(['../packages/nx', '../../nx']);
    });
  });

  it('reconciles a changed managed declaration and preserves a manual retarget', async () => {
    await withInstalled(WORKSPACE, { '@fixture/lib': '../first-lib', util: '../first-util' }, async (root) => {
      const first = await checkout(join(dirname(root), 'first-lib'), '@fixture/lib');
      await checkout(join(dirname(root), 'first-util'), 'util');
      await install(root);
      expect(readlinkSync(join(root, LINKED))).toBe(first);
      const mine = await checkout(join(dirname(root), 'my-util'), 'util');
      await linkEntry(root, UNLINKED, mine);
      const next = await checkout(join(dirname(root), 'next-lib'), '@fixture/lib');
      await checkout(join(dirname(root), 'next-util'), 'util');
      await declare(root, { '@fixture/lib': '../next-lib', util: '../next-util' });

      expect(await planDeclaredLinks(root)).toEqual({
        ...CURRENT,
        changes: [{ path: LINKED, binding: { kind: 'checkout', target: next } }],
      });
      await link(root);
      expect(readlinkSync(join(root, LINKED))).toBe(next);
      expect(readlinkSync(join(root, UNLINKED))).toBe(mine);
      expect(await planDeclaredLinks(root)).toEqual(CURRENT);
      await link(root);
      expect(readlinkSync(join(root, UNLINKED))).toBe(mine);
    });
  });

  it('keeps changed managed declarations after an install instead of restoring the previous target', async () => {
    await withInstalled(WORKSPACE, { '@fixture/lib': '../first-lib', util: '../declared-util' }, async (root) => {
      await checkout(join(dirname(root), 'first-lib'), '@fixture/lib');
      await checkout(join(dirname(root), 'declared-util'), 'util');
      await install(root);
      const mine = await checkout(join(dirname(root), 'my-util'), 'util');
      await linkEntry(root, UNLINKED, mine);
      const next = await checkout(join(dirname(root), 'next-lib'), '@fixture/lib');
      await declare(root, { '@fixture/lib': '../next-lib', util: '../declared-util' });

      await install(root);
      expect(readlinkSync(join(root, LINKED))).toBe(next);
      expect(readlinkSync(join(root, UNLINKED))).toBe(mine);
      await link(root);
      expect(readlinkSync(join(root, UNLINKED))).toBe(mine);
    });
  });

  it('does not claim a pre-existing manual link just because it matches a declaration', async () => {
    await withInstalled(WORKSPACE, { '@fixture/lib': '../first-lib' }, async (root) => {
      await install(root);
      const first = await checkout(join(dirname(root), 'first-lib'), '@fixture/lib');
      await linkEntry(root, LINKED, first);
      await link(root);
      await checkout(join(dirname(root), 'next-lib'), '@fixture/lib');
      await declare(root, { '@fixture/lib': '../next-lib' });
      expect(await planDeclaredLinks(root)).toEqual(CURRENT);
      await link(root);
      expect(readlinkSync(join(root, LINKED))).toBe(first);
    });
  });

  it('carries managed provenance into a copied checkout for sibling and contained declarations', async () => {
    await withInstalled(WORKSPACE, { '@fixture/lib': '../first-lib', util: '.cache/first-util' }, async (original) => {
      const first = await checkout(join(dirname(original), 'first-lib'), '@fixture/lib');
      const firstUtil = await checkout(join(original, '.cache', 'first-util'), 'util');
      await install(original);
      const clone = join(dirname(original), 'clone');
      await cp(original, clone, { recursive: true, verbatimSymlinks: true });
      await mkdir(join(clone, '.cowshed'), { recursive: true });
      await writeFile(join(clone, '.cowshed', 'workspace.json'), JSON.stringify({ projectRoot: original }));
      const next = await checkout(join(dirname(original), 'next-lib'), '@fixture/lib');
      const nextUtil = await checkout(join(clone, '.cache', 'next-util'), 'util');
      await declare(clone, { '@fixture/lib': '../next-lib', util: '.cache/next-util' });

      expect(await planDeclaredLinks(clone)).toEqual({
        ...CURRENT,
        changes: [
          { path: LINKED, binding: { kind: 'checkout', target: next } },
          { path: UNLINKED, binding: { kind: 'checkout', target: nextUtil } },
        ],
      });
      await link(clone);
      expect(realpathSync(join(clone, LINKED))).toBe(next);
      expect(realpathSync(join(clone, UNLINKED))).toBe(nextUtil);
      expect(await planDeclaredLinks(clone)).toEqual(CURRENT);
      expect(realpathSync(join(original, LINKED))).toBe(first);
      expect(realpathSync(join(original, UNLINKED))).toBe(firstUtil);
    });
  });

  it('removes an undeclared managed link at the next install and preserves a manual retarget', async () => {
    await withInstalled(WORKSPACE, { '@fixture/lib': '../local-lib', util: '../declared-util' }, async (root) => {
      await checkout(join(dirname(root), 'local-lib'), '@fixture/lib');
      await checkout(join(dirname(root), 'declared-util'), 'util');
      await install(root);
      const mine = await checkout(join(dirname(root), 'my-util'), 'util');
      await linkEntry(root, UNLINKED, mine);
      await declare(root, undefined);

      expect(await install(root)).toContain(
        `! removed developer link ${LINKED}: smoo.developerLinks no longer declares its package`,
      );
      expect(readlinkSync(join(root, LINKED))).toBe('../../../lib');
      expect(readlinkSync(join(root, UNLINKED))).toBe(mine);
      expect(JSON.parse(readFileSync(join(root, 'node_modules', '.smoo-declared-links'), 'utf8'))).toEqual([]);
    });
  });

  it('keeps an undeclared unrecorded manual link even when it matched the old declaration', async () => {
    await withInstalled(WORKSPACE, { '@fixture/lib': '../local-lib' }, async (root) => {
      await install(root);
      const local = await checkout(join(dirname(root), 'local-lib'), '@fixture/lib');
      await linkEntry(root, LINKED, local);
      await link(root);
      await declare(root, undefined);

      await install(root);
      expect(readlinkSync(join(root, LINKED))).toBe(local);
    });
  });
});

describe('keeping developer links across an install meant to replace some of them', () => {
  // `app` and a second member, `web`, declare both packages. The root declares neither, so a developer links
  // `@fixture/lib` there by hand: three trees hold it and two hold `util`, and a name selects links in all of them.
  const WEB = 'packages/web';
  const WEB_LIB = `${WEB}/node_modules/@fixture/lib`;
  const WEB_UTIL = `${WEB}/node_modules/util`;
  const ROOT_LIB = 'node_modules/@fixture/lib';
  const WITH_WEB: Workspace = {
    members: { ...WORKSPACE.members, [WEB]: 'web' },
    lockfile: { ...WORKSPACE.lockfile, [WEB_LIB]: '../../../lib', [WEB_UTIL]: '../../util' },
  };

  interface Developer {
    readonly root: string;
    /** Every link's text, as the developer wrote it. */
    readonly original: Readonly<Record<string, string>>;
    /** Every link's text now. */
    readonly texts: () => Record<string, string>;
  }

  /** An installed workspace in which a developer has linked both packages, in every tree that has them. */
  async function withDeveloperLinks(run: (developer: Developer) => Promise<void>): Promise<void> {
    await withInstalled(WITH_WEB, undefined, async (root) => {
      const lib = join(dirname(root), 'local-lib');
      const util = join(dirname(root), 'local-util');
      await mkdir(lib);
      await mkdir(util);
      const original = {
        [ROOT_LIB]: lib,
        [LINKED]: lib,
        [WEB_LIB]: lib,
        // Relative to its own directory: a link put back must keep its text, not only its target.
        [UNLINKED]: '../../../../local-util',
        [WEB_UTIL]: util,
      };
      for (const [entry, target] of Object.entries(original)) await linkEntry(root, entry, target);
      const texts = () =>
        Object.fromEntries(
          Object.keys(original).map((entry): [string, string] => [entry, readlinkSync(join(root, entry))]),
        );
      await run({ root, original, texts });
    });
  }

  /** A package-manager replacement of declared entries, followed by selected local targets. */
  function replaceDeclaredLinks(root: string, linked: Readonly<Record<string, string>>): () => Promise<void> {
    return async () => {
      for (const [entry, lockfile] of Object.entries(WITH_WEB.lockfile)) await linkEntry(root, entry, lockfile);
      for (const [entry, target] of Object.entries(linked)) await linkEntry(root, entry, target);
    };
  }

  it('keeps the scoped package an install relinked in every tree, and every other link as it was', async () => {
    await withDeveloperLinks(async ({ root, original, texts }) => {
      const provider = join(dirname(root), 'provider-lib');
      await mkdir(provider);
      const relinked = { [ROOT_LIB]: provider, [LINKED]: provider, [WEB_LIB]: provider };

      await saying(() => keepDeveloperLinks(root, replaceDeclaredLinks(root, relinked), { relink: ['@fixture/lib'] }));

      expect(texts()).toEqual({ ...original, ...relinked });
    });
  });

  it('keeps the unscoped package an install relinked, and puts back a scoped one sharing its basename', async () => {
    await withDeveloperLinks(async ({ root, original, texts }) => {
      const provider = join(dirname(root), 'provider-util');
      await mkdir(provider);
      const relinked = { [UNLINKED]: provider, [WEB_UTIL]: provider };

      // Every install re-points `@fixture/lib` at the lockfile too. `lib` is the basename of that package, not
      // its name, so naming it selects nothing and the scoped links are put back.
      await saying(() => keepDeveloperLinks(root, replaceDeclaredLinks(root, relinked), { relink: ['util', 'lib'] }));

      expect(texts()).toEqual({ ...original, ...relinked });
    });
  });

  it.skipIf(process.platform === 'win32' || process.getuid?.() === 0)(
    'retains developer links when their target metadata is denied, and installs around them',
    async () => {
      await withDeveloperLinks(async ({ root, texts }) => {
        const blocked = join(dirname(root), 'blocked-provider');
        const provider = join(blocked, 'package');
        await mkdir(provider, { recursive: true });
        await linkEntry(root, LINKED, provider);
        const before = texts();
        await chmod(blocked, 0);
        try {
          // An unreadable checkout is not a deleted one, nor a reason to fail: the install runs and the link is put
          // back as made.
          const said = await saying(() => keepDeveloperLinks(root, replaceDeclaredLinks(root, {})));
          expect(said).toContain(
            `! developer link ${LINKED} -> ${provider}: its target cannot be read here (EACCES); kept as it is\n`,
          );
          expect(texts()).toEqual(before);
        } finally {
          await chmod(blocked, 0o700);
        }
      });
    },
  );

  it('puts every link back, the named packages included, when the install fails after relinking', async () => {
    await withDeveloperLinks(async ({ root, original, texts }) => {
      const provider = join(dirname(root), 'provider');
      await mkdir(provider);
      const relink = replaceDeclaredLinks(root, {
        [ROOT_LIB]: provider,
        [LINKED]: provider,
        [WEB_LIB]: provider,
        [UNLINKED]: provider,
        [WEB_UTIL]: provider,
      });

      const failure = new Error('package-manager relink failed');
      await expect(
        saying(() =>
          keepDeveloperLinks(
            root,
            async () => {
              await relink();
              throw failure;
            },
            { relink: ['@fixture/lib', 'util'] },
          ),
        ),
      ).rejects.toBe(failure);
      expect(texts()).toEqual(original);
    });
  });

  it('restores captured links without hiding a new external entry left by a failed relink', async () => {
    await withDeveloperLinks(async ({ root, original, texts }) => {
      const provider = join(dirname(root), 'new-provider');
      await mkdir(provider);
      const created = 'node_modules/@fixture/new';
      const failure = new Error('package-manager partial relink failed');
      await expect(
        saying(() =>
          keepDeveloperLinks(
            root,
            async () => {
              await linkEntry(root, created, provider);
              await linkEntry(root, LINKED, provider);
              throw failure;
            },
            { relink: ['@fixture/lib', '@fixture/new'] },
          ),
        ),
      ).rejects.toBe(failure);
      expect(texts()).toEqual(original);
      expect(readlinkSync(join(root, created))).toBe(provider);
    });
  });
});
