import { describe, expect, it } from 'bun:test';
import { mkdirSync, mkdtempSync, realpathSync, rmSync, symlinkSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, relative } from 'node:path';
import {
  BunInstallIntegrityError,
  type BunInstallIntegrityOperation,
  danglingStoreLinks,
  verifyInstalledBunLinks,
} from '../../../nx-plugin/managed/raw/tooling/direnv/installed-bun-links.ts';

/**
 * Link admission, called directly over node_modules trees planted the way a Bun isolated install leaves them: store
 * entries that link into Bun's install cache, a cache entry's relative aliases for its dependencies, the store's
 * hoisted aliases, and the aliases of the root and of workspace members. verifyInstalledBunLinks is the admission
 * shell entry passes once before it writes an install receipt, and danglingStoreLinks is what makes a matching
 * receipt stale. A real install only plants these links, so setup-environment-receipt.test.ts keeps the one entry
 * that shows what the entry does with a refusal, and every topology is decided here.
 */

/** The checkout, Bun's install cache beside it, and the places the cases plant into and a refusal names. */
interface Layout {
  readonly root: string;
  /** node_modules/.bun: one entry per package version, each a link into the install cache. */
  readonly store: string;
  /** Bun's install cache, outside the checkout. */
  readonly cache: string;
  /** The store's link for @fixture/parent@1.0.0, and the cache directory it names. */
  readonly parentLink: string;
  readonly parentEntry: string;
  /** A second store entry of the same package and version: the variant a restored cache carries beside the intact one. */
  readonly staleLink: string;
  /** The alias the parent's cache entry holds for its dependency, and the same alias as the store link shows it. */
  readonly childAlias: string;
  readonly storedChildAlias: string;
  /** @fixture/child in a cache entry of its own, and the store's hoisted alias for it. */
  readonly childPackage: string;
  readonly hoistAlias: string;
}

/** What admission stops at: what it was doing, the path it was on, and where that path was meant to lead. */
interface Refusal {
  readonly operation: BunInstallIntegrityOperation;
  readonly actualPath: string;
  readonly expectedTarget: string;
}

interface Admitted {
  readonly name: string;
  /** Turns the complete install into this topology; none leaves it as it is. */
  readonly plant?: (layout: Layout) => void;
  /** The node_modules of each workspace member the entry names; none by default. */
  readonly members?: (layout: Layout) => readonly string[];
}

interface Refused {
  readonly name: string;
  /** Turns the complete install into this topology, and returns where admission must stop in it. */
  readonly plant: (layout: Layout) => Refusal;
  readonly members?: (layout: Layout) => readonly string[];
}

const parentIn = (entry: string): string => join(entry, 'node_modules', '@fixture', 'parent');
const childIn = (entry: string): string => join(entry, 'node_modules', '@fixture', 'child');
/** The root's alias for the parent package. */
const parentAlias = (layout: Layout): string => parentIn(layout.root);
const memberModules = (layout: Layout): string => join(layout.root, 'packages', 'member', 'node_modules');

function layoutIn(scratch: string): Layout {
  const root = join(scratch, 'checkout');
  const store = join(root, 'node_modules', '.bun');
  const cache = join(scratch, 'private-bun-cache');
  const parentEntry = join(cache, 'fixture-parent@1.0.0');
  const parentLink = join(store, '@fixture+parent@1.0.0');
  return {
    root,
    store,
    cache,
    parentLink,
    parentEntry,
    staleLink: join(store, '@fixture+parent@1.0.0+stale'),
    childAlias: childIn(parentEntry),
    storedChildAlias: childIn(parentLink),
    childPackage: childIn(join(cache, 'fixture-child@1.0.0')),
    hoistAlias: childIn(store),
  };
}

const manifest = (name: string, version: string, fields: Readonly<Record<string, unknown>> = {}): string =>
  JSON.stringify({ name, version, ...fields });

function file(path: string, text: string): void {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, text);
}

/** As ln -sfn does: the link text is target exactly as given, and whatever was at path is replaced. */
function link(path: string, target: string): void {
  mkdirSync(dirname(path), { recursive: true });
  rmSync(path, { recursive: true, force: true });
  symlinkSync(target, path);
}

/** A link written the way the install writes a relative one: the target as seen from the directory holding it. */
function relativeLink(path: string, target: string): void {
  link(path, relative(dirname(path), target));
}

/**
 * An install with nothing wrong: a store entry linked into the cache, holding @fixture/parent, which depends on
 * @fixture/child through a relative alias into a second cache entry and declares an optional dependency the install
 * left out; the store's hoisted alias for the child; and an intact entry beside them, the TypeScript pin a managed
 * repository's fixture carries.
 */
function plantComplete(layout: Layout): void {
  file(
    join(layout.store, 'typescript@6.0.3', 'node_modules', 'typescript', 'package.json'),
    manifest('typescript', '6.0.3'),
  );
  file(
    join(parentIn(layout.parentEntry), 'package.json'),
    manifest('@fixture/parent', '1.0.0', {
      dependencies: { '@fixture/child': '1.0.0' },
      optionalDependencies: { '@fixture/omitted': '1.0.0' },
    }),
  );
  file(join(layout.childPackage, 'package.json'), manifest('@fixture/child', '1.0.0'));
  link(layout.parentLink, layout.parentEntry);
  relativeLink(layout.childAlias, layout.childPackage);
  link(layout.hoistAlias, layout.childPackage);
}

/**
 * A superseded parent variant the store links to: its own dependency alias aims at a cache entry this machine never
 * had, as a restored cache's entries do.
 */
function plantStaleVariant(layout: Layout, entry: string): void {
  file(join(parentIn(entry), 'package.json'), manifest('@fixture/parent', '1.0.0'));
  link(childIn(entry), join(layout.cache, 'missing-child'));
  link(layout.staleLink, entry);
}

function withInstall(run: (layout: Layout) => void): void {
  const scratch = realpathSync(mkdtempSync(join(tmpdir(), 'smoo-bun-links-')));
  try {
    const layout = layoutIn(scratch);
    plantComplete(layout);
    run(layout);
  } finally {
    rmSync(scratch, { recursive: true, force: true });
  }
}

const resolving = (actualPath: string, expectedTarget: string): Refusal => ({
  operation: 'resolve installed path',
  actualPath,
  expectedTarget,
});

const reading = (actualPath: string, expectedTarget: string): Refusal => ({
  operation: 'read installed package manifest',
  actualPath,
  expectedTarget,
});

/** Where admission stops in a superseded variant something live reaches: its dependency alias, as the store shows it. */
const staleRefusal = (layout: Layout): Refusal =>
  resolving(childIn(layout.staleLink), join(layout.cache, 'missing-child'));

const ADMITTED: readonly Admitted[] = [
  { name: 'the scoped, transitive and hoisted links of a complete install' },
  {
    name: 'a root alias into an intact store entry',
    plant: (layout) => relativeLink(parentAlias(layout), parentIn(layout.parentLink)),
  },
  {
    // A store entry from another machine's install: the intact entry's package and version, its link aimed at an
    // install cache this machine never had.
    name: 'a superseded store variant whose link dangles',
    plant: (layout) => link(layout.staleLink, join(layout.cache, 'missing-stale-variant')),
  },
  {
    // A whole superseded variant: its own dependency alias dangles and nothing in the live tree references it. A live
    // alias into it is what makes the same tree a refusal below.
    name: 'a superseded parent variant whose own dependency alias dangles',
    plant: (layout) => plantStaleVariant(layout, join(layout.cache, 'fixture-parent-stale@1.0.0')),
  },
  {
    name: 'a root scope that is itself a symlink to a directory of packages',
    plant: (layout) => {
      const scope = join(layout.cache, 'linked-scope');
      file(join(scope, 'pkg', 'package.json'), manifest('@linked/pkg', '1.0.0'));
      link(join(layout.root, 'node_modules', '@linked'), scope);
    },
  },
  {
    name: 'a hoisted install, which has no isolated store to admit',
    plant: (layout) => rmSync(layout.store, { recursive: true }),
  },
  {
    // A member with no dependencies has nothing installed: no node_modules is not a broken one.
    name: 'a workspace member with nothing installed',
    members: (layout) => [memberModules(layout)],
  },
];

const REFUSED: readonly Refused[] = [
  {
    name: 'a store link whose install-cache entry is gone',
    plant: (layout) => {
      link(layout.parentLink, join(layout.cache, 'missing-parent'));
      return resolving(layout.parentLink, join(layout.cache, 'missing-parent'));
    },
  },
  {
    name: 'a dependency alias inside a cache entry aimed at a cache entry that is gone',
    plant: (layout) => {
      relativeLink(layout.childAlias, join(layout.cache, 'missing-child'));
      return resolving(layout.storedChildAlias, join(layout.cache, 'missing-child'));
    },
  },
  {
    name: 'a hoisted alias aimed at a cache entry that is gone',
    plant: (layout) => {
      link(layout.hoistAlias, join(layout.cache, 'missing-child'));
      return resolving(layout.hoistAlias, join(layout.cache, 'missing-child'));
    },
  },
  {
    // The hoisted alias reaches the same package: it is left out, so the dependency alias is the one place this shows.
    name: 'a linked package whose directory has no manifest',
    plant: (layout) => {
      rmSync(layout.hoistAlias);
      rmSync(join(layout.childPackage, 'package.json'));
      return reading(layout.storedChildAlias, layout.childPackage);
    },
  },
  {
    name: 'a linked package whose manifest is not JSON',
    plant: (layout) => {
      rmSync(layout.hoistAlias);
      writeFileSync(join(layout.childPackage, 'package.json'), '{');
      return reading(layout.storedChildAlias, layout.childPackage);
    },
  },
  {
    name: 'a dangling variant the installed tree still references, however intact its replacement',
    plant: (layout) => {
      link(layout.staleLink, join(layout.cache, 'missing-stale'));
      relativeLink(parentAlias(layout), parentIn(layout.staleLink));
      return resolving(parentAlias(layout), parentIn(layout.staleLink));
    },
  },
  {
    name: 'a live alias aimed at a version-named missing target, however intact its namesake',
    plant: (layout) => {
      link(layout.childAlias, join(layout.cache, 'missing', '@fixture+parent@1.0.0'));
      return resolving(layout.storedChildAlias, join(layout.cache, 'missing', '@fixture+parent@1.0.0'));
    },
  },
  {
    name: 'a root alias aimed into a store entry that does not exist, however intact its namesake',
    plant: (layout) => {
      const ghost = join(layout.store, '@fixture+parent@1.0.0+nonexistent');
      relativeLink(parentAlias(layout), parentIn(ghost));
      return resolving(parentAlias(layout), parentIn(ghost));
    },
  },
  {
    name: 'a root alias aimed at an absolute install-cache target that does not exist',
    plant: (layout) => {
      const old = join(layout.cache, 'links', '@fixture+parent@1.0.0+old');
      link(join(layout.store, '@fixture+parent@1.0.0+old'), old);
      link(parentAlias(layout), parentIn(old));
      return resolving(parentAlias(layout), parentIn(old));
    },
  },
  {
    name: 'a superseded variant under a suffixed cache basename, reached by an absolute root alias',
    plant: (layout) => {
      const entry = join(layout.cache, 'links', '@fixture+parent@1.0.0+stale-CACHEHASH');
      plantStaleVariant(layout, entry);
      link(parentAlias(layout), parentIn(entry));
      return staleRefusal(layout);
    },
  },
  {
    name: 'a superseded variant reached through an intermediate external alias',
    plant: (layout) => {
      const entry = join(layout.cache, 'links', '@fixture+parent@1.0.0+stale-CACHEHASH');
      plantStaleVariant(layout, entry);
      const intermediate = join(layout.cache, 'intermediate', 'parent');
      link(intermediate, parentIn(entry));
      link(parentAlias(layout), intermediate);
      return staleRefusal(layout);
    },
  },
  {
    name: 'a dangling variant reached only through a symlinked root scope',
    plant: (layout) => {
      plantStaleVariant(layout, join(layout.cache, 'fixture-parent-stale@1.0.0'));
      const scope = join(layout.cache, 'linked-scope');
      link(join(scope, 'parent'), parentIn(layout.staleLink));
      link(join(layout.root, 'node_modules', '@linked'), scope);
      return staleRefusal(layout);
    },
  },
  {
    name: 'a root package alias that resolves to a regular file, not a package directory',
    plant: (layout) => {
      const notAPackage = join(layout.root, 'README.md');
      file(notAPackage, 'not a package');
      link(join(layout.root, 'node_modules', '@fixture', 'filepkg'), notAPackage);
      return reading(join(layout.root, 'node_modules', '@fixture', 'filepkg'), notAPackage);
    },
  },
  {
    name: 'a workspace member alias aimed at a missing external target',
    members: (layout) => [memberModules(layout)],
    plant: (layout) => {
      const alias = join(memberModules(layout), '@fixture', 'memberpkg');
      link(alias, join(layout.cache, 'missing-member-target'));
      return resolving(alias, join(layout.cache, 'missing-member-target'));
    },
  },
];

function refusalOf(error: BunInstallIntegrityError | undefined): Refusal | undefined {
  return error === undefined
    ? undefined
    : { operation: error.operation, actualPath: error.actualPath, expectedTarget: error.expectedTarget };
}

describe('verifyInstalledBunLinks', () => {
  describe('admits', () => {
    for (const topology of ADMITTED) {
      it(topology.name, () => {
        withInstall((layout) => {
          topology.plant?.(layout);
          expect(verifyInstalledBunLinks(layout.root, topology.members?.(layout) ?? [])).toBeUndefined();
        });
      });
    }
  });

  describe('refuses', () => {
    for (const topology of REFUSED) {
      it(topology.name, () => {
        withInstall((layout) => {
          const expected = topology.plant(layout);
          const error = verifyInstalledBunLinks(layout.root, topology.members?.(layout) ?? []);
          expect(refusalOf(error)).toEqual(expected);
          expect(error).toBeInstanceOf(BunInstallIntegrityError);
          expect(error?.code).toBe('BUN_INSTALL_INTEGRITY');
          expect(error?.cause).toBeInstanceOf(Error);
          expect(error?.message).toStartWith(
            `Bun install integrity: ${expected.operation} failed for ${expected.actualPath}; expected target ${expected.expectedTarget}: `,
          );
        });
      });
    }
  });
});

describe('danglingStoreLinks', () => {
  // As an install writes a root dependency: relative, through the store entry's own node_modules.
  const reachFromRoot = (layout: Layout): void => relativeLink(parentAlias(layout), parentIn(layout.parentLink));
  // Bun's install cache pruned: the entry the store link names is gone.
  const pruneCacheEntry = (layout: Layout): void => rmSync(layout.parentEntry, { recursive: true });

  it('names no link while every store link reaches its install-cache entry, and never asks for the members', () => {
    withInstall((layout) => {
      reachFromRoot(layout);
      let asked = 0;
      const named = danglingStoreLinks(layout.root, () => {
        asked += 1;
        return [];
      });
      expect(named).toEqual([]);
      expect(asked).toBe(0);
    });
  });

  it('names no superseded variant whose install-cache entry is gone and nothing reaches', () => {
    withInstall((layout) => {
      reachFromRoot(layout);
      link(layout.staleLink, join(layout.cache, 'fixture-parent@1.0.0-superseded'));
      expect(danglingStoreLinks(layout.root, () => [])).toEqual([]);
    });
  });

  it('names the store link whose install-cache entry the installed tree reaches, and only that one', () => {
    withInstall((layout) => {
      reachFromRoot(layout);
      link(layout.staleLink, join(layout.cache, 'fixture-parent@1.0.0-superseded'));
      pruneCacheEntry(layout);
      expect(danglingStoreLinks(layout.root, () => [])).toEqual([layout.parentLink]);
    });
  });

  it("counts a workspace member's alias as reaching an entry, as a root alias does", () => {
    withInstall((layout) => {
      relativeLink(join(memberModules(layout), '@fixture', 'parent'), parentIn(layout.parentLink));
      pruneCacheEntry(layout);
      expect(danglingStoreLinks(layout.root, () => [memberModules(layout)])).toEqual([layout.parentLink]);
      // With no member named nothing reaches the entry: it is superseded garbage, not an out-of-date install.
      expect(danglingStoreLinks(layout.root, () => [])).toEqual([]);
    });
  });

  it('finds no store to link from in a hoisted install', () => {
    withInstall((layout) => {
      rmSync(layout.store, { recursive: true });
      expect(danglingStoreLinks(layout.root, () => [])).toEqual([]);
    });
  });
});
