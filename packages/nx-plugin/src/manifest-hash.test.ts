import { describe, expect, it } from 'bun:test';
import { createHash } from 'node:crypto';
import { mkdir, mkdtemp, rename, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';

import { hashVersionlessCrateManifests, updateVersionlessCrateManifest } from './manifest-hash.js';

const CRATE_MANIFEST = `[package]
name = "acme-core"
version = "1.2.3"
edition = "2024"

[dependencies]
serde = { version = "1.0.200", features = ["derive"] }
acme-sibling = { path = "../sibling", version = "1.2.3" }
`;

/**
 * The line a form writes the crate's own version on, and the manifest it sits
 * in. Dropping it must leave the digest exactly where hashing the manifest
 * with it leaves it: that is what "the release cannot move this digest" means.
 */
const OWN_VERSION_FORMS: Record<string, [manifest: string, line: string]> = {
  plain: ['[package]\nname = "acme-core"\n@\nedition = "2024"\n', 'version = "1.2.3"'],
  padded: ['[package]\nname = "acme-core"\n@\nedition = "2024"\n', '  version\t=   "1.2.3"   '],
  commented: ['[package]\nname = "acme-core"\n@\nedition = "2024"\n', 'version = "1.2.3" # bumped by the release'],
  inherited: ['[package]\nname = "acme-core"\n@\nedition = "2024"\n', 'version.workspace = true'],
  'inherited, spaced': ['[package]\nname = "acme-core"\n@\n', 'version . workspace = true'],
  'quoted key': ['[package]\nname = "acme-core"\n@\n', '"version" = "1.2.3"'],
  'literal-quoted key': ['[package]\nname = "acme-core"\n@\n', "'version' = '1.2.3'"],
  'literal-quoted value': ['[package]\nname = "acme-core"\n@\n', "version = '1.2.3'"],
  'padded header': ['[ package ]\nname = "acme-core"\n@\n', 'version = "1.2.3"'],
  'commented header': ['[package] # the crate itself\nname = "acme-core"\n@\n', 'version = "1.2.3"'],
  'quoted header': ['["package"]\nname = "acme-core"\n@\n', 'version = "1.2.3"'],
  'workspace package': [
    '[workspace]\nmembers = ["crates/*"]\n\n[workspace.package]\n@\nedition = "2024"\n',
    'version = "0.0.1"',
  ],
  'workspace package, spaced header': ['[workspace . package]\n@\nedition = "2024"\n', 'version = "0.0.1"'],
  'after a multi-line array': [
    '[package]\nname = "acme-core"\nkeywords = [\n  "build",\n  "cache",\n]\n@\n',
    'version = "1.2.3"',
  ],
  crlf: ['[package]\r\nname = "acme-core"\r\n@\r\nedition = "2024"\r\n', 'version = "1.2.3"'],
};

/**
 * A `version` that names a DIFFERENT crate. Dropping one of these lines must
 * move the digest, or a dependency change serves a stale result.
 */
const FOREIGN_VERSION_FORMS: Record<string, [manifest: string, line: string]> = {
  'dependency table': [
    '[package]\nname = "acme-core"\nversion = "1.2.3"\n\n[dependencies.serde]\n@\n',
    'version = "1.0.200"',
  ],
  'dev-dependency table': ['[package]\nname = "acme-core"\n\n[dev-dependencies.insta]\n@\n', 'version = "1.39"'],
  'build-dependency table': ['[package]\nname = "acme-core"\n\n[build-dependencies.cc]\n@\n', 'version = "1.0"'],
  'workspace dependency table': ['[workspace]\n\n[workspace.dependencies.serde]\n@\n', 'version = "1.0.200"'],
  'target cfg dependency table': [
    '[package]\nname = "acme-core"\n\n[target.\'cfg(unix)\'.dependencies.libc]\n@\n',
    'version = "0.2.155"',
  ],
  'inline table': ['[package]\nname = "acme-core"\n\n[dependencies]\n@\n', 'serde = { version = "1.0.200" }'],
  'package metadata table': ['[package]\nname = "acme-core"\n\n[package.metadata.acme]\n@\n', 'version = "7"'],
};

function digest(manifest: string): string {
  const hash = createHash('sha256');
  updateVersionlessCrateManifest(hash, Buffer.from(manifest));
  return hash.digest('hex');
}

/**
 * Whether each form's version line contributes nothing at all to the digest:
 * the manifest carrying it hashes to what the manifest without it hashes to.
 */
function elided(forms: Record<string, [manifest: string, line: string]>): Record<string, boolean> {
  const result: Record<string, boolean> = {};
  for (const [name, [manifest, line]] of Object.entries(forms)) {
    result[name] = digest(manifest.replace('@', line)) === digest(manifest.replace(/@\r?\n/, ''));
  }
  return result;
}

describe('versionless crate manifest hashing', () => {
  it('drops the version a crate declares for itself, in every form a manifest writes it', () => {
    expect(elided(OWN_VERSION_FORMS)).toEqual(Object.fromEntries(Object.keys(OWN_VERSION_FORMS).map((n) => [n, true])));
  });

  it('keeps every version that names a different crate', () => {
    expect(elided(FOREIGN_VERSION_FORMS)).toEqual(
      Object.fromEntries(Object.keys(FOREIGN_VERSION_FORMS).map((n) => [n, false])),
    );
  });

  it("separates the tables it left, so a later version is not read as the crate's own", () => {
    const own = '[package]\nname = "acme-core"\nversion = "1.2.3"\n';
    // The same bytes under a dependency table must not collapse onto the same
    // digest as the crate's own version: the scanner has to know which table
    // it is standing in, not just which line it is looking at.
    expect(digest(`${own}\n[dependencies.serde]\nversion = "1.0.200"\n`)).not.toBe(
      digest(`${own}\n[dependencies.serde]\nversion = "1.0.201"\n`),
    );
  });

  it('hashes verbatim from the first construct a line scan cannot read', () => {
    // A multi-line string can spell out a `[package]` header and a version
    // line, and no line scan can tell that text from the document. Everything
    // after one is hashed as written, so the release moves this digest — a
    // miss, never a wrong hit.
    const withNote = (version: string) =>
      `[package]\nname = "acme-core"\ndescription = """\n[package]\nversion = "0.0.0"\n"""\nversion = "${version}"\n`;
    expect(digest(withNote('1.2.3'))).not.toBe(digest(withNote('2.0.0')));

    // A version the scanner reached BEFORE the string is still dropped.
    const beforeNote = (version: string) =>
      `[package]\nname = "acme-core"\nversion = "${version}"\ndescription = """\nany text\n"""\n`;
    expect(digest(beforeNote('1.2.3'))).toBe(digest(beforeNote('2.0.0')));
  });

  it('ignores the version a crate declares for itself and nothing else', async () => {
    await withProject(async (project) => {
      const baseline = await hashVersionlessCrateManifests(project.root, project.workspaceRoot);

      await project.write('crates/core/Cargo.toml', CRATE_MANIFEST.replace('version = "1.2.3"', 'version = "2.0.0"'));
      expect(await hashVersionlessCrateManifests(project.root, project.workspaceRoot)).toBe(baseline);

      // Same release, one real change alongside it: the digest must move again.
      await project.write('crates/core/Cargo.toml', CRATE_MANIFEST.replace('1.0.200', '1.0.201'));
      expect(await hashVersionlessCrateManifests(project.root, project.workspaceRoot)).not.toBe(baseline);
    });
  });

  it('reports a moved crate manifest', async () => {
    await withProject(async (project) => {
      const baseline = await hashVersionlessCrateManifests(project.root, project.workspaceRoot);
      await project.move('crates/core/Cargo.toml', 'crates/renamed/Cargo.toml');

      expect(await hashVersionlessCrateManifests(project.root, project.workspaceRoot)).not.toBe(baseline);
    });
  });

  it('hashes a manifest it cannot read instead of dropping it', async () => {
    await withProject(async (project) => {
      await project.write('crates/core/Cargo.toml', 'this is not toml [[[');
      const broken = await hashVersionlessCrateManifests(project.root, project.workspaceRoot);
      expect(await hashVersionlessCrateManifests(project.root, project.workspaceRoot)).toBe(broken);

      // An unreadable manifest still contributes its bytes, so a change inside
      // it invalidates. Dropping it would leave a stale cache hit instead.
      await project.write('crates/core/Cargo.toml', 'this is not toml either ]]]');
      expect(await hashVersionlessCrateManifests(project.root, project.workspaceRoot)).not.toBe(broken);
    });
  });

  it('hashes the same tree the same way twice', async () => {
    await withProject(async (project) => {
      expect(await hashVersionlessCrateManifests(project.root, project.workspaceRoot)).toMatch(/^[0-9a-f]{64}$/);
      expect(await hashVersionlessCrateManifests(project.root, project.workspaceRoot)).toBe(
        await hashVersionlessCrateManifests(project.root, project.workspaceRoot),
      );
    });
  });
});

interface ProjectFixture {
  workspaceRoot: string;
  root: string;
  write(path: string, contents: string): Promise<void>;
  move(from: string, to: string): Promise<void>;
}

async function withProject(body: (project: ProjectFixture) => Promise<void>): Promise<void> {
  const workspaceRoot = await mkdtemp(join(tmpdir(), 'smoothbricks-manifest-hash-'));
  const root = 'packages/app';
  const write = async (path: string, contents: string): Promise<void> => {
    const absolute = join(workspaceRoot, path);
    await mkdir(dirname(absolute), { recursive: true });
    await writeFile(absolute, contents);
  };
  try {
    await write(join(root, 'package.json'), JSON.stringify({ name: '@acme/app', version: '1.2.3' }));
    await write(join(root, 'crates/core/Cargo.toml'), CRATE_MANIFEST);
    await write(join(root, 'src/index.ts'), 'export const answer = 42;\n');
    await body({
      workspaceRoot,
      root,
      write: async (path, contents) => write(join(root, path), contents),
      move: async (from, to) => {
        const target = join(workspaceRoot, root, to);
        await mkdir(dirname(target), { recursive: true });
        await rename(join(workspaceRoot, root, from), target);
      },
    });
  } finally {
    await rm(workspaceRoot, { recursive: true, force: true });
  }
}
