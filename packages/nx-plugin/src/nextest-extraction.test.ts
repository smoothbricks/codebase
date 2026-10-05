import { describe, expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import { mkdir, mkdtemp, readdir, realpath, rm, stat, utimes, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  EXTRACTED_BINARIES_METADATA,
  EXTRACTED_CARGO_METADATA,
  ensureNextestArchiveExtracted,
  NEXTEST_EXTRACTION_DIRECTORY,
  type NextestArchiveExtractor,
  type NextestExtractionEvent,
  nextestArchiveKey,
  nextestExtractionPaths,
  nextestExtractionRoot,
} from './nextest-extraction.js';

/** What nextest leaves behind: the two metadata files a reuse run reads. */
const unpack: NextestArchiveExtractor = async (_archive, destination) => {
  await mkdir(join(destination, 'target/nextest'), { recursive: true });
  await writeFile(join(destination, EXTRACTED_BINARIES_METADATA), '{}');
  await writeFile(join(destination, EXTRACTED_CARGO_METADATA), '{}');
};

const FAST = { heartbeatMs: 20, staleAfterMs: 400, pollMs: 10 };

describe('ensureNextestArchiveExtracted', () => {
  it('takes over a lock its holder stopped refreshing instead of waiting on it forever', async () => {
    const root = await mkdtemp(join(tmpdir(), 'nextest-extraction-'));
    try {
      const archive = join(root, 'archive.tar.zst');
      await writeFile(archive, 'archive bytes');
      const paths = nextestExtractionPaths(join(root, 'extractions'), await nextestArchiveKey(archive));
      // A holder killed mid-extraction (a bounded run's SIGKILL) leaves its
      // lock behind, and a pid in it that may since belong to anyone.
      await mkdir(paths.root, { recursive: true });
      await writeFile(paths.lock, `${process.pid} abandoned\n`);
      const minuteAgo = new Date(Date.now() - 60_000);
      await utimes(paths.lock, minuteAgo, minuteAgo);

      const events: NextestExtractionEvent[] = [];
      const extraction = await ensureNextestArchiveExtracted(
        archive,
        paths.root,
        unpack,
        (event) => events.push(event),
        FAST,
      );

      expect(extraction).toMatchObject({ kind: 'extracted', directory: paths.directory });
      expect(events).toEqual([{ kind: 'stale-lock', holder: `pid ${process.pid}`, ageMs: expect.any(Number) }]);
      expect(await readdir(paths.root)).toEqual([paths.directory.slice(paths.root.length + 1)]);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('publishes nothing when the archive changes while it is being extracted', async () => {
    const root = await mkdtemp(join(tmpdir(), 'nextest-extraction-'));
    try {
      const archive = join(root, 'archive.tar.zst');
      await writeFile(archive, 'archive bytes');
      const paths = nextestExtractionPaths(join(root, 'extractions'), await nextestArchiveKey(archive));
      // Unpacked from the old bytes, but the directory would be named for
      // them while the path now holds others: a later run hashing the new
      // bytes would never find it, and one hashing the old bytes again — the
      // archive restored from cache — must not find a mixture.
      const rebuiltMidway: NextestArchiveExtractor = async (source, destination) => {
        await unpack(source, destination);
        await writeFile(archive, 'rebuilt archive bytes');
      };

      await expect(ensureNextestArchiveExtracted(archive, paths.root, rebuiltMidway, () => {}, FAST)).rejects.toThrow(
        /changed while it was being extracted/,
      );
      // No directory under the old key, no staging left over, no lock held.
      expect(await readdir(paths.root)).toEqual([]);
      await expect(stat(paths.directory)).rejects.toMatchObject({ code: 'ENOENT' });
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });
});

describe('nextestExtractionRoot', () => {
  it("extracts under the target directory Cargo reports, never beside the archive's Nx output", async () => {
    const root = await realpath(await mkdtemp(join(tmpdir(), 'nextest-extraction-root-')));
    try {
      await writeFile(
        join(root, 'Cargo.toml'),
        '[package]\nname = "extraction-root-probe"\nversion = "0.1.0"\nedition = "2021"\n',
      );
      await mkdir(join(root, 'src'));
      await writeFile(join(root, 'src/lib.rs'), '');
      expect(spawnSync('cargo', ['generate-lockfile', '--offline'], { cwd: root }).status).toBe(0);
      expect(await nextestExtractionRoot(root)).toBe(join(root, 'target', NEXTEST_EXTRACTION_DIRECTORY));

      // A repository that moves Cargo's build tree moves its extractions with it.
      await mkdir(join(root, '.cargo'));
      await writeFile(join(root, '.cargo/config.toml'), '[build]\ntarget-dir = "out/cargo"\n');
      expect(await nextestExtractionRoot(root)).toBe(join(root, 'out/cargo', NEXTEST_EXTRACTION_DIRECTORY));
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });
});
