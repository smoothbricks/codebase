import { afterEach, describe, expect, it, jest } from 'bun:test';
import { spawn, spawnSync } from 'node:child_process';
import { once } from 'node:events';
import { mkdir, mkdtemp, readdir, realpath, rm, stat, utimes, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { BOUNDED_TEST_IDLE_TIMEOUT_MS } from './bounded-test-policy.js';
import {
  EXTRACTED_BINARIES_METADATA,
  EXTRACTED_CARGO_METADATA,
  EXTRACTED_TARGET_DIR,
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

/**
 * The lock's timing. `staleAfterMs` is the bound the first case is about; `heartbeatMs` and `pollMs` are intervals, not
 * bounds. The heartbeat must stay below `staleAfterMs` for a live holder, and neither case reaches the poll.
 */
const FAST = { heartbeatMs: 20, staleAfterMs: 400, pollMs: 10 };

describe('ensureNextestArchiveExtracted', () => {
  afterEach(() => jest.useRealTimers());

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
      // Dated on the fake clock, which moves only when advanced: the lock's age the product reads is exactly this minute.
      // No holder refreshes it, so it is stale at any host speed and the case never reaches the poll.
      jest.useFakeTimers();
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
      expect(events).toEqual([{ kind: 'stale-lock', holder: `pid ${process.pid}`, ageMs: 60_000 }]);
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

      // Nothing else holds the lock, so no staleness bound can decide this case: it takes the leg's backstop.
      const timing = { ...FAST, staleAfterMs: BOUNDED_TEST_IDLE_TIMEOUT_MS };
      await expect(ensureNextestArchiveExtracted(archive, paths.root, rebuiltMidway, () => {}, timing)).rejects.toThrow(
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

describe('smoo-nx-nextest-extract', () => {
  /** One shard's run of the real helper, as a runner's `$(...)` makes it: own TMPDIR, the archive in the checkout. */
  async function shard(root: string, n: number) {
    const tmp = join(root, `tmp-${n}`);
    await mkdir(tmp);
    const child = spawn(
      process.execPath,
      [join(import.meta.dir, 'bin/smoo-nx-nextest-extract.ts'), 'archive.tar.zst'],
      {
        cwd: root,
        env: { ...process.env, TMPDIR: tmp },
        stdio: ['ignore', 'pipe', 'pipe'],
      },
    );
    let stdout = '';
    let stderr = '';
    child.stdout.on('data', (chunk: Buffer) => {
      stdout += chunk.toString();
    });
    child.stderr.on('data', (chunk: Buffer) => {
      stderr += chunk.toString();
    });
    const [code]: unknown[] = await once(child, 'close');
    return { code, stdout, stderr, tmp };
  }

  it('unpacks an archive once for four concurrent shards, in the build tree and in none of their temp directories', async () => {
    // A downstream land gate: shards that each ran `nextest run --archive-file` unpacked the whole 1.97 GiB archive into
    // their own TMPDIR, a lease on one 8 GiB RAM volume, and four of them filled it.
    const root = await realpath(await mkdtemp(join(tmpdir(), 'nextest-extract-shards-')));
    try {
      await writeFile(
        join(root, 'Cargo.toml'),
        '[package]\nname = "shard-probe"\nversion = "0.1.0"\nedition = "2021"\n',
      );
      await mkdir(join(root, 'src'));
      await writeFile(join(root, 'src/lib.rs'), '#[test]\nfn adds() {\n    assert_eq!(1 + 1, 2);\n}\n');
      expect(spawnSync('cargo', ['generate-lockfile', '--offline'], { cwd: root }).status).toBe(0);
      const archived = spawnSync(
        'cargo',
        ['--frozen', 'nextest', 'archive', '--archive-file', 'archive.tar.zst', '--user-config-file', 'none'],
        { cwd: root, encoding: 'utf8' },
      );
      expect({ status: archived.status, stderr: archived.status === 0 ? '' : archived.stderr }).toEqual({
        status: 0,
        stderr: '',
      });

      const shards = await Promise.all([1, 2, 3, 4].map((n) => shard(root, n)));

      const extractions = join(root, 'target', NEXTEST_EXTRACTION_DIRECTORY);
      const key = await nextestArchiveKey(join(root, 'archive.tar.zst'));
      const directory = join(extractions, key);
      expect(shards.map(({ code, stdout }) => ({ code, stdout }))).toEqual(
        Array.from({ length: 4 }, () => ({ code: 0, stdout: `${directory}\n` })),
      );
      // One shard unpacked it; the others waited on it or found it done.
      expect(shards.filter(({ stderr }) => stderr.includes(': extracted archive.tar.zst ')).length).toBe(1);
      // One extraction under the archive's hash, nothing staged or locked beside it, and no shard's TMPDIR touched.
      expect(await readdir(extractions)).toEqual([key]);
      for (const { tmp } of shards) {
        expect({ tmp, entries: await readdir(tmp) }).toEqual({ tmp, entries: [] });
      }
      // A shard runs its tests from that one extraction.
      const ran = spawnSync(
        'cargo',
        [
          '--frozen',
          'nextest',
          'run',
          '--binaries-metadata',
          join(directory, EXTRACTED_BINARIES_METADATA),
          '--cargo-metadata',
          join(directory, EXTRACTED_CARGO_METADATA),
          '--target-dir-remap',
          join(directory, EXTRACTED_TARGET_DIR),
          '--workspace-remap',
          '.',
          '--user-config-file',
          'none',
        ],
        { cwd: root, encoding: 'utf8' },
      );
      expect({ status: ran.status, passed: ran.stderr.includes('1 test run: 1 passed') }).toEqual({
        status: 0,
        passed: true,
      });
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });
});
