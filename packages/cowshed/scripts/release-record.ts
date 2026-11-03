/**
 * Writes a release build's record beside its binary (specs/cowshed/05_gateway.md, "Build records and the basis"):
 * `<binary>.build`, the JSON `{"commit","commitTime","sha256"}` setup reads to judge an install
 * (crates/cowshed-cli/src/build_record.rs, `BuildRecord::beside`).
 *
 *   bun scripts/release-record.ts <binary>
 *
 * The binary is cached by its sources alone, so the one built at an earlier commit is replayed for every commit that
 * left them unchanged. The record is what names the commit: each `cli-record-<platform>` target keys on HEAD and on the
 * binary's bytes, runs this after its `cli-<platform>` build, and binds the commit to exactly those bytes by their
 * SHA-256, which setup checks before it believes the record.
 *
 * Git runs with no global or system configuration, as the runtime input that keys these targets does: the Nx daemon
 * evaluates inputs inside the workspace sandbox, where the user's git configuration cannot be read.
 */
import { renameSync, writeFileSync } from 'node:fs';

/** What a release record says: the build, and the bytes it describes. */
export interface ReleaseRecord {
  readonly commit: string;
  readonly commitTime: number;
  readonly sha256: string;
}

/** Runs git with `args` and answers its trimmed stdout, or throws with what git said. */
export type Git = (args: readonly string[]) => string;

/** Git in `cwd`, reading no global or system configuration. */
export function gitIn(cwd: string): Git {
  return (args) => {
    const result = Bun.spawnSync(['git', ...args], {
      cwd,
      env: { ...process.env, GIT_CONFIG_GLOBAL: '/dev/null', GIT_CONFIG_NOSYSTEM: '1' },
      stdout: 'pipe',
      stderr: 'pipe',
    });
    if (result.exitCode !== 0) {
      throw new Error(`git ${args.join(' ')} in ${cwd} exited ${result.exitCode}: ${result.stderr.toString().trim()}`);
    }
    return result.stdout.toString().trim();
  };
}

/** The record of `binary` as built from the commit HEAD names. */
export async function releaseRecord(binary: string, git: Git): Promise<ReleaseRecord> {
  const hasher = new Bun.CryptoHasher('sha256');
  for await (const chunk of Bun.file(binary).stream()) {
    hasher.update(chunk);
  }
  const commit = git(['rev-parse', 'HEAD']);
  if (!/^[0-9a-f]{40}$/.test(commit)) {
    throw new Error(`git rev-parse HEAD answered ${JSON.stringify(commit)}, not a commit id`);
  }
  const time = git(['log', '-1', '--format=%ct', commit]);
  const commitTime = Number(time);
  if (!/^[0-9]+$/.test(time) || !Number.isSafeInteger(commitTime)) {
    throw new Error(`git log answered ${JSON.stringify(time)} for ${commit}'s commit time, not seconds`);
  }
  return { commit, commitTime, sha256: hasher.digest('hex') };
}

/** Where the record of `binary` goes: beside it, its name and `.build`. */
export function releaseRecordPath(binary: string): string {
  return `${binary}.build`;
}

/** Writes `record` beside `binary`, whole or not at all: a reader never sees half a record. */
export function writeReleaseRecord(binary: string, record: ReleaseRecord): void {
  const path = releaseRecordPath(binary);
  const partial = `${path}.partial`;
  writeFileSync(partial, `${JSON.stringify(record)}\n`);
  renameSync(partial, path);
}

if (import.meta.main) {
  const [binary, ...rest] = process.argv.slice(2);
  if (binary === undefined || rest.length > 0) {
    process.stderr.write('usage: bun scripts/release-record.ts <binary>\n');
    process.exit(2);
  }
  const record = await releaseRecord(binary, gitIn(process.cwd()));
  writeReleaseRecord(binary, record);
  process.stdout.write(`${releaseRecordPath(binary)}: commit ${record.commit}, sha256 ${record.sha256}\n`);
}
