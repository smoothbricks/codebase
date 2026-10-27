/**
 * The native CLI against the published Node CLI on copies of one churned real Cargo target: each scenario's stdout,
 * stderr, exit status and resulting tree must be identical, and each must end where it should (reclaimed, busy,
 * refused), so two binaries failing alike prove nothing. The Node side is the executable the packed tarball's
 * `package.json#bin` declares, run through its own shebang and execute bit: a published bin npm cannot run, or one whose
 * locks Node cannot bind, fails every scenario past usage. This file is not `*.test.ts`, so a plain `bun test` skips
 * it; the native target names it (`bun test ./src/__tests__/native-cli.differential.ts`) after building
 * `dist/bin/<platform>-<arch>/smoo-nx-cargo-retain`, and a missing binary fails the run.
 */
import { describe, it } from 'bun:test';
import { type SpawnSyncReturns, spawnSync } from 'node:child_process';
import { existsSync } from 'node:fs';
import { chmod, cp, mkdir, readdir, readFile, rm, symlink, writeFile } from 'node:fs/promises';
import { constants } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { RETENTION_RECEIPT, type RetentionOutcome } from '../cargo-retention.js';
import {
  backdate,
  build,
  CLI_NAME,
  cargoEnv,
  churnFixture,
  churnGenerations,
  declaredCli,
  exists,
  holdProfile,
  inFixture,
  labelled,
  metasOf,
  PACKAGE_DIR,
  parseReport,
  put,
  RETENTION_LOCK,
  relock,
  scopedTo,
  snapshot,
  TRASH,
  writeMid,
} from '../testing/cargo-fixture.js';
import { useFixtureCargoHome } from '../testing/fixture-cargo-home.js';

useFixtureCargoHome();

const NATIVE_CLI = join(PACKAGE_DIR, 'dist', 'bin', `${process.platform}-${process.arch}`, CLI_NAME);

function ran(command: string, args: readonly string[], cwd: string): void {
  const done = spawnSync(command, args, { cwd, encoding: 'utf8' });
  if (done.status !== 0) {
    throw new Error(
      `${command} ${args.join(' ')} failed: ${done.error?.message ?? `exit ${done.status}`}\n${done.stderr}`,
    );
  }
}

/**
 * This package as npm ships it: `bun pm pack`, the command smoo's release pack and packed-package validation run, packs
 * the checkout into `base`, and the tarball unpacks where a consumer's install puts it, beside the typia its emit
 * imports. Returns the executable the packed `package.json#bin` declares.
 */
async function packedCli(base: string): Promise<string> {
  const tarball = join(base, 'cargo-retain.tgz');
  const modules = join(base, 'consumer', 'node_modules');
  const installed = join(modules, '@smoothbricks', 'cargo-retain');
  await mkdir(installed, { recursive: true });
  ran(process.execPath, ['pm', 'pack', '--filename', tarball, '--ignore-scripts', '--quiet'], PACKAGE_DIR);
  ran('tar', ['-xzf', tarball, '-C', installed, '--strip-components=1'], base);
  await symlink(dirname(fileURLToPath(import.meta.resolve('typia/package.json'))), join(modules, 'typia'));
  return declaredCli(installed);
}

interface Run {
  readonly status: number | null;
  readonly stdout: string;
  readonly stderr: string;
}

/** Text with the copy's own path, and the receipt's clock and configuration hash, made comparable across copies. */
function normalized(text: string, copy: string): string {
  return text
    .replaceAll(`/private${copy}`, '<copy>')
    .replaceAll(copy, '<copy>')
    .replace(/"at": \d+/g, '"at": 0')
    .replace(/"configuration": "[0-9a-f]+"/g, '"configuration": "<hash>"');
}

/** What a CLI printed and how it exited, comparable across copies. */
function comparable(run: SpawnSyncReturns<string>, copy: string): Run {
  if (run.error !== undefined) throw run.error;
  return { status: run.status, stdout: normalized(run.stdout, copy), stderr: normalized(run.stderr, copy) };
}

interface Scenario {
  readonly name: string;
  /** Changes one copy before either CLI runs. */
  readonly prepare?: (copy: string) => Promise<void>;
  /** Puts back what `prepare` took from a copy's permissions, so the tree can be read. */
  readonly restore?: (copy: string) => Promise<void>;
  readonly args: readonly string[];
  /** The directory operand; absent runs the CLI with none, from the copy's workspace. */
  readonly operand?: (copy: string) => string;
  /** A lock held while the CLIs run, as a Cargo build holds its own. */
  readonly hold?: (copy: string) => string;
  readonly exit: number;
  /** The report's outcome kind; absent when the CLI prints no report. */
  readonly kind?: RetentionOutcome['kind'];
  /** An exact canonical refusal, still subject to byte-identical output comparison. */
  readonly exactReason?: string;
  /**
   * For a refusal whose reason each runtime words its own way, the operating system's text rather than ours: both reasons
   * must match this, and the two outputs are not compared byte for byte.
   */
  readonly reason?: RegExp;
}

const workspaceOf = (copy: string): string => join(copy, 'ws');
const targetOf = (copy: string, ...segments: string[]): string => join(copy, 'ws', 'target', ...segments);

/** macOS refuses flock(2) on a FIFO (Linux allows it): a lock that fails without being contended is a refusal, not a busy skip. */
const FIFO_LOCK: Scenario = {
  name: 'a retention lock that is a FIFO, which flock(2) refuses',
  prepare: async (copy) => {
    const made = spawnSync('mkfifo', [targetOf(copy, RETENTION_LOCK)], { encoding: 'utf8' });
    if (made.status !== 0) throw new Error(`mkfifo failed: ${made.stderr}`);
  },
  restore: async (copy) => {
    await rm(targetOf(copy, RETENTION_LOCK));
  },
  args: ['--apply'],
  operand: workspaceOf,
  exit: 3,
  kind: 'refused',
  reason: /cannot attempt the retention lock: .*(not supported|ENOTSUP)/i,
};
const SCENARIOS: readonly Scenario[] = [
  { name: 'plan', args: [], operand: workspaceOf, exit: 0, kind: 'planned' },
  { name: 'apply', args: ['--apply'], operand: workspaceOf, exit: 0, kind: 'reclaimed' },
  { name: 'the working directory as the workspace', args: [], exit: 0, kind: 'planned' },
  { name: 'a usage error', args: ['--bogus'], exit: 2 },
  {
    name: 'a directory that is not a Cargo workspace',
    prepare: async (copy) => {
      await mkdir(join(copy, 'empty'));
    },
    args: [],
    operand: (copy) => join(copy, 'empty'),
    exit: 1,
  },
  {
    name: 'a held Cargo lock',
    args: ['--apply'],
    operand: workspaceOf,
    hold: (copy) => targetOf(copy, 'debug', '.cargo-lock'),
    exit: 0,
    kind: 'busy',
  },
  {
    name: 'a held retention lock',
    prepare: async (copy) => {
      await put(targetOf(copy, RETENTION_LOCK), '');
    },
    args: ['--apply'],
    operand: workspaceOf,
    hold: (copy) => targetOf(copy, RETENTION_LOCK),
    exit: 0,
    kind: 'busy',
  },
  {
    name: 'a legacy layout beside the current one',
    prepare: async (copy) => {
      const debug = targetOf(copy, 'debug');
      await put(join(debug, '.fingerprint', 'legacy-unit-0123456789abcdef', 'lib-legacy-unit.json'), '{}\n');
      await mkdir(join(debug, 'deps'), { recursive: true });
      await put(join(debug, 'build', 'old-script-0123456789abcdef', 'out', 'x'), 'x');
    },
    args: ['--apply'],
    operand: workspaceOf,
    exit: 0,
    kind: 'reclaimed',
  },
  {
    name: 'a profile holding only a legacy layout',
    prepare: async (copy) => {
      const profile = targetOf(copy, 'legacy-profile');
      await put(join(profile, '.cargo-lock'), '');
      await put(join(profile, '.cargo-build-lock'), '');
      await mkdir(join(profile, 'build'), { recursive: true });
      await put(join(profile, '.fingerprint', 'legacy-unit-0123456789abcdef', 'lib-legacy-unit.json'), '{}\n');
      await mkdir(join(profile, 'deps'));
    },
    args: ['--apply'],
    operand: workspaceOf,
    exit: 3,
    kind: 'refused',
  },
  {
    name: 'an unreadable fingerprint',
    prepare: async (copy) => {
      const [meta] = await metasOf(targetOf(copy), 'ret-mid');
      const fingerprints = join(meta ?? '', 'fingerprint');
      const json = (await readdir(fingerprints)).find((entry) => entry.endsWith('.json'));
      await writeFile(join(fingerprints, json ?? ''), '{');
    },
    args: ['--apply'],
    operand: workspaceOf,
    exit: 3,
    kind: 'refused',
  },
  {
    name: 'a lock file that cannot be opened',
    prepare: async (copy) => {
      await chmod(targetOf(copy, 'debug', '.cargo-lock'), 0o000);
    },
    restore: async (copy) => {
      await chmod(targetOf(copy, 'debug', '.cargo-lock'), 0o644);
    },
    args: ['--apply'],
    operand: workspaceOf,
    exit: 3,
    kind: 'refused',
    exactReason: `cannot attempt Cargo's locks: open '<copy>/ws/target/debug/.cargo-lock': errno ${constants.errno.EACCES}: permission denied`,
  },
  {
    name: 'trash entries nobody can prove',
    prepare: async (copy) => {
      const trash = targetOf(copy, 'debug', TRASH);
      await put(join(trash, 'aaaaaaaa-0000-0000-0000-000000000000.json'), '{');
      await put(join(trash, 'bbbbbbbb-0000-0000-0000-000000000000', '0', 'stray'), 'x');
      await put(
        join(trash, 'cccccccc-0000-0000-0000-000000000000.json'),
        JSON.stringify({ version: 1, entries: [{ from: '/x', dev: '1', ino: '2' }] }),
      );
      await mkdir(join(trash, 'cccccccc-0000-0000-0000-000000000000', '0'), { recursive: true });
    },
    args: ['--apply'],
    operand: workspaceOf,
    exit: 0,
    kind: 'reclaimed',
  },
  ...(process.platform === 'darwin' ? [FIFO_LOCK] : []),
];

describe('the native CLI against the Node CLI on copies of one churned Cargo target', () => {
  it('prints, changes and refuses exactly as the Node CLI does', async () => {
    if (!existsSync(NATIVE_CLI)) {
      throw new Error(
        `no native CLI at ${NATIVE_CLI}: build it first (nx run cargo-retain:build, which runs the host's scriptc-<arch>-<family>)`,
      );
    }
    await inFixture(async (base) => {
      const check = labelled('the native CLI matches the Node CLI');
      const publishedCli = await packedCli(base);
      const original = join(base, 'original');
      await mkdir(original);
      const fx = await churnFixture(original, false);
      const nested = join(fx.target, 'nested-root');
      const superseded: string[] = [];
      await scopedTo(fx.target, async () => {
        superseded.push(...(await churnGenerations(fx)).superseded);
        const buildNested = async (): Promise<void> => {
          await scopedTo(nested, async () => {
            await build(fx.ws);
          });
        };
        await buildNested();
        for (const pkg of ['ret-mid', 'ret-app']) superseded.push(...(await metasOf(nested, pkg)));
        await writeMid(fx.ws, false, '0.1');
        relock(fx.ws);
        await buildNested();
      });
      await backdate([...new Set(superseded)]);
      const copyOf = async (name: string): Promise<string> => {
        const copy = join(base, name);
        await cp(original, copy, { recursive: true, preserveTimestamps: true, verbatimSymlinks: true });
        return copy;
      };
      for (const scenario of SCENARIOS) {
        const { name } = scenario;
        const run = async (copy: string, command: string): Promise<Run> => {
          const release = scenario.hold === undefined ? undefined : await holdProfile(scenario.hold(copy));
          try {
            const operand = scenario.operand === undefined ? [] : [scenario.operand(copy)];
            const cwd = scenario.operand === undefined ? workspaceOf(copy) : copy;
            return comparable(
              spawnSync(command, [...scenario.args, ...operand], {
                cwd,
                // Keep NO_COLOR and every other input; Nx's contradictory FORCE_COLOR makes Node warn under its own PID.
                env: Object.fromEntries(Object.entries(cargoEnv()).filter(([name]) => name !== 'FORCE_COLOR')),
                encoding: 'utf8',
              }),
              copy,
            );
          } finally {
            await release?.();
          }
        };
        const node = await copyOf(`${name}-node`);
        const native = await copyOf(`${name}-native`);
        for (const copy of [node, native]) await scenario.prepare?.(copy);
        const expected = await run(node, publishedCli);
        const actual = await run(native, NATIVE_CLI);
        for (const copy of [node, native]) await scenario.restore?.(copy);
        if (scenario.reason === undefined) {
          check(actual, `${name}: stdout, stderr and exit status`).toEqual(expected);
        } else {
          check(actual.status, `${name}: exit status`).toBe(expected.status);
          for (const printed of [expected, actual]) {
            const outcome = parseReport(printed.stdout).outcome;
            check(outcome.kind === 'refused' ? outcome.reason : '', `${name}: refusal reason`).toMatch(scenario.reason);
          }
        }
        check(expected.status, `${name}: exit status`).toBe(scenario.exit);
        if (scenario.exactReason !== undefined) {
          const outcome = parseReport(actual.stdout).outcome;
          check(outcome.kind === 'refused' ? outcome.reason : '', `${name}: exact refusal`).toBe(scenario.exactReason);
        }
        if (scenario.kind !== undefined) {
          check(parseReport(expected.stdout).outcome.kind, `${name}: outcome`).toBe(scenario.kind);
        }
        const receipt = join('ws', 'target', RETENTION_RECEIPT);
        const without = new Set([receipt]);
        check(await snapshot(native, without), `${name}: resulting tree`).toEqual(await snapshot(node, without));
        const receipts = await Promise.all(
          [node, native].map(async (copy) =>
            (await exists(join(copy, receipt)))
              ? normalized(await readFile(join(copy, receipt), 'utf8'), copy)
              : undefined,
          ),
        );
        check(receipts[1], `${name}: receipt`).toEqual(receipts[0]);
      }
    });
  });
});
