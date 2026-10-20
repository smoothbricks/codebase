import { describe, expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { existsSync } from 'node:fs';
import { chmod, mkdir, mkdtemp, readdir, readFile, rm, stat, symlink, utimes, writeFile } from 'node:fs/promises';
import { homedir, tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { fixtureNxEnv, withNxFixture } from './__tests__/fixture-nx-env.js';
import { RUNTIME_INPUT_SCRIPT, runtimeInput, shellWord } from './runtime-input.js';
import {
  commandPrograms,
  hostPrograms,
  RUNTIME_INPUT_FAILURES,
  runtimeInputFailureReports,
} from './runtime-input-policy.js';

const packageRoot = dirname(dirname(fileURLToPath(import.meta.url)));
const repoRoot = dirname(dirname(packageRoot));
const script = join(packageRoot, 'runtime-input.sh');
const nxEntry = join(repoRoot, 'node_modules', '.bin', 'nx');

interface Ran {
  readonly status: number | null;
  readonly stdout: Buffer;
  readonly stderr: string;
}

function wrapped(workspace: string, ...command: string[]): Ran {
  const ran = spawnSync('sh', [script, ...command], { cwd: workspace, encoding: 'buffer' });
  return { status: ran.status, stdout: ran.stdout, stderr: ran.stderr.toString('utf8') };
}

async function withWrapperWorkspace(run: (workspace: string) => Promise<void>): Promise<void> {
  const workspace = await mkdtemp(join(tmpdir(), 'runtime-input-wrapper-'));
  try {
    await run(workspace);
  } finally {
    await rm(workspace, { recursive: true, force: true });
  }
}

async function failureRecord(workspace: string): Promise<string> {
  const root = join(workspace, '.runtime-input-failures');
  const records = await readdir(root);
  expect(records).toHaveLength(1);
  return join(root, records[0]);
}

describe('runtime-input.sh', () => {
  it('preserves successful stdout bytes and their hash exactly, including whitespace and UTF-8', async () => {
    await withWrapperWorkspace(async (workspace) => {
      const command = ['sh', '-c', "printf ' \tvalue café\n\n'; printf 'diagnostic\n\n' >&2"];
      // The pre-evidence wrapper's success path: stdout passes through, stderr is discarded.
      const baseline = spawnSync(
        'sh',
        [
          '-c',
          'exec 3>&1; if diagnostic=$("$@" 2>&1 >&3 3>&-); then exit 0; else exit "$?"; fi',
          'baseline',
          ...command,
        ],
        { cwd: workspace, encoding: 'buffer' },
      );
      const ran = wrapped(workspace, ...command);
      expect(baseline.status).toBe(0);
      expect(baseline.stderr).toEqual(Buffer.alloc(0));
      expect(ran).toEqual({ status: 0, stdout: Buffer.from(' \tvalue café\n\n'), stderr: '' });
      expect(ran.stdout).toEqual(baseline.stdout);
      const hash = (bytes: Buffer) => createHash('sha256').update(bytes).digest('hex');
      expect(hash(ran.stdout)).toBe(hash(baseline.stdout));
      expect(existsSync(join(workspace, '.runtime-input-failures'))).toBe(false);
    });
  });

  it('poisons a failure and durably retains exact argv, stderr and exit status', async () => {
    await withWrapperWorkspace(async (workspace) => {
      const command = [
        'sh',
        '-c',
        String.raw`printf 'partial\n'; printf 'the-cause\000raw\n\n' >&2; exit 3`,
        '--',
        '',
        'two words',
        "a'quote",
        'line\nbreak',
      ];
      const ran = wrapped(workspace, ...command);
      expect(ran.status).toBe(3);
      // The failed command's partial stdout is evidence, never input bytes:
      // the only byte the hasher sees is the poison.
      expect(ran.stdout).toEqual(Buffer.from([0xff]));
      const record = await failureRecord(workspace);
      expect((await stat(record)).mode & 0o777).toBe(0o700);
      expect(await readFile(join(record, 'argv'))).toEqual(Buffer.from(`${command.join('\0')}\0`));
      expect(await readFile(join(record, 'stderr'))).toEqual(Buffer.from('the-cause\0raw\n\n'));
      expect(await readFile(join(record, 'stdout'))).toEqual(Buffer.from('partial\n'));
      expect(await readFile(join(record, 'exit-status'), 'utf8')).toBe('3\n');
      const report = await readFile(join(record, 'report'), 'utf8');
      expect(report).toContain('runtime input exited 3: sh -c');
      expect(report).toContain('argv[4]: <>');
      expect(report).toContain('the-cause\0raw\n\n');
      // Direct invocation sees the attempted stderr notice; Nx is allowed to drop it.
      expect(ran.stderr).toContain('the-cause');
      expect(ran.stderr).toContain('runtime input exited 3: sh -c');
      expect(ran.stderr).toContain(join(record, 'report'));
    });
  });

  it('refuses to run nothing', async () => {
    await withWrapperWorkspace(async (workspace) => {
      const ran = wrapped(workspace);
      expect(ran.status).toBe(64);
      expect(ran.stdout).toEqual(Buffer.from([0xff]));
    });
  });
});

/**
 * WHY this test is permanent. Nx 23.2.1 hashes a runtime input's stdout and
 * stderr and ignores its exit status; the wrapper's 0xFF byte is what makes a
 * failed input an error instead of a key (runtime-input.sh). Planted through
 * real Nx, both through the daemon that hashes for a workspace's clients and
 * without one, the failure must stop the run before the task executes. A
 * future Nx that hashes those bytes turns this red; one that honours the exit
 * status changes the message, which is the cue to drop the byte.
 */
describe('a planted runtime input failure through real Nx', () => {
  for (const daemon of [false, true]) {
    it(`is an error, not a hash${daemon ? ', through the daemon' : ''}`, async () => {
      await withNxFixture('runtime-input-', async ({ workspace, socketDirectory }) => {
        await writeProbeWorkspace(workspace);
        const env = { ...fixtureNxEnv(workspace, socketDirectory), NX_DAEMON: String(daemon) };

        const good = nx(workspace, env, 'probe:good');
        expect(good.status, good.output).toBe(0);
        expect(existsSync(join(workspace, 'good.ran'))).toBe(true);

        const started = Date.now();
        const planted = nx(workspace, env, 'probe:planted');
        expect(planted.status, planted.output).toBe(1);
        expect(planted.output).toContain('invalid utf-8 sequence');
        // Nx's message names neither the command nor its cause; the retained report names both.
        expect(planted.output).not.toContain('planted-cause');
        expect(existsSync(join(workspace, 'planted.ran'))).toBe(false);
        const record = await failureRecord(workspace);
        const reports = runtimeInputFailureReports(workspace, started);
        expect(reports).toHaveLength(1);
        expect(reports[0]).toContain(`${join(record, 'report')}:\nruntime input exited 3: sh -c`);
        expect(reports[0]).toContain('planted-cause');
        expect(await readFile(join(record, 'argv'))).toEqual(Buffer.from('sh\0-c\0echo planted-cause >&2; exit 3\0'));
        expect(await readFile(join(record, 'stderr'), 'utf8')).toBe('planted-cause\n');
        expect(await readFile(join(record, 'exit-status'), 'utf8')).toBe('3\n');
        expect(await readFile(join(record, 'report'), 'utf8')).toContain('planted-cause');
        const reset = spawnSync('bun', [nxEntry, 'reset'], { cwd: workspace, env, encoding: 'utf8' });
        expect(reset.status, `${reset.stdout}${reset.stderr}`).toBe(0);
        expect(await readFile(join(record, 'stderr'), 'utf8')).toBe('planted-cause\n');
        expect(await readFile(join(record, 'report'), 'utf8')).toContain('runtime input exited 3: sh -c');
      });
    });
  }

  it('is an error on a cold cache too, before anything has run', async () => {
    // The two cases above warm the fixture's cache and daemon with a good run
    // first. A fresh shed meets the failure cold: no cache, no daemon, and the
    // refusal must still stop the run before the task executes.
    await withNxFixture('runtime-input-cold-', async ({ workspace, socketDirectory }) => {
      await writeProbeWorkspace(workspace);
      const env = { ...fixtureNxEnv(workspace, socketDirectory), NX_DAEMON: 'false' };

      const planted = nx(workspace, env, 'probe:planted');
      expect(planted.status, planted.output).toBe(1);
      expect(planted.output).toContain('invalid utf-8 sequence');
      expect(existsSync(join(workspace, 'planted.ran'))).toBe(false);
      const record = await failureRecord(workspace);
      expect(await readFile(join(record, 'exit-status'), 'utf8')).toBe('3\n');
      expect(await readFile(join(record, 'report'), 'utf8')).toContain('runtime input exited 3: sh -c');
    });
  });
});

describe('runtime inputs of this repository', () => {
  it('all run through the wrapper', async () => {
    const offenders: string[] = [];
    const declared = (file: string, value: unknown): void => {
      if (Array.isArray(value)) {
        for (const item of value) declared(file, item);
      } else if (value !== null && typeof value === 'object') {
        for (const [key, item] of Object.entries(value)) {
          if (key === 'runtime' && !String(item).startsWith(`sh ${RUNTIME_INPUT_SCRIPT} `))
            offenders.push(`${file}: ${item}`);
          else declared(file, item);
        }
      }
    };
    const tracked = spawnSync(
      'git',
      ['ls-files', '-z', 'nx.json', 'package.json', '**/package.json', '**/project.json'],
      {
        cwd: repoRoot,
        encoding: 'utf8',
      },
    );
    expect(tracked.status, tracked.stderr).toBe(0);
    for (const file of tracked.stdout.split('\0').filter(Boolean)) {
      declared(file, JSON.parse(await readFile(join(repoRoot, file), 'utf8')));
    }
    // Inferred inputs: every one goes through runtimeInput(), never a literal.
    for (const file of await readdir(join(packageRoot, 'src'))) {
      if (!file.endsWith('.ts') || file.endsWith('.test.ts') || file === 'runtime-input.ts') continue;
      const source = await readFile(join(packageRoot, 'src', file), 'utf8');
      for (const literal of source.match(/\{\s*runtime:\s*[`'"]/g) ?? []) offenders.push(`src/${file}: ${literal}`);
    }
    expect(offenders).toEqual([]);
  });

  it('name only programs the workspace declares', async () => {
    const search = { path: process.env.PATH ?? '', root: repoRoot, home: homedir() };
    const offenders: string[] = [];
    const declared = (file: string, value: unknown): void => {
      if (Array.isArray(value)) {
        for (const item of value) declared(file, item);
      } else if (value !== null && typeof value === 'object') {
        for (const [key, item] of Object.entries(value)) {
          if (key !== 'runtime' || typeof item !== 'string') declared(file, item);
          else
            for (const host of hostPrograms(item, search))
              offenders.push(`${file}: ${host.program} (${host.found ?? 'not on PATH'})`);
        }
      }
    };
    const tracked = spawnSync(
      'git',
      ['ls-files', '-z', 'nx.json', 'package.json', '**/package.json', '**/project.json'],
      {
        cwd: repoRoot,
        encoding: 'utf8',
      },
    );
    expect(tracked.status, tracked.stderr).toBe(0);
    for (const file of tracked.stdout.split('\0').filter(Boolean)) {
      declared(file, JSON.parse(await readFile(join(repoRoot, file), 'utf8')));
    }
    expect(offenders).toEqual([]);
  });
});

describe('runtime input programs', () => {
  it("are each simple command's program, through the wrapper, env, sh -c and substitutions, without builtins", () => {
    expect(commandPrograms(`sh ${RUNTIME_INPUT_SCRIPT} shasum -a 256 packages/x/y.wasm`)).toEqual(['sh', 'shasum']);
    expect(
      commandPrograms(
        `sh ${RUNTIME_INPUT_SCRIPT} sh -c 'if [ -n "\${SRC:-}" ]; then cd "$SRC" && export A=/dev/null && git rev-parse HEAD && changes=$(git status --porcelain) && printf "%s\\n" "$changes" | shasum -a 256; else echo installed; fi'`,
      ),
    ).toEqual(['sh', 'sh', 'git', 'shasum', 'git']);
    expect(
      commandPrograms(
        'env GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_NOSYSTEM=1 git -c core.excludesFile=/dev/null ls-files -z',
      ),
    ).toEqual(['env', 'git']);
    expect(commandPrograms("bun -e 'console.log(1)' >/dev/null 2>&1 </dev/null")).toEqual(['bun']);
    expect(commandPrograms('x=`cut -d: -f1 list` && command -v tr; exec date')).toEqual(['date', 'cut']);
    expect(commandPrograms('for f in a b; do stat "$f"; done # head')).toEqual(['stat']);
    expect(commandPrograms("sh tooling/identity.sh rustc 'sh -c awk'")).toEqual(['sh']);
  });

  it('that the host supplies, or nothing supplies, are refused by name', async () => {
    const scratch = await mkdtemp(join(tmpdir(), 'runtime-input-programs-'));
    try {
      const host = join(scratch, 'usr-bin');
      const root = join(scratch, 'checkout');
      await mkdir(host, { recursive: true });
      await mkdir(join(root, 'node_modules', '.bin'), { recursive: true });
      for (const file of [join(host, 'shasum'), join(root, 'node_modules', '.bin', 'tool')]) {
        await writeFile(file, '#!/bin/sh\n');
        await chmod(file, 0o755);
      }
      // A file that is not executable provides nothing.
      await writeFile(join(host, 'absent'), '');
      const search = { path: `${host}:${join(root, 'node_modules', '.bin')}`, root, home: join(scratch, 'home') };
      const command = 'shasum -a 256 f && tool && tooling/x.sh && /nix/store/0-a/bin/a && absent; /usr/bin/env true';
      expect(hostPrograms(`${command}; /usr/bin/shasum`, search)).toEqual([
        { program: 'shasum', found: join(host, 'shasum') },
        { program: 'absent', found: undefined },
        { program: '/usr/bin/shasum', found: '/usr/bin/shasum' },
      ]);
    } finally {
      await rm(scratch, { recursive: true, force: true });
    }
  });
});

describe('retained runtime input failure reports', () => {
  it('are read back from a time on, oldest first, naming a record whose report is missing', async () => {
    await withWrapperWorkspace(async (workspace) => {
      const failures = join(workspace, RUNTIME_INPUT_FAILURES);
      const record = async (name: string, at: number, report?: string): Promise<void> => {
        await mkdir(join(failures, name), { recursive: true });
        if (report !== undefined) await writeFile(join(failures, name, 'report'), report);
        await utimes(join(failures, name), at / 1000, at / 1000);
      };
      expect(runtimeInputFailureReports(workspace, 0)).toEqual([]);
      await record('failure.old', 1_000_000, 'old\n');
      await record('failure.second', 3_000_000, 'second\n');
      await record('failure.first', 2_000_000);
      expect(runtimeInputFailureReports(workspace, 2_000_000)).toEqual([
        `${join(failures, 'failure.first')}: no report; its evidence is incomplete\n`,
        `${join(failures, 'failure.second', 'report')}:\nsecond\n`,
      ]);
    });
  });
});

async function writeProbeWorkspace(workspace: string): Promise<void> {
  expect(spawnSync('git', ['init', '--quiet', workspace]).status).toBe(0);
  await mkdir(join(workspace, 'probe'), { recursive: true });
  // The repository's own node_modules: the Nx under test, and the wrapper at the path inputs name it by.
  await symlink(join(repoRoot, 'node_modules'), join(workspace, 'node_modules'), 'dir');
  await writeFile(join(workspace, 'nx.json'), JSON.stringify({ useDaemonProcess: true, cacheDirectory: '.nx/cache' }));
  const target = (name: string, command: string) => ({
    executor: 'nx:run-commands',
    cache: true,
    inputs: [runtimeInput(command)],
    options: { command: `touch ${name}.ran` },
  });
  await writeFile(
    join(workspace, 'probe', 'project.json'),
    JSON.stringify({
      name: 'probe',
      targets: {
        good: target('good', `sh -c ${shellWord('echo value; echo noise >&2')}`),
        planted: target('planted', `sh -c ${shellWord('echo planted-cause >&2; exit 3')}`),
      },
    }),
  );
}

function nx(workspace: string, env: Record<string, string>, target: string): { status: number | null; output: string } {
  const ran = spawnSync('bun', [nxEntry, 'run', target, '--outputStyle=static'], {
    cwd: workspace,
    env,
    encoding: 'utf8',
  });
  return { status: ran.status, output: `${ran.stdout}${ran.stderr}` };
}
