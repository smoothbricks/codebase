import { describe, expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { existsSync } from 'node:fs';
import {
  chmod,
  mkdir,
  mkdtemp,
  readdir,
  readFile,
  realpath,
  rm,
  stat,
  symlink,
  utimes,
  writeFile,
} from 'node:fs/promises';
import { homedir, tmpdir } from 'node:os';
import { delimiter, dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import type { Task, TaskGraph } from 'nx/src/config/task-graph';
import { NativeTaskHasherImpl } from 'nx/src/hasher/native-task-hasher-impl';
import { fixtureNxEnv, withNxFixture } from './__tests__/fixture-nx-env.js';
import { RUNTIME_INPUT_SCRIPT, runtimeInput, shellWord } from './runtime-input.js';
import {
  commandPrograms,
  hostProgramReason,
  hostPrograms,
  RUNTIME_INPUT_FAILURES,
  runtimeInputFailureReports,
} from './runtime-input-policy.js';

const packageRoot = dirname(dirname(fileURLToPath(import.meta.url)));
const repoRoot = dirname(dirname(packageRoot));
const script = join(packageRoot, 'runtime-input.sh');
const nxEntry = join(repoRoot, 'node_modules', '.bin', 'nx');
/** The probe's failing input: Nx's refusal must name it. */
const plantedCommand = `sh -c ${shellWord('echo planted-cause >&2; exit 3')}`;

interface Ran {
  readonly status: number | null;
  readonly stdout: Buffer;
  readonly stderr: string;
}

function wrapped(workspace: string, ...command: string[]): Ran {
  // No controlling terminal, as under Nx's daemon: `detached` starts a new session (setsid), which has none.
  const ran = Bun.spawnSync(['sh', script, ...command], {
    cwd: workspace,
    detached: true,
    stdout: 'pipe',
    stderr: 'pipe',
  });
  return { status: ran.exitCode, stdout: ran.stdout, stderr: ran.stderr.toString('utf8') };
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

  it('runs no program but the command on success, so a runtime input costs its command alone', async () => {
    await withWrapperWorkspace(async (workspace) => {
      // An empty PATH: a wrapper that ran mktemp, cat or rm on its success path could not find them.
      const empty = join(workspace, 'empty-path');
      await mkdir(empty);
      const ran = Bun.spawnSync(['/bin/sh', script, '/bin/sh', '-c', "printf 'value\\n\\n'; printf 'note\\n' >&2"], {
        cwd: workspace,
        detached: true,
        env: { PATH: empty },
        stdout: 'pipe',
        stderr: 'pipe',
      });
      expect({ status: ran.exitCode, stdout: ran.stdout.toString(), stderr: ran.stderr.toString() }).toEqual({
        status: 0,
        stdout: 'value\n\n',
        stderr: '',
      });
      expect(existsSync(join(workspace, '.runtime-input-failures'))).toBe(false);
    });
  });

  it('poisons a failure and durably retains only its exact argv, stdout, stderr and exit status', async () => {
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
      // No terminal to show the report on is no failure of the input: the record holds its evidence alone.
      expect((await readdir(record)).sort()).toEqual(['argv', 'exit-status', 'report', 'stderr', 'stdout']);
      expect(await readFile(join(record, 'argv'))).toEqual(Buffer.from(`${command.join('\0')}\0`));
      // A shell variable holds no NUL byte, so the capture drops it; every other byte is kept.
      expect(await readFile(join(record, 'stderr'))).toEqual(Buffer.from('the-causeraw\n\n'));
      expect(await readFile(join(record, 'stdout'))).toEqual(Buffer.from('partial\n'));
      expect(await readFile(join(record, 'exit-status'), 'utf8')).toBe('3\n');
      const report = await readFile(join(record, 'report'), 'utf8');
      expect(report).toContain('runtime input exited 3: sh -c');
      expect(report).toContain('argv[4]: <>');
      expect(report).toContain('the-causeraw\n\n');
      // Direct invocation sees the attempted stderr notice; Nx is allowed to drop it.
      expect(ran.stderr).toContain('the-cause');
      expect(ran.stderr).toContain('runtime input exited 3: sh -c');
      expect(ran.stderr).toContain(join(record, 'report'));
      // The shell's refusal to open /dev/tty is a diagnostic beside that notice, not evidence.
      expect(ran.stderr).toContain('/dev/tty');
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
 * without one, the failure must stop the run before the task executes, and
 * Nx's error must name the task and the runtime input it was hashing (the
 * carried Nx patch's hasher). A future Nx that hashes those bytes turns this
 * red; one that honours the exit status changes the message, which is the cue
 * to drop the byte.
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
        // Nx's native refusal names neither the task nor the input. The patched hasher names
        // both; the retained report names the cause.
        expect(planted.output).toContain(
          'Could not hash probe:planted: invalid utf-8 sequence of 1 bytes from index 0',
        );
        expect(planted.output).toContain(runtimeInput(plantedCommand).runtime);
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

/**
 * Guards the hash-failure hunk of patches/nx@23.2.1.patch. Nx 23.2.1's native hasher fails a runtime input whose output
 * is not UTF-8 with `invalid utf-8 sequence of 1 bytes from index 0`, naming no task and no input. The patched
 * `hashTasks` rethrows it naming the tasks (ten, the rest counted) and the runtime inputs their plans hold, read from its
 * planner. When the planner cannot answer, the native message still leads and the native error stays the cause. The
 * table drives the installed `hashTasks` over stand-ins for Nx's native planner and hasher, faked at their published
 * methods; the planted failure above proves the same path through real Nx.
 */
describe("Nx's error for a failed hash", () => {
  const NATIVE = new Error('invalid utf-8 sequence of 1 bytes from index 0');
  const GRAPH: TaskGraph = { roots: [], tasks: {}, dependencies: {}, continuousDependencies: {} };
  const WRAPPED = runtimeInput('bun --version').runtime;
  const MANY = Array.from({ length: 12 }, (_, index) => `p${String(index).padStart(2, '0')}:build`);
  const TEN_AND_TWO = `${MANY.slice(0, 10).join(', ')}, and 2 more`;
  const task = (id: string): Task => {
    const colon = id.indexOf(':');
    return {
      id,
      target: { project: id.slice(0, colon), target: id.slice(colon + 1) },
      overrides: {},
      outputs: [],
      cache: false,
    };
  };

  /** What `hashTasks` rejects with when the native hasher throws `error` and the planner's `getPlans` is the given one. */
  async function refusal(
    error: unknown,
    taskIds: string[],
    getPlans: () => Record<string, string[]>,
  ): Promise<{ message: string; cause: unknown }> {
    const native = {
      planner: { getPlansReference: () => 'plans', getPlans },
      hasher: {
        hashPlans: () => {
          throw error;
        },
      },
    };
    const envs = Object.fromEntries(taskIds.map((id) => [id, {}]));
    const settled: unknown = await NativeTaskHasherImpl.prototype.hashTasks
      .call(native, taskIds.map(task), GRAPH, envs, repoRoot, false)
      .then(
        () => new Error('hashTasks resolved'),
        (rejected: unknown) => rejected,
      );
    return settled instanceof Error
      ? { message: settled.message, cause: settled.cause }
      : { message: `rejected with a non-Error: ${String(settled)}`, cause: settled };
  }

  it('names the tasks being hashed and their runtime inputs, and keeps the native error when the planner fails', async () => {
    const rows = [
      {
        label: 'plans listed',
        error: NATIVE,
        taskIds: ['b:test', 'a:build'],
        getPlans: () => ({
          'a:build': [`runtime:${WRAPPED}`, 'AllExternalDependencies'],
          'b:test': [`runtime:${WRAPPED}`, 'runtime:echo b'],
        }),
        message: [
          `Could not hash a:build, b:test: ${NATIVE.message}`,
          'Runtime inputs of those tasks:',
          '  echo b (b:test)',
          `  ${WRAPPED} (a:build, b:test)`,
        ],
      },
      {
        label: 'ten tasks named, the rest counted',
        error: NATIVE,
        taskIds: MANY,
        getPlans: () => Object.fromEntries(MANY.map((id) => [id, [`runtime:${WRAPPED}`]])),
        message: [
          `Could not hash ${TEN_AND_TWO}: ${NATIVE.message}`,
          'Runtime inputs of those tasks:',
          `  ${WRAPPED} (${TEN_AND_TWO})`,
        ],
      },
      {
        label: 'planner fails',
        error: NATIVE,
        taskIds: ['a:build'],
        getPlans: () => {
          throw new Error('the planner is gone');
        },
        message: [
          `Could not hash a:build: ${NATIVE.message}`,
          'The runtime inputs of those tasks could not be listed: the planner is gone',
        ],
      },
      {
        label: 'native value is no Error',
        error: 'a native failure as a string',
        taskIds: ['a:build'],
        getPlans: () => ({ 'a:build': ['AllExternalDependencies'] }),
        message: ['Could not hash a:build: a native failure as a string'],
      },
    ];
    const actual = await Promise.all(
      rows.map(async (row) => ({ label: row.label, ...(await refusal(row.error, row.taskIds, row.getPlans)) })),
    );
    expect(actual).toEqual(
      rows.map((row) => ({ label: row.label, message: row.message.join('\n'), cause: row.error })),
    );
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

  it('name only programs the workspace declares, on the PATH it declares', async () => {
    // Nx hands a runtime input its own process environment unchanged (nx task-env.js getTaskSpecificEnv), and this
    // repository composes that PATH with tooling/direnv/repo-path, in its devenv shell and on CI (`--github-path`): the
    // devenv profile and the checkout's own directories, no /usr/bin or /bin. Judged on that PATH alone, an input that
    // needs a program the host's directories supply fails here on every OS, as it fails on the NixOS runner.
    const composed = spawnSync('bash', [join(repoRoot, 'tooling', 'direnv', 'repo-path'), '--print'], {
      cwd: repoRoot,
      encoding: 'utf8',
    });
    expect(composed.status, composed.stderr).toBe(0);
    const path = composed.stdout.trim().split('\n').join(delimiter);
    const venv = process.env.VIRTUAL_ENV === undefined ? undefined : join(process.env.VIRTUAL_ENV, 'bin');
    expect(
      path
        .split(delimiter)
        .filter((entry) => !entry.startsWith('/nix/') && !entry.startsWith(`${repoRoot}/`) && entry !== venv),
    ).toEqual([]);
    const search = { path, root: repoRoot, home: homedir() };
    const offenders: string[] = [];
    const declared = (file: string, value: unknown): void => {
      if (Array.isArray(value)) {
        for (const item of value) declared(file, item);
      } else if (value !== null && typeof value === 'object') {
        for (const [key, item] of Object.entries(value)) {
          if (key !== 'runtime' || typeof item !== 'string') declared(file, item);
          else for (const host of hostPrograms(item, search)) offenders.push(`${file}: ${hostProgramReason(host)}`);
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

  it('that the host supplies, nothing supplies, or only a workspace build supplies, are refused by name', async () => {
    const scratch = await mkdtemp(join(tmpdir(), 'runtime-input-programs-'));
    try {
      const host = join(scratch, 'usr-bin');
      const root = join(scratch, 'checkout');
      const bin = join(root, 'node_modules', '.bin');
      await mkdir(host, { recursive: true });
      await mkdir(bin, { recursive: true });
      await mkdir(join(root, 'packages', 'tool', 'dist'), { recursive: true });
      await mkdir(join(root, 'node_modules', '.bun', 'ext', 'bin'), { recursive: true });
      for (const file of [
        join(host, 'shasum'),
        join(bin, 'tool'),
        join(root, 'packages', 'tool', 'dist', 'built.js'),
        join(root, 'node_modules', '.bun', 'ext', 'bin', 'ext.js'),
      ]) {
        await writeFile(file, '#!/bin/sh\n');
        await chmod(file, 0o755);
      }
      // A workspace package's bin links into its build output; an installed package's into node_modules.
      await symlink('../../packages/tool/dist/built.js', join(bin, 'built'));
      await symlink('../.bun/ext/bin/ext.js', join(bin, 'ext'));
      // A file that is not executable provides nothing.
      await writeFile(join(host, 'absent'), '');
      const search = { path: `${host}:${bin}`, root, home: join(scratch, 'home') };
      const command = 'shasum -a 256 f && tool && ext && tooling/x.sh && /nix/store/0-a/bin/a && absent; built';
      const refused = hostPrograms(`${command}; /usr/bin/env true; /usr/bin/shasum`, search);
      const built = await realpath(join(root, 'packages', 'tool', 'dist', 'built.js'));
      expect(refused).toEqual([
        { program: 'shasum', refusal: 'host', found: join(host, 'shasum') },
        { program: 'absent', refusal: 'absent' },
        { program: 'built', refusal: 'workspace-bin', found: join(bin, 'built'), target: built },
        { program: '/usr/bin/shasum', refusal: 'host', found: '/usr/bin/shasum' },
      ]);
      expect(refused.map(hostProgramReason)).toEqual([
        `PATH finds shasum at ${join(host, 'shasum')}, a host program no devenv profile declares`,
        'nothing on PATH provides absent',
        `PATH finds built at ${join(bin, 'built')}, a link to this checkout's build output ${built}, which an ` +
          'install that runs before the build does not create; run that file with node or bun instead',
        'PATH finds /usr/bin/shasum at /usr/bin/shasum, a host program no devenv profile declares',
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
        planted: target('planted', plantedCommand),
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
